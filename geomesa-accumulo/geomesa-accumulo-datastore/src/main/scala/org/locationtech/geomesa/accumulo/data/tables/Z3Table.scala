/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import java.util.Date

import com.google.common.base.Charsets
import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.BatchDeleter
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Mutation, Value, Range => aRange}
import org.apache.hadoop.io.Text
import org.joda.time.{DateTime, DateTimeZone, Seconds, Weeks}
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data.EMPTY_TEXT
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object Z3Table extends GeoMesaTable {

  val EPOCH = new DateTime(0) // min value we handle - 1970-01-01T00:00:00.000
  val EPOCH_END = EPOCH.plusSeconds(Int.MaxValue) // max value we can calculate - 2038-01-18T22:19:07.000
  val FULL_CF = new Text("F")
  val BIN_CF = new Text("B")
  val EMPTY_BYTES = Array.empty[Byte]
  val EMPTY_VALUE = new Value(EMPTY_BYTES)
  val NUM_SPLITS = 4 // can't be more than Byte.MaxValue (127)
  val SPLIT_ARRAYS = (0 until NUM_SPLITS).map(_.toByte).toArray.map(Array(_)).toSeq

  // the bytes of z we keep for complex geoms
  // 3 bytes is 15 bits of geometry (not including time bits and the first 2 bits which aren't used)
  // roughly equivalent to 3 digits of geohash (32^3 == 2^15) and ~78km resolution
  // (4 bytes is 20 bits, equivalent to 4 digits of geohash and ~20km resolution)
  // note: we also lose time resolution
  val GEOM_Z_NUM_BYTES = 3
  // mask for zeroing the last (8 - GEOM_Z_NUM_BYTES) bytes
  val GEOM_Z_MASK: Long = Long.MaxValue << (64 - 8 * GEOM_Z_NUM_BYTES)
  // step needed (due to the mask) to bump up the z value for a complex geom
  val GEOM_Z_STEP: Long = 1L << (64 - 8 * GEOM_Z_NUM_BYTES)

  override def supports(sft: SimpleFeatureType): Boolean =
    sft.getDtgField.isDefined && ((sft.getSchemaVersion > 6 && sft.getGeometryDescriptor != null) ||
        (sft.getSchemaVersion > 4 && sft.isPoints))

  override val suffix: String = "z3"

  // z3 always needs a separate table since we don't include the feature name in the row key
  override def formatTableName(prefix: String, sft: SimpleFeatureType): String =
    GeoMesaTable.formatSoloTableName(prefix, suffix, sft.getTypeName)

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("Z3 writer requires a valid date"))
    val getRowKeys: (FeatureToWrite, Int) => Seq[Array[Byte]] =
      if (sft.isPoints) {
        if (hasSplits(sft)) {
          getPointRowKey
        } else {
          (ftw, i) => getPointRowKey(ftw, i).map(_.drop(1))
        }
      } else {
        getGeomRowKeys
      }
    val getValue: (FeatureToWrite) => Value = if (sft.getSchemaVersion > 5) {
      // we know the data is kryo serialized in version 6+
      (fw) => fw.dataValue
    } else {
      // we always want to use kryo - reserialize the value to ensure it
      val writer = new KryoFeatureSerializer(sft)
      (fw) => new Value(writer.serialize(fw.feature))
    }
    sft.getVisibilityLevel match {
      case VisibilityLevel.Feature =>
        (fw: FeatureToWrite) => {
          val rows = getRowKeys(fw, dtgIndex)
          // store the duplication factor in the column qualifier for later use
          val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
          rows.map { row =>
            val mutation = new Mutation(row)
            mutation.put(FULL_CF, cq, fw.columnVisibility, getValue(fw))
            fw.binValue.foreach(v => mutation.put(BIN_CF, cq, fw.columnVisibility, v))
            mutation
          }
        }
      case VisibilityLevel.Attribute =>
        (fw: FeatureToWrite) => {
          val rows = getRowKeys(fw, dtgIndex)
          // TODO GEOMESA-1254 duplication factor, bin values
          rows.map { row =>
            val mutation = new Mutation(row)
            fw.perAttributeValues.foreach(key => mutation.put(key.cf, key.cq, key.vis, key.value))
            mutation
          }
        }
    }
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("Z3 writer requires a valid date"))
    val getRowKeys: (FeatureToWrite, Int) => Seq[Array[Byte]] =
      if (sft.isPoints) {
        if (hasSplits(sft)) {
          getPointRowKey
        } else {
          (ftw, i) => getPointRowKey(ftw, i).map(_.drop(1))
        }
      } else {
        getGeomRowKeys
      }
    sft.getVisibilityLevel match {
      case VisibilityLevel.Feature =>
        (fw: FeatureToWrite) => {
          val rows = getRowKeys(fw, dtgIndex)
          val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
          rows.map { row =>
            val mutation = new Mutation(row)
            mutation.putDelete(BIN_CF, cq, fw.columnVisibility)
            mutation.putDelete(FULL_CF, cq, fw.columnVisibility)
            mutation
          }
        }
      case VisibilityLevel.Attribute =>
        (fw: FeatureToWrite) => {
          val rows = getRowKeys(fw, dtgIndex)
          // TODO duplication factor, bin values
          rows.map { row =>
            val mutation = new Mutation(row)
            fw.perAttributeValues.foreach(key => mutation.putDelete(key.cf, key.cq, key.vis))
            mutation
          }
        }
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String = {
    val offset = getIdRowOffset(sft)
    (row: Array[Byte]) => new String(row, offset, row.length - offset, Charsets.UTF_8)
  }

  override def deleteFeaturesForType(sft: SimpleFeatureType, bd: BatchDeleter): Unit = {
    bd.setRanges(Seq(new aRange()))
    bd.delete()
  }

  // geoms always have splits, but they weren't added until schema 7
  def hasSplits(sft: SimpleFeatureType) = sft.getSchemaVersion > 6

  // gets week and seconds into that week
  def getWeekAndSeconds(time: DateTime): (Short, Int) = {
    val weeks = Weeks.weeksBetween(EPOCH, time)
    val secondsInWeek = Seconds.secondsBetween(EPOCH, time).getSeconds - weeks.toStandardSeconds.getSeconds
    (weeks.getWeeks.toShort, secondsInWeek)
  }

  // gets week and seconds into that week
  def getWeekAndSeconds(time: Long): (Short, Int) = getWeekAndSeconds(new DateTime(time, DateTimeZone.UTC))

  // split(1 byte), week(2 bytes), z value (8 bytes), id (n bytes)
  private def getPointRowKey(ftw: FeatureToWrite, dtgIndex: Int): Seq[Array[Byte]] = {
    val split = SPLIT_ARRAYS(ftw.idHash % NUM_SPLITS)
    val (week, z) = {
      val dtg = ftw.feature.getAttribute(dtgIndex).asInstanceOf[Date]
      val time = if (dtg == null) 0 else dtg.getTime
      val (w, t) = getWeekAndSeconds(time)
      val geom = ftw.feature.point
      (w, Z3SFC.index(geom.getX, geom.getY, t).z)
    }
    val id = ftw.feature.getID.getBytes(Charsets.UTF_8)
    Seq(Bytes.concat(split, Shorts.toByteArray(week), Longs.toByteArray(z), id))
  }

  // split(1 byte), week (2 bytes), z value (3 bytes), id (n bytes)
  private def getGeomRowKeys(ftw: FeatureToWrite, dtgIndex: Int): Seq[Array[Byte]] = {
    val split = SPLIT_ARRAYS(ftw.idHash % NUM_SPLITS)
    val (week, zs) = {
      val dtg = ftw.feature.getAttribute(dtgIndex).asInstanceOf[Date]
      val time = if (dtg == null) 0 else dtg.getTime
      val (w, t) = getWeekAndSeconds(time)
      val geom = ftw.feature.getDefaultGeometry.asInstanceOf[Geometry]
      (Shorts.toByteArray(w), zBox(geom, t).toSeq)
    }
    val id = ftw.feature.getID.getBytes(Charsets.UTF_8)
    zs.map(z => Bytes.concat(split, week, Longs.toByteArray(z).take(GEOM_Z_NUM_BYTES), id))
  }

  // gets a sequence of (week, z) values that cover the geometry
  private def zBox(geom: Geometry, t: Int): Set[Long] = geom match {
    case g: Point => Set(Z3SFC.index(g.getX, g.getY, t).z)
    case g: LineString =>
      // we flatMap bounds for each line segment so we cover a smaller area
      (0 until g.getNumPoints).map(g.getPointN).sliding(2).flatMap { case Seq(one, two) =>
        val (xmin, xmax) = minMax(one.getX, two.getX)
        val (ymin, ymax) = minMax(one.getY, two.getY)
        getZPrefixes(xmin, ymin, xmax, ymax, t)
      }.toSet
    case g: GeometryCollection => (0 until g.getNumGeometries).toSet.map(g.getGeometryN).flatMap(zBox(_, t))
    case g: Geometry =>
      val env = g.getEnvelopeInternal
      getZPrefixes(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY, t)
  }

  private def minMax(a: Double, b: Double): (Double, Double) = if (a < b) (a, b) else (b, a)

  // gets z values that cover the bounding box
  private def getZPrefixes(xmin: Double, ymin: Double, xmax: Double, ymax: Double, t: Long): Set[Long] = {
    Z3SFC.ranges((xmin, xmax), (ymin, ymax), (t, t), 8 * GEOM_Z_NUM_BYTES).flatMap { range =>
      val lower = range.lower & GEOM_Z_MASK
      val upper = range.upper & GEOM_Z_MASK
      if (lower == upper) {
        Seq(lower)
      } else {
        val count = ((upper - lower) / GEOM_Z_STEP).toInt
        Seq.tabulate(count)(i => lower + i * GEOM_Z_STEP) :+ upper
      }
    }.toSet
  }

  // gets the offset into the row for the id bytes
  def getIdRowOffset(sft: SimpleFeatureType): Int = {
    val length = if (sft.isPoints) 10 else 2 + GEOM_Z_NUM_BYTES // week + z bytes
    val prefix = if (hasSplits(sft)) 1 else 0 // shard
    prefix + length
  }

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    val localityGroups = Seq(BIN_CF, FULL_CF).map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
    tableOps.setLocalityGroups(table, localityGroups)

    // drop first split, otherwise we get an empty tablet
    val splits = SPLIT_ARRAYS.drop(1).map(new Text(_)).toSet
    val splitsToAdd = splits -- tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }
  }
}
