/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import org.locationtech.geomesa.cassandra.{CassandraAppendFeatureWriterType, CassandraFeatureWriterType}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


class CassandraAppendFeatureWriter(sft: SimpleFeatureType, ds: CassandraDataStore)
    extends CassandraFeatureWriterType(sft, ds)
    with CassandraAppendFeatureWriterType
    with CassandraFeatureWriter


trait CassandraFeatureWriter
    extends CassandraFeatureWriterType {

  private val serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)

  override protected def createMutators(tables: Seq[String]): Seq[String] = {
    tables.map(tableName =>s"INSERT INTO $tableName (rowId, feature) VALUES (?, ?)")
  }

  override protected def createWrites(mutators: Seq[String]): Seq[((String, String)) => Unit] =
    mutators.map(
      statement =>
        (t: (String, String)) => {
          val (rowId, feature) = t
          val rs = ds.session.execute(statement, rowId, feature)
        }
    )

  override protected def createRemoves(mutators: Seq[String]): Seq[((String, String)) => Unit] = ???

  override protected def wrapFeature(feature: SimpleFeature): CassandraFeature =
    new CassandraFeature(feature, serializer)

}



/*

trait CassandraFeatureWriter extends FW[SimpleFeatureType, SimpleFeature] {
  import CassandraDataStore._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

  import scala.collection.JavaConversions._

  def sft: SimpleFeatureType
  def session: Session

  val cols = sft.getAttributeDescriptors.map { ad => ad.getLocalName }
  val serializers = sft.getAttributeDescriptors.map { ad => FieldSerializer(ad) }
  val geomField = sft.getGeomField
  val geomIdx   = sft.getGeomIndex
  val dtgField  = sft.getDtgField.get
  val dtgIdx    = sft.getDtgIndex.get
  val insert = session.prepare(s"INSERT INTO ${sft.getTypeName} (pkz, z31, fid, ${cols.mkString(",")}) values (${Seq.fill(3+cols.length)("?").mkString(",")})")

  private var curFeature: SimpleFeature = new ScalaSimpleFeature(UUID.randomUUID().toString, sft)

  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature(UUID.randomUUID().toString, sft)
    curFeature
  }

  override def remove(): Unit = ???

  override def write(): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions._

    val geom = curFeature.point
    val geo = ByteBuffer.wrap(WKBUtils.write(geom))
    val x = geom.getX
    val y = geom.getY
    val dtg = new DateTime(curFeature.getAttribute(dtgIdx).asInstanceOf[java.util.Date])
    val weeks = CassandraPrimaryKey.epochWeeks(dtg)

    val secondsInWeek = CassandraPrimaryKey.secondsInCurrentWeek(dtg)
    val pk = CassandraPrimaryKey(dtg, x, y)
    val z3 = CassandraPrimaryKey.SFC3D.index(x, y, secondsInWeek)
    val z31 = z3.z

    val bindings = Array(Int.box(pk.idx), Long.box(z31): java.lang.Long, curFeature.getID) ++
      curFeature.getAttributes.zip(serializers).map { case (o, ser) => ser.serialize(o) }
    session.execute(insert.bind(bindings: _*))
    curFeature = null
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}
}

class AppendFW(val sft: SimpleFeatureType, val session: Session) extends CassandraFeatureWriter {
  def hasNext = false
}

// TODO: need to implement update functionality
class UpdateFW(val sft: SimpleFeatureType, val session: Session) extends CassandraFeatureWriter {
  override def hasNext: Boolean = true
}

*/