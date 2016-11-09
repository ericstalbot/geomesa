/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.index

import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.Bytes
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature, WhateverQueryPlan}
import org.locationtech.geomesa.cassandra.{CassandraFeatureIndexType, CassandraFilterStrategyType, CassandraIndexManagerType, CassandraQueryPlanType}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureDeserializer}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.index.IndexAdapter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter


object CassandraFeatureIndex extends CassandraIndexManagerType {
  // note: keep in priority order for running full table scans
  override def AllIndices: Seq[CassandraFeatureIndex] =
      Seq(CassandraZ3Index, CassandraZ2Index, CassandraIdIndex) //todo: implement more indices for cassandra

  override val CurrentIndices: Seq[CassandraFeatureIndex] = AllIndices
}


trait CassandraFeatureIndex extends CassandraFeatureIndexType
    with IndexAdapter[CassandraDataStore, CassandraFeature, (String, String), Row, Array[Byte]] {

  override def configure(sft: SimpleFeatureType, ds: CassandraDataStore): Unit = {
    super.configure(sft, ds)
    val tableName = getTableName(sft.getTypeName, ds)
    val q = s"CREATE TABLE IF NOT EXISTS $tableName (rowId blob PRIMARY KEY, feature blob)"
    ds.session.execute(q)
  }

  override protected def createInsert(row: Array[Byte], feature: CassandraFeature): (String, String) = {
    return (Bytes.toHexString(row), Bytes.toHexString(feature.value))
  }


  override protected def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Row) => SimpleFeature = {
    val getId = getIdFromRow(sft)
    val deserializer = if (sft == returnSft) {
      new KryoFeatureSerializer(sft, SerializationOptions.withoutId)
    } else {
      new ProjectingKryoFeatureDeserializer(sft, returnSft, SerializationOptions.withoutId)
    }

    (result) => {

      val r = result.getString("feature")
      val s = Bytes.fromHexString(r)
      val t = Bytes.getArray(s)
      val u = deserializer.deserialize(t)
      u

    }

    //todo: attach id to feature (see hbase)

  }

  override protected def createDelete(row: Array[Byte], feature: CassandraFeature): (String, String) = ???


  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: CassandraDataStore,
                                  filter: CassandraFilterStrategyType,
                                  hints: Hints,
                                  ranges: Seq[Array[Byte]],
                                  ecql: Option[Filter]): CassandraQueryPlanType = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val table = getTableName(sft.getTypeName, ds)
    val eToF = entriesToFeatures(sft, hints.getReturnSft)

    WhateverQueryPlan(filter, table, ranges, eToF)

  }

  override def delete(sft: SimpleFeatureType, ds: CassandraDataStore, shared: Boolean): Unit = ???

  override protected def rangeExact(row: Array[Byte]): Array[Byte] = {
    row
  }

  override protected def range(start: Array[Byte], end: Array[Byte]): Array[Byte] = ???

  override protected def rangePrefix(prefix: Array[Byte]): Array[Byte] = ???


}
