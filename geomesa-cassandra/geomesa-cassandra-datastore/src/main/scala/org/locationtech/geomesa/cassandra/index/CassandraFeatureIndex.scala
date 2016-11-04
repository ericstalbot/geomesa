/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.index

import com.datastax.driver.core.utils.Bytes
import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.cassandra.{CassandraFeatureIndexType, CassandraFilterStrategyType, CassandraIndexManagerType, CassandraQueryPlanType}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.index.IndexAdapter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter


object CassandraFeatureIndex extends CassandraIndexManagerType {
  // note: keep in priority order for running full table scans
  override def AllIndices: Seq[CassandraFeatureIndex] =
      Seq(CassandraZ3Index, CassandraIdIndex) //todo: implement more indices for cassandra

  override val CurrentIndices: Seq[CassandraFeatureIndex] = AllIndices
}


trait CassandraFeatureIndex extends CassandraFeatureIndexType
    with IndexAdapter[CassandraDataStore, CassandraFeature, (String, String), Integer, String] {

  override def configure(sft: SimpleFeatureType, ds: CassandraDataStore): Unit = {
    super.configure(sft, ds)
    val tableName = getTableName(sft.getTypeName, ds)
    val q = s"CREATE TABLE IF NOT EXISTS $tableName (rowId blob PRIMARY KEY, feature blob)"
    ds.session.execute(q)
  }

  override protected def createInsert(row: Array[Byte], feature: CassandraFeature): (String, String) = {
    return (Bytes.toHexString(row), Bytes.toHexString(feature.value))
  }


  //todo: implement these methods
  override protected def range(start: Array[Byte], end: Array[Byte]): String = ???

  override protected def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Any) => SimpleFeature = ???

  override protected def createDelete(row: Array[Byte], feature: CassandraFeature): (String, String) = ???


  override protected def scanPlan(
                                   sft: SimpleFeatureType,
                                   ds: CassandraDataStore,
                                   filter: CassandraFilterStrategyType,
                                   hints: Hints,
                                   ranges: Seq[String],
                                   ecql: Option[Filter]): CassandraQueryPlanType = ???


  override protected def rangeExact(row: Array[Byte]): String = ???

  override def delete(sft: SimpleFeatureType, ds: CassandraDataStore, shared: Boolean): Unit = ???

  override protected def rangePrefix(prefix: Array[Byte]): String = ???
}
