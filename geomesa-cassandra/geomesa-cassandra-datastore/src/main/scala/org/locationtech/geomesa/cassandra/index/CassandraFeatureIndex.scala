package org.locationtech.geomesa.cassandra.index

import com.datastax.driver.core.utils.Bytes
import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra.{CassandraFeatureIndexType, CassandraIndexManagerType}
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.index.IndexAdapter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter


object CassandraFeatureIndex extends CassandraIndexManagerType {
  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[CassandraFeatureIndex] =
    Seq(CassandraIdIndex)

  override val CurrentIndices: Seq[CassandraFeatureIndex] = AllIndices
}


trait CassandraFeatureIndex extends CassandraFeatureIndexType
    with IndexAdapter[CassandraDataStore, CassandraFeature, Any, Any, Any] {
  override protected def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Any) => SimpleFeature = ???


  override def delete(sft: SimpleFeatureType, ds: CassandraDataStore, shared: Boolean): Unit = ???

  override protected def createInsert(row: Array[Byte], feature: CassandraFeature): Any = {
    (Bytes.toHexString(row), Bytes.toHexString(feature.value))
  }

  override protected def createDelete(row: Array[Byte], feature: CassandraFeature): Any = ???

  override protected def scanPlan(sft: SimpleFeatureType, ds: CassandraDataStore, filter: FilterStrategy[CassandraDataStore, CassandraFeature, Any, Any], hints: Hints, ranges: Seq[Any], ecql: Option[Filter]): QueryPlan[CassandraDataStore, CassandraFeature, Any, Any] = ???

  // range with start row included and end row excluded. no start/end is indicated by an empty byte array.
  override protected def range(start: Array[Byte], end: Array[Byte]): Any = ???

  override protected def rangeExact(row: Array[Byte]): Any = ???
}
