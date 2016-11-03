package org.locationtech.geomesa.cassandra.index


import com.datastax.driver.core.SimpleStatement
import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra.{CassandraFeatureIndexType, CassandraFilterStrategyType, CassandraIndexManagerType, CassandraQueryPlanType}
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, GeoMesaIndexManager, QueryPlan}
import org.locationtech.geomesa.index.index.IndexAdapter
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter


object CassandraFeatureIndex extends CassandraIndexManagerType {

  // note: keep in priority order for running full table scans
  override def AllIndices: Seq[CassandraFeatureIndex] =
      Seq(CassandraZ3Index, CassandraIdIndex)

  override val CurrentIndices: Seq[CassandraFeatureIndex] = AllIndices

}


trait CassandraFeatureIndex extends CassandraFeatureIndexType
    with IndexAdapter[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer, String] {

  override def configure(sft: SimpleFeatureType, ds: CassandraDataStore): Unit = {

    super.configure(sft, ds)

    val name = getTableName(sft.getTypeName, ds)

    ds.session.execute(s"""CREATE TABLE IF NOT EXISTS $name (idx blob, feature blob, PRIMARY KEY (idx))""")

  }

  override protected def createInsert(row: Array[Byte], feature: CassandraFeature): (Array[Byte], CassandraFeature) = {
    //currently this is no-op
    //todo: determine if there is a reasonable thing to do here
    return (row, feature)
  }

  override protected def range(start: Array[Byte], end: Array[Byte]): String = ???

  override protected def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Any) => SimpleFeature = ???

  override protected def createDelete(row: Array[Byte], feature: CassandraFeature): (Array[Byte], CassandraFeature) = ???

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: CassandraDataStore,
                                  filter: CassandraFilterStrategyType,
                                  hints: Hints,
                                  ranges: Seq[String],
                                  ecql: Option[Filter]): CassandraQueryPlanType = ???

  override protected def rangeExact(row: Array[Byte]): String = ???


  override def delete(sft: SimpleFeatureType, ds: CassandraDataStore, shared: Boolean): Unit = ???

  override protected def rangePrefix(prefix: Array[Byte]): String = ???
}


