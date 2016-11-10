package org.locationtech.geomesa.cassandra.index

import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.index.index.IdIndex
import org.opengis.feature.simple.SimpleFeatureType


case object CassandraIdIndex

    extends CassandraFeatureIndex with IdIndex[CassandraDataStore, CassandraFeature, Any, Any, Any] {

  override val version: Int = 1

  override def configure(sft: SimpleFeatureType, ds: CassandraDataStore): Unit = {
    super.configure(sft, ds)
    val name = getTableName(sft.getTypeName, ds)
    ds.session.execute(s"CREATE TABLE IF NOT EXISTS $name (featureId blob, feature blob, PRIMARY KEY (featureId))")
  }






}
