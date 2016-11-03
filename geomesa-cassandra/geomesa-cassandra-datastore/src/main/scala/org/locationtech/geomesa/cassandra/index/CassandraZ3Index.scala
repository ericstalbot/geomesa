package org.locationtech.geomesa.cassandra.index




import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}


import org.locationtech.geomesa.index.index.Z3Index




case object CassandraZ3Index
    extends CassandraFeatureIndex with Z3Index[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer, String] {
  override val version: Int = 1


}