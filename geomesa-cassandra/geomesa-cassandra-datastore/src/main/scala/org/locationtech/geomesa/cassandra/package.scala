

package org.locationtech.geomesa

import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, GeoMesaIndexManager}
import org.locationtech.geomesa.index.geotools.{GeoMesaDataStore, GeoMesaFeatureWriter}

package object cassandra {

  type CassandraDataStoreType = GeoMesaDataStore[CassandraDataStore, CassandraFeature, Any, Any]
  type CassandraIndexManagerType = GeoMesaIndexManager[CassandraDataStore, CassandraFeature, Any, Any]

  type CassandraFeatureIndexType = GeoMesaFeatureIndex[CassandraDataStore, CassandraFeature, Any, Any]

  type CassandraFeatureWriterType = GeoMesaFeatureWriter[CassandraDataStore, CassandraFeature, Any, Any, Any]

}