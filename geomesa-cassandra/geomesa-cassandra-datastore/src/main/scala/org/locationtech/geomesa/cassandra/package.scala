/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.{GeoMesaAppendFeatureWriter, GeoMesaDataStore, GeoMesaFeatureWriter, GeoMesaModifyFeatureWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object cassandra {
  type CassandraDataStoreType = GeoMesaDataStore[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer]
  type CassandraFeatureIndexType = GeoMesaFeatureIndex[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer]
  type CassandraFilterPlanType = FilterPlan[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer]
  type CassandraFilterStrategyType = FilterStrategy[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer]
  type CassandraQueryPlannerType = QueryPlanner[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer]
  type CassandraQueryPlanType = QueryPlan[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer]
  type CassandraIndexManagerType = GeoMesaIndexManager[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer]
  type CassandraFeatureWriterType = GeoMesaFeatureWriter[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer, String]
  type CassandraAppendFeatureWriterType = GeoMesaAppendFeatureWriter[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer, String]
  type CassandraModifyFeatureWriterType = GeoMesaModifyFeatureWriter[CassandraDataStore, CassandraFeature, (Array[Byte], CassandraFeature), Integer, String]

  object CassandraSystemProperties {
    val WriteBatchSize = SystemProperty("geomesa.cassandra.write.batch", null)
  }
}
