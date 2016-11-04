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

package object cassandra {                                                                                            //Integer here is a placeholder
  type CassandraDataStoreType = GeoMesaDataStore[CassandraDataStore, CassandraFeature, (String, String), Integer]
  type CassandraFeatureIndexType = GeoMesaFeatureIndex[CassandraDataStore, CassandraFeature, (String, String), Integer]
  type CassandraFilterPlanType = FilterPlan[CassandraDataStore, CassandraFeature, (String, String), Integer]
  type CassandraFilterStrategyType = FilterStrategy[CassandraDataStore, CassandraFeature, (String, String), Integer]
  type CassandraQueryPlannerType = QueryPlanner[CassandraDataStore, CassandraFeature, (String, String), Integer]
  type CassandraQueryPlanType = QueryPlan[CassandraDataStore, CassandraFeature, (String, String), Integer]
  type CassandraIndexManagerType = GeoMesaIndexManager[CassandraDataStore, CassandraFeature, (String, String), Integer]
  type CassandraFeatureWriterType = GeoMesaFeatureWriter[CassandraDataStore, CassandraFeature, (String, String), Integer, String]
  type CassandraAppendFeatureWriterType = GeoMesaAppendFeatureWriter[CassandraDataStore, CassandraFeature, (String, String), Integer, String]
  type CassandraModifyFeatureWriterType = GeoMesaModifyFeatureWriter[CassandraDataStore, CassandraFeature, (String, String), Integer, String]

  object CassandraSystemProperties {
    val WriteBatchSize = SystemProperty("geomesa.cassandra.write.batch", null)  //not sure if this is used anywhere
  }
}
