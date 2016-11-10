/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.io.Serializable
import java.net.URI
import java.util

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, TokenAwarePolicy}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{AbstractDataStoreFactory, DataStore, Parameter}
import org.geotools.util.KVP
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.CassandraDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}

class CassandraDataStoreFactory extends AbstractDataStoreFactory {
  import CassandraDataStoreFactory.Params._

  override def createDataStore(map: util.Map[String, Serializable]): DataStore = {
    import GeoMesaDataStoreFactory.RichParam

    val Array(cp, port) = CONTACT_POINT.lookUp(map).asInstanceOf[String].split(":")
    val ks = KEYSPACE.lookUp(map).asInstanceOf[String]
    val ns = NAMESPACE.lookUp(map).asInstanceOf[URI]
    val cluster =
      Cluster.builder()
        .addContactPoint(cp)
        .withPort(port.toInt)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .build()
    val session = cluster.connect(ks)

    val catalog = CatalogNameParam.lookup[String](map)
    val generateStats = GenerateStatsParam.lookupWithDefault[Boolean](map)
    val audit = if (AuditQueriesParam.lookupWithDefault[Boolean](map)) {
      Some(AuditLogger, Option(AuditProvider.Loader.load(map)).getOrElse(NoOpAuditProvider), "cassandra")
    } else {
      None
    }
    val queryThreads = QueryThreadsParam.lookupWithDefault[Int](map)
    val queryTimeout = GeoMesaDataStoreFactory.queryTimeout(map)
    val looseBBox = LooseBBoxParam.lookupWithDefault[Boolean](map)
    val caching = CachingParam.lookupWithDefault[Boolean](map)

    val config = CassandraDataStoreConfig(catalog, generateStats, audit, queryThreads, queryTimeout, looseBBox, caching)

    new CassandraDataStore(session, cluster.getMetadata.getKeyspace(ks), ns, config)
  }

  override def createNewDataStore(map: util.Map[String, Serializable]): DataStore = ???

  override def getDisplayName: String = "Cassandra (GeoMesa)"

  override def getDescription: String = "GeoMesa Cassandra Data Store"

  override def getParametersInfo: Array[Param] = Array(CONTACT_POINT, KEYSPACE, NAMESPACE)
}

object CassandraDataStoreFactory {

  object Params {
    val CONTACT_POINT = new Param("geomesa.cassandra.contact.point", classOf[String], "HOST:PORT to Cassandra", true)
    val KEYSPACE = new Param("geomesa.cassandra.keyspace", classOf[String], "Cassandra Keyspace", true)
    val NAMESPACE = new Param("namespace", classOf[URI], "uri to a the namespace", false, null, new KVP(Parameter.LEVEL, "advanced"))
    val CatalogNameParam = new Param("catalog", classOf[String])
    val LooseBBoxParam     = GeoMesaDataStoreFactory.LooseBBoxParam
    val QueryThreadsParam  = GeoMesaDataStoreFactory.QueryThreadsParam
    val GenerateStatsParam = GeoMesaDataStoreFactory.GenerateStatsParam
    val AuditQueriesParam  = GeoMesaDataStoreFactory.AuditQueriesParam
    val QueryTimeoutParam  = GeoMesaDataStoreFactory.QueryTimeoutParam
    val CachingParam       = GeoMesaDataStoreFactory.CachingParam
  }

  case class CassandraDataStoreConfig(catalog: String,
                                      generateStats: Boolean,
                                      audit: Option[(AuditWriter, AuditProvider, String)],
                                      queryThreads: Int,
                                      queryTimeout: Option[Long],
                                      looseBBox: Boolean,
                                      caching: Boolean) extends GeoMesaDataStoreConfig

}
