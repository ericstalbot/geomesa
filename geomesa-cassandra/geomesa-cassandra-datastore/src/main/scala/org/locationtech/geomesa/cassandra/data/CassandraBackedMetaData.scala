/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.Session
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.index.utils.{GeoMesaMetadata, MetadataSerializer}

import scala.collection.JavaConversions._


class CassandraBackedMetaData[T](session: Session, catalog: String, serializer: MetadataSerializer[T])
  extends GeoMesaMetadata[T] with LazyLogging {

  override def getFeatureTypes: Array[String] = {
    ensureTableExists()
    val rows = session.execute(s"SELECT typeName FROM $catalog")
    rows.iterator().map(_.getString("typeName")).toArray.distinct
  }

  override def insert(typeName: String, key: String, value: T): Unit = {
    ensureTableExists()
    remove(typeName, key)
    val q = s"INSERT INTO $catalog (typeName, key, value) VALUES (?, ?, ?)"
    session.execute(q, typeName, key, value.asInstanceOf[String])
  }

  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = {
    kvPairs.foreach { case (k, v) =>
      insert(typeName, k, v)
    }
  }

  override def remove(typeName: String, key: String): Unit = {
    ensureTableExists()
    val q = s"DELETE FROM $catalog WHERE (typeName = ?) and (key = ?)"
    session.execute(q, typeName, key)
  }

  override def read(typeName: String, key: String, cache: Boolean): Option[T] = {
    ensureTableExists()
    val q = s"select value from $catalog where (typeName = ?) and (key = ?)"
    val row = Option(session.execute(q, typeName, key).one())
    row match {
      case Some(row_) => Option(row_.getString("value").asInstanceOf[T])
      case None => None
    }
  }

  override def invalidateCache(typeName: String, key: String): Unit = ???

  override def delete(typeName: String): Unit = {
    ensureTableExists()
    val q = s"DELETE FROM $catalog WHERE typeName = ?"
    session.execute(q, typeName)
  }

  def ensureTableExists(): Unit = {
    session.execute(s"CREATE TABLE IF NOT EXISTS $catalog (typeName text, key text, value text, PRIMARY KEY (typeName, key))")
  }

}




