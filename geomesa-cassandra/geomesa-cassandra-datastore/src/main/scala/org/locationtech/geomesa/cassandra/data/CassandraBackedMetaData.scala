package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.Session
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.index.utils.GeoMesaMetadata

import scala.collection.JavaConversions._

class CassandraBackedMetaData(session: Session, catalog: String) extends GeoMesaMetadata[String] with LazyLogging {

  override def getFeatureTypes: Array[String] = {
    ensureTableExists()
    session.execute(s"SELECT typeName FROM $catalog").all().map(_.getString("typeName")).toArray.distinct
  }

  override def insert(typeName: String, key: String, value: String): Unit = {
    ensureTableExists()
    session.execute(s"INSERT INTO $catalog (typeName, key, value) VALUES (?, ?, ?)", typeName, key, value)
  }

  override def insert(typeName: String, kvPairs: Map[String, String]): Unit = {
    ensureTableExists()
    kvPairs foreach {case (k, v) => insert(typeName, k, v)}
  }

  override def remove(typeName: String, key: String): Unit = ???

  override def read(typeName: String, key: String, cache: Boolean): Option[String] = {
    val r = session.execute(s"SELECT  value FROM $catalog WHERE typeName = ? AND key = ?", typeName, key).all()
    if (r.length < 1) {
      None
    } else {
      Option(r.head.getString("value"))
    }
  }

  override def invalidateCache(typeName: String, key: String): Unit = ???

  override def delete(typeName: String): Unit = ???

  def ensureTableExists(): Unit = {
    session.execute(s"CREATE TABLE IF NOT EXISTS $catalog (typeName text, key text, value text, PRIMARY KEY (typeName, key))")
  }
}
