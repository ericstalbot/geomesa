package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.Session
import com.typesafe.scalalogging.LazyLogging

import org.locationtech.geomesa.index.utils.{GeoMesaMetadata, MetadataSerializer}

import scala.collection.JavaConversions._


class CassandraBackedMetaData[T](session: Session, catalog: String, serializer: MetadataSerializer[T])
  extends GeoMesaMetadata[T] with LazyLogging {


  /**
    * Returns existing simple feature types
    *
    * @return simple feature type names
    */
  override def getFeatureTypes: Array[String] = {
    val rows = session.execute(s"SELECT typeName FROM $catalog")
    rows.iterator().map(_.getString("typeName")).toArray.distinct
  }

  /**
    * Insert a value - any existing value under the given key will be overwritten
    *
    * @param typeName simple feature type name
    * @param key      key
    * @param value    value
    */
  override def insert(typeName: String, key: String, value: T): Unit = {
    session.execute(s"INSERT INTO $catalog (typeName, key, value) VALUES ($typeName, $key, $value)")
    //todo: over write value if key already exists
    //todo: when/where do we run CREATE TABLE?
    //todo: query parameterization to prevent injection
  }


  /**
    * Insert multiple values at once - may be more efficient than single inserts
    *
    * @param typeName simple feature type name
    * @param kvPairs  key/values
    */
  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = {
    kvPairs.foreach { case (k, v) =>
      insert(typeName, k, v)
    }
  }

  /**
    * Delete a key
    *
    * @param typeName simple feature type name
    * @param key      key
    */
  override def remove(typeName: String, key: String): Unit = {
    session.execute(s"DELETE FROM $catalog WHERE (typeName == $typeName) && (key == $key)")
  }

  /**
    * Reads a value
    *
    * @param typeName simple feature type name
    * @param key      key
    * @param cache    may return a cached value if true, otherwise may use a slower lookup
    * @return value, if present
    */
  override def read(typeName: String, key: String, cache: Boolean): Option[T] = {
    val rows = session.execute(s"select value from $catalog where ($typeName == typeName) and ($key == key)")
    Option(rows.one().getString("value").asInstanceOf[T])
  }

  /**
    * Invalidates any cached value for the given key
    *
    * @param typeName simple feature type name
    * @param key      key
    */
  override def invalidateCache(typeName: String, key: String): Unit = {}

  /**
    * Deletes all values associated with a given feature type
    *
    * @param typeName simple feature type name
    */
  override def delete(typeName: String): Unit = {
    session.execute(s"DELETE FROM $catalog WHERE typeName == $typeName")
  }
}





