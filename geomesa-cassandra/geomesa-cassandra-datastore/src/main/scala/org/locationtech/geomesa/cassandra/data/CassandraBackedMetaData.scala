package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.InvalidQueryException
import com.datastax.driver.core.exceptions.InvalidQueryException._
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.index.utils.{GeoMesaMetadata, MetadataSerializer}

import scala.util.{Try,Success,Failure}

import scala.collection.JavaConversions._


class CassandraBackedMetaData[T](session: Session, catalog: String, serializer: MetadataSerializer[T])
  extends GeoMesaMetadata[T] with LazyLogging {


  /**
    * Returns existing simple feature types
    *
    * @return simple feature type names
    */
  override def getFeatureTypes: Array[String] = {
    ensureTableExists()
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
    ensureTableExists()
    remove(typeName, key)
    val escValue = value.toString().replace("'", "''")
    val s = s"INSERT INTO $catalog (typeName, key, value) VALUES ('$typeName', '$key', '$escValue')"
    println(s)
    session.execute(s)
    //todo: query parameterization to prevent injection
    //todo: also escape special characters [ ]
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
    session.execute(s"""DELETE FROM $catalog WHERE (typeName = '$typeName') and (key = '$key')""")
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
    Try {
      val s = s"""select value from $catalog where (typeName = '$typeName') and (key = '$key')"""
      session.execute(s)
    } match {
      case Success(rows) => Option(rows.one().getString("value").asInstanceOf[T])
      case Failure(f) => None
    }
  }
  //select value from mycatalog where (testType = typeName) and (attributes = key)
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


  def ensureTableExists(): Unit = {
    println("//////////////////////////////////////////making table")
    session.execute(s"CREATE TABLE IF NOT EXISTS $catalog (typeName text, key text, value text, PRIMARY KEY (typeName, key))")
  }


}


//mvn <goals> -rf :geomesa-cassandra-datastore_2.11


