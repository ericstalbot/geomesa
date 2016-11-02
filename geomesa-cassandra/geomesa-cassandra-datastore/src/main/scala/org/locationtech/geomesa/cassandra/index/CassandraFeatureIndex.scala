package org.locationtech.geomesa.cassandra.index

import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, GeoMesaIndexManager, QueryPlan}
import org.locationtech.geomesa.index.index.IndexAdapter
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Created by etalbot on 11/2/16.
  */
object CassandraFeatureIndex extends GeoMesaIndexManager[CassandraDataStore, CassandraFeature, Any, Any] {

  // note: keep in priority order for running full table scans
  override def AllIndices: Seq[CassandraFeatureIndex] =
      Seq(CassandraZ3Index, CassandraIdIndex)

  override val CurrentIndices: Seq[CassandraFeatureIndex] = AllIndices

}


trait CassandraFeatureIndex extends GeoMesaFeatureIndex[CassandraDataStore, CassandraFeature, Any, Any]
  with IndexAdapter[CassandraDataStore, CassandraFeature, Any, Any, Any] {
  /**
    * The name used to identify the index
    */
  override def name: String = ???

  /**
    * Current version of the index
    *
    * @return
    */
  override def version: Int = ???

  /**
    * Is the index compatible with the given feature type
    *
    * @param sft simple feature type
    * @return
    */
  override def supports(sft: SimpleFeatureType): Boolean = ???

  /**
    * Creates a function to write a feature to the index
    *
    * @param sft simple feature type
    * @param ds  data store
    * @return
    */
  override def writer(sft: SimpleFeatureType, ds: CassandraDataStore): (CassandraFeature) => Any = ???

  /**
    * Creates a function to delete a feature from the index
    *
    * @param sft simple feature type
    * @param ds  data store
    * @return
    */
  override def remover(sft: SimpleFeatureType, ds: CassandraDataStore): (CassandraFeature) => Any = ???

  /**
    * Deletes the entire index
    *
    * @param sft    simple feature type
    * @param ds     data store
    * @param shared true if this index shares physical space with another (e.g. shared tables)
    */
  override def delete(sft: SimpleFeatureType, ds: CassandraDataStore, shared: Boolean): Unit = ???

  /**
    * Gets options for a 'simple' filter, where each OR is on a single attribute, e.g.
    * (bbox1 OR bbox2) AND dtg
    * bbox AND dtg AND (attr1 = foo OR attr = bar)
    * not:
    * bbox OR dtg
    *
    * Because the inputs are simple, each one can be satisfied with a single query filter.
    * The returned values will each satisfy the query.
    *
    * @param filter input filter
    * @return sequence of options, any of which can satisfy the query
    */
  override def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[TypedFilterStrategy] = ???

  /**
    * Gets the estimated cost of running the query. In general, this is the estimated
    * number of features that will have to be scanned.
    */
  override def getCost(sft: SimpleFeatureType, ds: Option[CassandraDataStore], filter: TypedFilterStrategy, transform: Option[SimpleFeatureType]): Long = ???

  /**
    * Plans the query
    */
  override def getQueryPlan(sft: SimpleFeatureType, ds: CassandraDataStore, filter: TypedFilterStrategy, hints: Hints, explain: Explainer): QueryPlan[CassandraDataStore, CassandraFeature, Any, Any] = ???

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String = ???

  override protected def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Any) => SimpleFeature = ???

  override protected def createInsert(row: Array[Byte], feature: CassandraFeature): Any = ???

  override protected def createDelete(row: Array[Byte], feature: CassandraFeature): Any = ???

  override protected def range(start: Array[Byte], end: Array[Byte]): Any = ???

  override protected def rangeExact(row: Array[Byte]): Any = ???

  override protected def rangePrefix(prefix: Array[Byte]): Any = ???

  override protected def scanPlan(sft: SimpleFeatureType, ds: CassandraDataStore, filter: FilterStrategy[CassandraDataStore, CassandraFeature, Any, Any], hints: Hints, ranges: Seq[Any], ecql: Option[Filter]): QueryPlan[CassandraDataStore, CassandraFeature, Any, Any] = ???
}