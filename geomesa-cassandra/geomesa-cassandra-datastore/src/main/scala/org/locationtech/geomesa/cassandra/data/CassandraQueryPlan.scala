package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.Bytes
import org.locationtech.geomesa.cassandra.{CassandraFilterStrategyType, CassandraQueryPlanType}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature





case class WhateverQueryPlan(filter: CassandraFilterStrategyType,
                             table: String,
                             ranges: Seq[Array[Byte]],
                             entriesToFeatures: (Row) => SimpleFeature) extends CassandraQueryPlanType {

    override def scan(ds: CassandraDataStore): CloseableIterator[Row] = {
    import scala.collection.JavaConversions._

    val q = s"select rowid, blobAsText(feature) as feature from $table where rowid IN (${Seq.fill(ranges.length)("?").mkString(",")})"

    val r = ranges.map(Bytes.toHexString(_))

    CloseableIterator(ds.session.execute(q, r: _*).iterator())
  }

  override def explain(explainer: Explainer, prefix: String): Unit = {
    println("the do nothing explainer")
  }
}
