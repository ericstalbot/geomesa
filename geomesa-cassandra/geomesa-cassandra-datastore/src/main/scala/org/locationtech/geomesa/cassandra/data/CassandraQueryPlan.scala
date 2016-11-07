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



  //val insert = session.prepare(s"INSERT INTO ${sft.getTypeName} (pkz, z31, fid, ${cols.mkString(",")}) values (${Seq.fill(3+cols.length)("?").mkString(",")})")



  override def scan(ds: CassandraDataStore): CloseableIterator[Row] = {
    import scala.collection.JavaConversions._
    val q = s"select * from $table where rowid IN (${Seq.fill(ranges.length)("?").mkString(",")})"
    println(q)
    println(ranges)
    val r = ranges.map(Bytes.toHexString(_))
    println(r)
    CloseableIterator(ds.session.execute(q, r: _*).iterator())
  }

  override def explain(explainer: Explainer, prefix: String): Unit = {
    println("the do nothing explainer")
  }
}
