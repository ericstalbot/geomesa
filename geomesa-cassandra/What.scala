import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params._
import scala.collection.JavaConversions._


import com.vividsolutions.jts.geom._
import org.geotools.feature.simple.SimpleFeatureTypeBuilder

import org.geotools.data.Transaction

println("heyhey")

val params = Map[String, String](

  CONTACT_POINT.getName -> "127.0.0.1:9042",
  KEYSPACE.getName -> "mykeyspace",
  NAMESPACE.getName -> "mynamespace",
  CATALOG.getName -> "mycatalog",
  LooseBBoxParam.getName -> "",
  QueryThreadsParam.getName -> "",
  GenerateStatsParam.getName -> "",
  AuditQueriesParam.getName -> "",
  QueryTimeoutParam.getName -> "",
  CachingParam.getName -> ""

)

val ds = DataStoreFinder.getDataStore(params)


//create the builder
val builder = new SimpleFeatureTypeBuilder()

//set global state
builder.setName( "testType" )
builder.setNamespaceURI( "http://www.geotools.org/" )
//builder.setSRS( "EPSG:4326" )

//add attributes
builder.add( "intProperty", classOf[java.lang.Integer] )
builder.add( "pointProperty", classOf[Point] )

//build the type
val sft = builder.buildFeatureType();

ds.createSchema(sft)

val fw = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)

