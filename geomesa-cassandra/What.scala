import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params._
import scala.collection.JavaConversions._


import com.vividsolutions.jts.geom._
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.feature.simple.{SimpleFeatureTypeBuilder, SimpleFeatureBuilder}

import org.geotools.data.Transaction

import org.locationtech.geomesa.cassandra.data.CassandraAppendFeatureWriter



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


//////////////////////////
//create the builder
val builder = new SimpleFeatureTypeBuilder()

//set global state
builder.setName( "testType2" )
builder.setNamespaceURI( "http://www.geotools.org/" )
//builder.setSRS( "EPSG:4326" )

//add attributes
builder.add( "intProperty", classOf[java.lang.Integer] )
builder.add( "pointProperty", classOf[Point] )

//build the type
val sft = builder.buildFeatureType();

ds.createSchema(sft)

///////////////////////////







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

val fw = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT).asInstanceOf[CassandraAppendFeatureWriter]




val geometry_factory = JTSFactoryFinder.getGeometryFactory(null)

val point = geometry_factory.createPoint(new Coordinate(0, 0))



val feature_builder = new SimpleFeatureBuilder(sft)


feature_builder.add(1)
feature_builder.add(point)

val feature = feature_builder.buildFeature("fid")

//fw.writeFeature(feature)

fw.currentFeature = feature
fw.write()

