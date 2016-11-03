package org.locationtech.geomesa.cassandra.data

import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.index.api.WrappedFeature
import org.opengis.feature.simple.SimpleFeature

class CassandraFeature(val feature: SimpleFeature, serializer: SimpleFeatureSerializer) extends WrappedFeature {

  lazy val value = serializer.serialize(feature)

}
