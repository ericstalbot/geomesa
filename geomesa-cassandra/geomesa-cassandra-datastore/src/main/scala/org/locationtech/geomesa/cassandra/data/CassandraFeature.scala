package org.locationtech.geomesa.cassandra.data

import org.locationtech.geomesa.index.api.WrappedFeature
import org.opengis.feature.simple.SimpleFeature

class CassandraFeature(val feature: SimpleFeature) extends WrappedFeature {}
