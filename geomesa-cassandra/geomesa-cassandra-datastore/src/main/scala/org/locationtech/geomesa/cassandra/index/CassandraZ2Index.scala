/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.index

import com.datastax.driver.core.Row
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.index.index.{Z2Index, Z3Index}

case object CassandraZ2Index
    extends CassandraFeatureIndex with Z2Index[CassandraDataStore, CassandraFeature, (String, String), Row, Array[Byte]] {
  override val version: Int = 1
}
