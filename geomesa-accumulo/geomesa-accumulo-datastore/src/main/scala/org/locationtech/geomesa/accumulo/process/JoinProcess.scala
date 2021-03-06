/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.collection.DecoratingSimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.feature.{AttributeTypeBuilder, DefaultFeatureCollection}
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.VectorProcess
import org.geotools.util.NullProgressListener
import org.locationtech.geomesa.accumulo.process.query.{QueryResult, QueryVisitor}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.util.ProgressListener

/**
  * Returns features from a feature type based on a join against a second feature type.
  */
@DescribeProcess(
  title = "Join Process",
  description = "Queries a feature type based on attributes from a second feature type"
)
class JoinProcess extends VectorProcess with LazyLogging {

  /**
    *
    * @param primary main feature collection to query
    * @param secondary secondary feature collection to query with results from first feature collection
    * @param joinAttribute attribute to join on
    * @param joinFilter additional filter to apply to joined features
    * @param attributes attributes to return, from both collections, qualified by schema name
    * @param bins flag to return results in BIN format
    * @param binDtg date field for bin records - will use default date if not specified
    * @param binTrackId track ID field for bin records - will not include trackId if not specified
    * @param binLabel label field for bin record (optional)
    * @param monitor listener to monitor progress
    * @throws org.geotools.process.ProcessException if something goes wrong
    * @return
    */
  @throws(classOf[ProcessException])
  @DescribeResult(name = "result", description = "Output features")
  def execute(@DescribeParameter(name = "primary", description = "Primary feature collection being queried", min = 1)
              primary: SimpleFeatureCollection,
              @DescribeParameter(name = "secondary", description = "Secondary feature collection to be joined", min = 1)
              secondary: SimpleFeatureCollection,
              @DescribeParameter(name = "joinAttribute", description = "Attribute field to join on", min = 1)
              joinAttribute: String,
              @DescribeParameter(name = "joinFilter", description = "Additional filter to apply to joined features", min = 0)
              joinFilter: Filter,
              @DescribeParameter(name = "attributes", description = "Attributes to return. Attribute names should be qualified with the schema name, e.g. foo.bar", min = 0, max = 128, collectionType = classOf[String])
              attributes: java.util.List[String],
              @DescribeParameter(name = "bins", description = "Return BIN records instead of regular records", min = 0)
              bins: java.lang.Boolean,
              @DescribeParameter(name = "binDtg", description = "Date field to use for BIN records", min = 0)
              binDtg: String,
              @DescribeParameter(name = "binTrackId", description = "Track field to use for BIN records", min = 0)
              binTrackId: String,
              @DescribeParameter(name = "binLabel", description = "Label field to use for BIN records", min = 0)
              binLabel: String,
              monitor: ProgressListener): SimpleFeatureCollection = {

    import org.locationtech.geomesa.filter.ff

    import scala.collection.JavaConversions._

    logger.trace(s"Attempting join query on ${joinAttribute.getClass.getName}")

    if (primary.isInstanceOf[ReTypingFeatureCollection] || secondary.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    require(primary.getSchema.getDescriptor(joinAttribute) != null,
      s"Attribute '$joinAttribute' does not exist in the primary feature collection")

    val joinDescriptor = secondary.getSchema.getDescriptor(joinAttribute)
    require(joinDescriptor != null, s"Attribute '$joinAttribute' does not exist in the joined feature collection")

    val binOptions = if (bins != null && bins) {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      def toDtg(schema: SimpleFeatureType) = schema.getDtgField.map(a => s"${schema.getTypeName}.$a")
      def toTrackId(schema: SimpleFeatureType) = schema.getBinTrackId.map(a => s"${schema.getTypeName}.$a")

      val dtg = Option(binDtg).orElse(toDtg(secondary.getSchema)).orElse(toDtg(primary.getSchema)).getOrElse {
        throw new IllegalArgumentException("Please specify binDtg for BIN output format")
      }
      val track = Option(binTrackId).orElse(toTrackId(secondary.getSchema)).orElse(toTrackId(primary.getSchema))
      Some(EncodingOptions(dtg, track, Option(binLabel)))
    } else {
      None
    }

    // create the return sft based on the input attributes, or by combining the qualified names from each schema
    val returnSft = if (attributes != null && attributes.nonEmpty) {
      getCombinedSft(primary.getSchema, secondary.getSchema, attributes, joinAttribute)
    } else {
      def toAttributes(schema: SimpleFeatureType): Seq[String] = {
        val names = schema.getAttributeDescriptors.map(_.getLocalName)
        names.filter(_ != joinAttribute).map(d => s"${schema.getTypeName}.$d")
      }
      val primaryAttributes = toAttributes(primary.getSchema)
      val secondaryAttributes = toAttributes(secondary.getSchema)
      val attributes = Seq(joinAttribute) ++ primaryAttributes ++ secondaryAttributes
      getCombinedSft(primary.getSchema, secondary.getSchema, attributes, joinAttribute)
    }

    // check for too many features coming back - limit is somewhat arbitrary, but this
    // class is mainly intended for a single feature lookup
    val primaryFeatures = SelfClosingIterator(primary).toList
    require(primaryFeatures.length < 129,
      s"Too many features returned from primary query - got ${primaryFeatures.length}, max 128")

    val joinProperty = ff.property(joinAttribute)
    val joinFilters = {
      val values = primaryFeatures.map(_.getAttribute(joinAttribute)).distinct
      values.map(p => ff.equals(joinProperty, ff.literal(p)))
    }

    val result = if (joinFilters.isEmpty) {
      new DefaultFeatureCollection(null, returnSft)
    } else {
      val or = ff.or(joinFilters.toList)
      val filter = if (joinFilter != null && joinFilter != Filter.INCLUDE) { ff.and(or, joinFilter) } else { or }
      val visitor = new QueryVisitor(secondary, filter)
      secondary.accepts(visitor, new NullProgressListener)
      val results = visitor.getResult.asInstanceOf[QueryResult].results

      // mappings from the secondary feature result to the return schema
      // (return sft index, result sft index, is from primary result (or secondary result))
      val attributeMappings: Seq[(Int, Int, Boolean)] = returnSft.getAttributeDescriptors.map { d =>
        val toAttribute = d.getLocalName
        val dot = toAttribute.indexOf('.')
        if (dot == -1) {
          val fromPrimary = secondary.getSchema.getAttributeDescriptors.exists(_.getLocalName == toAttribute)
          val from = if (fromPrimary) {
            primary.getSchema.indexOf(toAttribute)
          } else {
            secondary.getSchema.indexOf(toAttribute)
          }
          (returnSft.indexOf(toAttribute), from, fromPrimary)
        } else {
          val fromPrimary = primary.getSchema.getTypeName == toAttribute.substring(0, dot)
          val from = if (fromPrimary) {
            primary.getSchema.indexOf(toAttribute.substring(dot + 1))
          } else {
            secondary.getSchema.indexOf(toAttribute.substring(dot + 1))
          }
          (returnSft.indexOf(toAttribute), from, fromPrimary)
        }
      }

      new DecoratingSimpleFeatureCollection(results) {
        override def getSchema: SimpleFeatureType = returnSft
        override def features(): SimpleFeatureIterator = new SimpleFeatureIterator {
          val delegate = results.features
          override def next(): SimpleFeature = {
            val secondarySf = delegate.next()
            val toJoin = secondarySf.getAttribute(joinAttribute)
            val primarySf = primaryFeatures.find(_.getAttribute(joinAttribute) == toJoin).getOrElse {
              throw new RuntimeException("No feature joined from attribute query")
            }
            val sf = new ScalaSimpleFeature(s"${primarySf.getID}-${secondarySf.getID}" , returnSft)
            attributeMappings.foreach { case (to, from, fromPrimary) =>
              val a = if (fromPrimary) { primarySf.getAttribute(from) } else { secondarySf.getAttribute(from) }
              sf.setAttribute(to, a)
            }
            sf
          }
          override def hasNext: Boolean = delegate.hasNext
          override def close(): Unit = delegate.close()
        }
      }
    }

    // pass bin parameters off to the output format
    binOptions match {
      case None    => BinaryOutputEncoder.CollectionEncodingOptions.remove(result.getID)
      case Some(o) => BinaryOutputEncoder.CollectionEncodingOptions.put(result.getID, o)
    }

    result
  }

  /**
    * Builds a combined sft
    *
    * @param primary primary sft being joined
    * @param secondary secondary sft being joined
    * @param attributes attributes to include - must be sft qualified if ambiguous
    * @param join attribute being joined on - this will always be unqualified and should exist in both sfts
    * @return
    */
  private def getCombinedSft(primary: SimpleFeatureType,
                             secondary: SimpleFeatureType,
                             attributes: Seq[String],
                             join: String): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.setName(s"${primary.getTypeName}_join_${secondary.getTypeName}")

    val descriptorBuilder = new AttributeTypeBuilder()

    attributes.foreach { attribute =>
      val dot = attribute.indexOf('.')
      val descriptor = if (dot == -1) {
        val primaryDescriptor = primary.getDescriptor(attribute)
        val secondaryDescriptor = secondary.getDescriptor(attribute)
        if (primaryDescriptor == null || attribute == join) {
          secondaryDescriptor
        } else if (secondaryDescriptor == null) {
          primaryDescriptor
        } else {
          throw new IllegalArgumentException(s"Ambiguous property requested: $attribute exists in both schemas")
        }
      } else {
        val typeName = attribute.substring(0, dot)
        if (typeName == primary.getTypeName) {
          primary.getDescriptor(attribute.substring(dot + 1))
        } else if (typeName == secondary.getTypeName) {
          secondary.getDescriptor(attribute.substring(dot + 1))
        } else {
          null
        }
      }

      if (descriptor != null) {
        descriptorBuilder.init(descriptor)
        val toAdd = descriptorBuilder.buildDescriptor(attribute)
        builder.add(toAdd)
        if (descriptor == secondary.getGeometryDescriptor) {
          builder.setDefaultGeometry(toAdd.getLocalName)
        }
      }
    }

    builder.buildFeatureType()
  }
}
