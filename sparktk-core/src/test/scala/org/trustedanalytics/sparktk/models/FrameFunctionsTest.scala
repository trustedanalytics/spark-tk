/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.sparktk.models

import org.apache.spark.ml.attribute.{ NominalAttribute, NumericAttribute, AttributeGroup }
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.testutils._
import FrameImplicits._
class FrameFunctionsTest extends TestingSparkContextWordSpec with Matchers {

  "FrameFunctions" should {
    "convert to labeled dataframe with categorical label" in {
      val schema = FrameSchema(Vector(Column("age", DataTypes.int32), Column("weight", DataTypes.float64),
        Column("gender", DataTypes.int32), Column("has_diabetes", DataTypes.int32)))
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](20, 130.0, 1, 0)))

      val frameRDD = new FrameRdd(schema, sparkContext.parallelize(rows))
      val labelCol = "has_diabetes"
      val observationCols = List("age", "weight", "gender")
      val categoricalFeatures = Map("gender" -> 2)

      // Convert to labeled frame with categorical label
      val dfWithCategoricalLabel = frameRDD.toLabeledDataFrame(observationCols, labelCol,
        "features", Some(categoricalFeatures), Some(2))

      val dfSchema = dfWithCategoricalLabel.schema
      val mixedFeatureMetadata = AttributeGroup.fromStructField(dfSchema("features"))
      assert(mixedFeatureMetadata.size === 3)
      assert(mixedFeatureMetadata.getAttr(0) == NumericAttribute.defaultAttr.withIndex(0))
      assert(mixedFeatureMetadata.getAttr(1) == NumericAttribute.defaultAttr.withIndex(1))
      assert(mixedFeatureMetadata.getAttr(2) == NominalAttribute.defaultAttr.withIndex(2).withNumValues(2))

      val labelMetadata = NominalAttribute.fromStructField(dfSchema(labelCol))
      assert(labelMetadata == NominalAttribute.defaultAttr.withName(labelCol).withNumValues(2))

      val featureRow = dfWithCategoricalLabel.select("features").first()
      val labelRow = dfWithCategoricalLabel.select("has_diabetes").first()
      assert(featureRow.getAs[Vector]("features").toDense === Vectors.dense(20, 130, 1))
      assert(labelRow.getInt(0) == 0)
    }

    "convert to labeled dataframe with categorical and continuous features" in {
      val schema = FrameSchema(Vector(Column("age", DataTypes.int32), Column("weight", DataTypes.float64),
        Column("gender", DataTypes.int32), Column("has_diabetes", DataTypes.int32)))
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](56, 230.0, 0, 1)))

      val frameRDD = new FrameRdd(schema, sparkContext.parallelize(rows))
      val labelCol = "has_diabetes"
      val observationCols = List("age", "weight", "gender")

      // Convert to labeled frame with continuous features and label
      val dfWithContinuousLabel = frameRDD.toLabeledDataFrame(observationCols, labelCol, "features")

      val dfSchema = dfWithContinuousLabel.schema
      val contFeatureMetadata = AttributeGroup.fromStructField(dfSchema("features"))
      assert(contFeatureMetadata.size === 3)
      assert(contFeatureMetadata.getAttr(0) == NumericAttribute.defaultAttr.withIndex(0))
      assert(contFeatureMetadata.getAttr(1) == NumericAttribute.defaultAttr.withIndex(1))
      assert(contFeatureMetadata.getAttr(2) == NumericAttribute.defaultAttr.withIndex(2))

      val labelMetadata = NumericAttribute.fromStructField(dfSchema(labelCol))
      assert(labelMetadata == NumericAttribute.defaultAttr.withName(labelCol))

      val featureRow = dfWithContinuousLabel.select("features").first()
      val labelRow = dfWithContinuousLabel.select("has_diabetes").first()
      assert(featureRow.getAs[Vector]("features").toDense === Vectors.dense(56, 230, 0))
      assert(labelRow.getInt(0) == 1)
    }

  }

}
