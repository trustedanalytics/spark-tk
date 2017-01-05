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

import org.apache.spark.ml.attribute.{ AttributeGroup, NumericAttribute, NominalAttribute }
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

class FrameFunctions(self: FrameRdd) extends Serializable {

  /**
   * Convert FrameRdd to Spark dataframe with feature vector and label
   *
   * @param observationColumns List of observation column names
   * @param labelColumn Label column name
   * @param outputFeatureVectorName Column name of output feature vector
   * @param categoricalFeatures Optional arity of categorical features. Entry (name -> k) indicates that
   *                            feature 'name' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}
   * @param labelNumClasses Optional number of categories in label column. If None, label column is continuous.
   * @return Dataframe with feature vector and label
   */
  def toLabeledDataFrame(observationColumns: List[String],
                         labelColumn: String,
                         outputFeatureVectorName: String,
                         categoricalFeatures: Option[Map[String, Int]] = None,
                         labelNumClasses: Option[Int] = None): DataFrame = {
    require(labelColumn != null, "label column name must not be null")
    require(observationColumns != null, "feature column names must not be null")
    require(outputFeatureVectorName != null, "output feature vector name must not be null")
    require(labelNumClasses.isEmpty || labelNumClasses.get >= 2,
      "number of categories in label column must be greater than 1")

    val assembler = new VectorAssembler().setInputCols(observationColumns.toArray)
      .setOutputCol(outputFeatureVectorName)
    val featureFrame = assembler.transform(self.toDataFrame)

    // Identify categorical and numerical features
    val categoricalFeaturesMap = categoricalFeatures.getOrElse(Map.empty[String, Int])
    val featuresAttributes = observationColumns.indices.map { featureIndex =>
      val featureName = observationColumns(featureIndex)
      if (categoricalFeaturesMap.contains(featureName)) {
        NominalAttribute.defaultAttr.withIndex(featureIndex)
          .withNumValues(categoricalFeaturesMap(featureName))
      }
      else {
        NumericAttribute.defaultAttr.withIndex(featureIndex)
      }
    }.toArray
    val labelAttribute = if (labelNumClasses.isEmpty) {
      NumericAttribute.defaultAttr.withName(labelColumn)
    }
    else {
      NominalAttribute.defaultAttr.withName(labelColumn).withNumValues(labelNumClasses.get)
    }

    // Update frame metadata with categorical and numerical features
    val featuresMetadata = new AttributeGroup(outputFeatureVectorName, featuresAttributes).toMetadata()
    val labelMetadata = labelAttribute.toMetadata()
    featureFrame.select(featureFrame(outputFeatureVectorName).as(outputFeatureVectorName, featuresMetadata),
      featureFrame(labelColumn).as(labelColumn, labelMetadata))
  }
}

object FrameImplicits {
  implicit def frameRddToFrameFunctions(frameRdd: FrameRdd) = new FrameFunctions(frameRdd)
}