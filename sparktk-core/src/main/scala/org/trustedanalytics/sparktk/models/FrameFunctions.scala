package org.trustedanalytics.sparktk.models

import org.apache.spark.ml.attribute.{ AttributeGroup, NumericAttribute, NominalAttribute }
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

class FrameFunctions(self: FrameRdd) extends Serializable {

  /**
   * Convert FrameRdd to Spark dataframe with feature vector and label
   *
   * @param labelColumnName Label column name
   * @param featureColumnNames List of feature column names
   * @param outputFeatureVectorName Column name of output feature vector
   * @param categoricalFeatures Optional arity of categorical features. Entry (name -> k) indicates that
   *                            feature 'name' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}
   * @param labelNumClasses Optional number of categories in label column. If None, label column is continuous.
   * @return Dataframe with feature vector and label
   */
  def toLabeledDataFrame(labelColumnName: String,
                         featureColumnNames: List[String],
                         outputFeatureVectorName: String,
                         categoricalFeatures: Option[Map[String, Int]] = None,
                         labelNumClasses: Option[Int] = None): DataFrame = {
    require(labelColumnName != null, "label column name must not be null")
    require(featureColumnNames != null, "feature column names must not be null")
    require(outputFeatureVectorName != null, "output feature vector name must not be null")
    require(labelNumClasses.isEmpty || labelNumClasses.get >= 2,
      "number of categories in label column must be greater than 1")

    val assembler = new VectorAssembler().setInputCols(featureColumnNames.toArray)
      .setOutputCol(outputFeatureVectorName)
    val featureFrame = assembler.transform(self.toDataFrame)

    // Identify categorical and numerical features
    val categoricalFeaturesMap = categoricalFeatures.getOrElse(Map.empty[String, Int])
    val featuresAttributes = featureColumnNames.indices.map { featureIndex =>
      val featureName = featureColumnNames(featureIndex)
      if (categoricalFeaturesMap.contains(featureName)) {
        NominalAttribute.defaultAttr.withIndex(featureIndex)
          .withNumValues(categoricalFeaturesMap(featureName))
      }
      else {
        NumericAttribute.defaultAttr.withIndex(featureIndex)
      }
    }.toArray
    val labelAttribute = if (labelNumClasses.isEmpty) {
      NumericAttribute.defaultAttr.withName(labelColumnName)
    }
    else {
      NominalAttribute.defaultAttr.withName(labelColumnName).withNumValues(labelNumClasses.get)
    }

    // Update frame metadata with categorical and numerical features
    val featuresMetadata = new AttributeGroup(outputFeatureVectorName, featuresAttributes).toMetadata()
    val labelMetadata = labelAttribute.toMetadata()
    featureFrame.select(featureFrame(outputFeatureVectorName).as(outputFeatureVectorName, featuresMetadata),
      featureFrame(labelColumnName).as(labelColumnName, labelMetadata))
  }
}

object FrameImplicits {
  implicit def frameRddToFrameFunctions(frameRdd: FrameRdd) = new FrameFunctions(frameRdd)
}