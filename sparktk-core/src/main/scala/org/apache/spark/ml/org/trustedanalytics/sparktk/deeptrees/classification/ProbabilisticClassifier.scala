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
package org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.classification

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.param.shared._
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util.SchemaUtils
import org.apache.spark.mllib.linalg.{ DenseVector, Vector, VectorUDT }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ DataType, StructType }

/**
 * (private[classification])  Params for probabilistic classification.
 */
private[classification] trait ProbabilisticClassifierParams
    extends ClassifierParams with HasProbabilityCol with HasThresholds {
  override protected def validateAndTransformSchema(
    schema: StructType,
    fitting: Boolean,
    featuresDataType: DataType): StructType = {
    val parentSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    SchemaUtils.appendColumn(parentSchema, $(probabilityCol), new VectorUDT)
  }
}

/**
 * :: DeveloperApi ::
 *
 * Single-label binary or multiclass classifier which can output class conditional probabilities.
 *
 * @tparam FeaturesType  Type of input features.  E.g., `Vector`
 * @tparam E  Concrete Estimator type
 * @tparam M  Concrete Model type
 */
@DeveloperApi
abstract class ProbabilisticClassifier[FeaturesType, E <: ProbabilisticClassifier[FeaturesType, E, M], M <: ProbabilisticClassificationModel[FeaturesType, M]]
    extends Classifier[FeaturesType, E, M] with ProbabilisticClassifierParams {

  /** @group setParam */
  def setProbabilityCol(value: String): E = set(probabilityCol, value).asInstanceOf[E]

  /** @group setParam */
  def setThresholds(value: Array[Double]): E = set(thresholds, value).asInstanceOf[E]
}

/**
 * :: DeveloperApi ::
 *
 * Model produced by a SparkRandomForestRegressionModelProbabilisticClassifier.
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., `Vector`
 * @tparam M  Concrete Model type
 */
@DeveloperApi
abstract class ProbabilisticClassificationModel[FeaturesType, M <: ProbabilisticClassificationModel[FeaturesType, M]]
    extends ClassificationModel[FeaturesType, M] with ProbabilisticClassifierParams {

  /** @group setParam */
  def setProbabilityCol(value: String): M = set(probabilityCol, value).asInstanceOf[M]

  /** @group setParam */
  def setThresholds(value: Array[Double]): M = {
    require(value.length == numClasses, this.getClass.getSimpleName +
      ".setThresholds() called with non-matching numClasses and thresholds.length." +
      s" numClasses=$numClasses, but thresholds has length ${value.length}")
    set(thresholds, value).asInstanceOf[M]
  }

  /**
   * Transforms dataset by reading from SparkRandomForestRegressionModelfeaturesCol, and appending new columns as specified by
   * parameters:
   *  - predicted labels as SparkRandomForestRegressionModelpredictionCol of type SparkRandomForestRegressionModelDouble
   *  - raw predictions (confidences) as SparkRandomForestRegressionModelrawPredictionCol of type SparkRandomForestRegressionModelVector
   *  - probability of each class as SparkRandomForestRegressionModelprobabilityCol of type SparkRandomForestRegressionModelVector.
   *
   * @param dataset input dataset
   * @return transformed dataset
   */
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".transform() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    // Output selected columns only.
    // This is a bit complicated since it tries to avoid repeated computation.
    var outputData = dataset
    var numColsOutput = 0
    if ($(rawPredictionCol).nonEmpty) {
      val predictRawUDF = udf { (features: Any) =>
        predictRaw(features.asInstanceOf[FeaturesType])
      }
      outputData = outputData.withColumn(getRawPredictionCol, predictRawUDF(col(getFeaturesCol)))
      numColsOutput += 1
    }
    if ($(probabilityCol).nonEmpty) {
      val probUDF = if ($(rawPredictionCol).nonEmpty) {
        udf(raw2probability _).apply(col($(rawPredictionCol)))
      }
      else {
        val probabilityUDF = udf { (features: Any) =>
          predictProbability(features.asInstanceOf[FeaturesType])
        }
        probabilityUDF(col($(featuresCol)))
      }
      outputData = outputData.withColumn($(probabilityCol), probUDF)
      numColsOutput += 1
    }
    if ($(predictionCol).nonEmpty) {
      val predUDF = if ($(rawPredictionCol).nonEmpty) {
        udf(raw2prediction _).apply(col($(rawPredictionCol)))
      }
      else if ($(probabilityCol).nonEmpty) {
        udf(probability2prediction _).apply(col($(probabilityCol)))
      }
      else {
        val predictUDF = udf { (features: Any) =>
          predict(features.asInstanceOf[FeaturesType])
        }
        predictUDF(col($(featuresCol)))
      }
      outputData = outputData.withColumn($(predictionCol), predUDF)
      numColsOutput += 1
    }

    if (numColsOutput == 0) {
      this.logWarning(s"$uid: ProbabilisticClassificationModel.transform() was called as NOOP" +
        " since no output columns were set.")
    }
    outputData
  }

  /**
   * Estimate the probability of each class given the raw prediction,
   * doing the computation in-place.
   * These predictions are also called class conditional probabilities.
   *
   * This internal method is used to implement SparkRandomForestRegressionModeltransform() and output SparkRandomForestRegressionModelprobabilityCol.
   *
   * @return Estimated class conditional probabilities (modified input vector)
   */
  protected def raw2probabilityInPlace(rawPrediction: Vector): Vector

  /** Non-in-place version of SparkRandomForestRegressionModelraw2probabilityInPlace() */
  protected def raw2probability(rawPrediction: Vector): Vector = {
    val probs = rawPrediction.copy
    raw2probabilityInPlace(probs)
  }

  override protected def raw2prediction(rawPrediction: Vector): Double = {
    if (!isDefined(thresholds)) {
      rawPrediction.argmax
    }
    else {
      probability2prediction(raw2probability(rawPrediction))
    }
  }

  /**
   * Predict the probability of each class given the features.
   * These predictions are also called class conditional probabilities.
   *
   * This internal method is used to implement SparkRandomForestRegressionModeltransform() and output SparkRandomForestRegressionModelprobabilityCol.
   *
   * @return Estimated class conditional probabilities
   */
  protected def predictProbability(features: FeaturesType): Vector = {
    val rawPreds = predictRaw(features)
    raw2probabilityInPlace(rawPreds)
  }

  /**
   * Given a vector of class conditional probabilities, select the predicted label.
   * This supports thresholds which favor particular labels.
   * @return  predicted label
   */
  protected def probability2prediction(probability: Vector): Double = {
    if (!isDefined(thresholds)) {
      probability.argmax
    }
    else {
      val thresholds = getThresholds
      var argMax = 0
      var max = Double.NegativeInfinity
      var i = 0
      val probabilitySize = probability.size
      while (i < probabilitySize) {
        // Thresholds are all > 0, excepting that at most one may be 0.
        // The single class whose threshold is 0, if any, will always be predicted
        // ('scaled' = +Infinity). However in the case that this class also has
        // 0 probability, the class will not be selected ('scaled' is NaN).
        val scaled = probability(i) / thresholds(i)
        if (scaled > max) {
          max = scaled
          argMax = i
        }
        i += 1
      }
      argMax
    }
  }
}

private[ml] object ProbabilisticClassificationModel {

  /**
   * Normalize a vector of raw predictions to be a multinomial probability vector, in place.
   *
   * The input raw predictions should be nonnegative.
   * The output vector sums to 1, unless the input vector is all-0 (in which case the output is
   * all-0 too).
   *
   * NOTE: This is NOT applicable to all models, only ones which effectively use class
   *       instance counts for raw predictions.
   */
  def normalizeToProbabilitiesInPlace(v: DenseVector): Unit = {
    val sum = v.values.sum
    if (sum != 0) {
      var i = 0
      val size = v.size
      while (i < size) {
        v.values(i) /= sum
        i += 1
      }
    }
  }
}
