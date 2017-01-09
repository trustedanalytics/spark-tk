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
package org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.regression

import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree._
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree.impl.RandomForest
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util._
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.{ PredictionModel, Predictor }
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.configuration.{ Algo => OldAlgo }
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.model.{ RandomForestModel => OldRandomForestModel }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.json4s.JsonDSL._
import org.json4s.{ DefaultFormats, JObject }

/**
 * <a href="http://en.wikipedia.org/wiki/Random_forest">Random Forest</a>
 * learning algorithm for regression.
 * It supports both continuous and categorical features.
 */
@Since("1.4.0")
class RandomForestRegressor @Since("1.4.0") (@Since("1.4.0") override val uid: String)
    extends Predictor[Vector, RandomForestRegressor, RandomForestRegressionModel]
    with RandomForestRegressorParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("rfr"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeRegressorParams:

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * SparkRandomForestRegressionModelorg.apache.spark.SparkContext.
   * Must be >= 1.
   * (default = 10)
   * @group setParam
   */
  @Since("1.4.0")
  override def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setImpurity(value: String): this.type = set(impurity, value)

  // Parameters from TreeEnsembleParams:

  /** @group setParam */
  @Since("1.4.0")
  override def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setSeed(value: Long): this.type = set(seed, value)

  // Parameters from RandomForestParams:

  /** @group setParam */
  @Since("1.4.0")
  override def setNumTrees(value: Int): this.type = set(numTrees, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setFeatureSubsetStrategy(value: String): this.type =
    set(featureSubsetStrategy, value)

  override protected def train(dataset: DataFrame): RandomForestRegressionModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val strategy =
      super.getOldStrategy(categoricalFeatures, numClasses = 0, OldAlgo.Regression, getOldImpurity)

    val trees = RandomForest
      .run(oldDataset, strategy, getNumTrees, getFeatureSubsetStrategy, getSeed)
      .map(_.asInstanceOf[DecisionTreeRegressionModel])

    val numFeatures = oldDataset.first().features.size
    val m = new RandomForestRegressionModel(trees, numFeatures)
    m
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): RandomForestRegressor = defaultCopy(extra)
}

@Since("1.4.0")
object RandomForestRegressor extends DefaultParamsReadable[RandomForestRegressor] {
  /** Accessor for supported impurity settings: variance */
  @Since("1.4.0")
  final val supportedImpurities: Array[String] = TreeRegressorParams.supportedImpurities

  /** Accessor for supported featureSubsetStrategy settings: auto, all, onethird, sqrt, log2 */
  @Since("1.4.0")
  final val supportedFeatureSubsetStrategies: Array[String] =
    RandomForestParams.supportedFeatureSubsetStrategies

  @Since("2.0.0")
  override def load(path: String): RandomForestRegressor = super.load(path)

}

/**
 * <a href="http://en.wikipedia.org/wiki/Random_forest">Random Forest</a> model for regression.
 * It supports both continuous and categorical features.
 *
 * @param _trees  Decision trees in the ensemble.
 * @param numFeatures  Number of features used by this model
 */
@Since("1.4.0")
class RandomForestRegressionModel private[ml] (
  override val uid: String,
  private val _trees: Array[DecisionTreeRegressionModel],
  override val numFeatures: Int)
    extends PredictionModel[Vector, RandomForestRegressionModel]
    with RandomForestRegressorParams with TreeEnsembleModel[DecisionTreeRegressionModel]
    with MLWritable with Serializable {

  require(_trees.nonEmpty, "RandomForestRegressionModel requires at least 1 tree.")

  /**
   * Construct a random forest regression model, with all trees weighted equally.
   *
   * @param trees  Component trees
   */
  private[ml] def this(trees: Array[DecisionTreeRegressionModel], numFeatures: Int) =
    this(Identifiable.randomUID("rfr"), trees, numFeatures)

  @Since("1.4.0")
  override def trees: Array[DecisionTreeRegressionModel] = _trees

  // Note: We may add support for weights (based on tree performance) later on.
  private lazy val _treeWeights: Array[Double] = Array.fill[Double](_trees.length)(1.0)

  @Since("1.4.0")
  override def treeWeights: Array[Double] = _treeWeights

  override protected def transformImpl(dataset: DataFrame): DataFrame = {
    val bcastModel = dataset.rdd.sparkContext.broadcast(this)
    val predictUDF = udf { (features: Any) =>
      bcastModel.value.predict(features.asInstanceOf[Vector])
    }
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override def predict(features: Vector): Double = {
    // TODO: When we add a generic Bagging class, handle transform there.  SPARK-7128
    // Predict average of tree predictions.
    // Ignore the weights since all are 1.0 for now.
    _trees.map(_.rootNode.predictImpl(features).prediction).sum / getNumTrees
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): RandomForestRegressionModel = {
    copyValues(new RandomForestRegressionModel(uid, _trees, numFeatures), extra).setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"RandomForestRegressionModel (uid=$uid) with $getNumTrees trees"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * Each feature's importance is the average of its importance across all trees in the ensemble
   * The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
   * (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
   * and follows the implementation from scikit-learn.
   *
   * @see `DecisionTreeRegressionModel.featureImportances`
   */
  @Since("1.5.0")
  lazy val featureImportances: Vector = TreeEnsembleModel.featureImportances(trees, numFeatures)

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldRandomForestModel = {
    new OldRandomForestModel(OldAlgo.Regression, _trees.map(_.toOld))
  }

  @Since("2.0.0")
  override def write: MLWriter =
    new RandomForestRegressionModel.RandomForestRegressionModelWriter(this)
}

@Since("2.0.0")
object RandomForestRegressionModel extends MLReadable[RandomForestRegressionModel] {

  @Since("2.0.0")
  override def read: MLReader[RandomForestRegressionModel] = new RandomForestRegressionModelReader

  @Since("2.0.0")
  override def load(path: String): RandomForestRegressionModel = super.load(path)

  private[RandomForestRegressionModel] class RandomForestRegressionModelWriter(instance: RandomForestRegressionModel)
      extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata: JObject = Map(
        "numFeatures" -> instance.numFeatures,
        "numTrees" -> instance.getNumTrees)
      EnsembleModelReadWrite.saveImpl(instance, path, sqlContext, extraMetadata)
    }
  }

  private class RandomForestRegressionModelReader extends MLReader[RandomForestRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[RandomForestRegressionModel].getName
    private val treeClassName = classOf[DecisionTreeRegressionModel].getName

    override def load(path: String): RandomForestRegressionModel = {
      implicit val format = DefaultFormats
      val (metadata: Metadata, treesData: Array[(Metadata, Node)], treeWeights: Array[Double]) =
        EnsembleModelReadWrite.loadImpl(path, sqlContext, className, treeClassName)
      val numFeatures = (metadata.metadata \ "numFeatures").extract[Int]
      val numTrees = (metadata.metadata \ "numTrees").extract[Int]

      val trees: Array[DecisionTreeRegressionModel] = treesData.map {
        case (treeMetadata, root) =>
          val tree =
            new DecisionTreeRegressionModel(treeMetadata.uid, root, numFeatures)
          DefaultParamsReader.getAndSetParams(tree, treeMetadata)
          tree
      }
      require(numTrees == trees.length, s"RandomForestRegressionModel.load expected $numTrees" +
        s" trees based on metadata but found ${trees.length} trees.")

      val model = new RandomForestRegressionModel(metadata.uid, trees, numFeatures)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  /** Convert a model from the old API */
  private[ml] def fromOld(
    oldModel: OldRandomForestModel,
    parent: RandomForestRegressor,
    categoricalFeatures: Map[Int, Int],
    numFeatures: Int = -1): RandomForestRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression, "Cannot convert RandomForestModel" +
      s" with algo=${oldModel.algo} (old API) to RandomForestRegressionModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent for each tree is null since there is no good way to set this.
      DecisionTreeRegressionModel.fromOld(tree, null, categoricalFeatures)
    }
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("rfr")
    new RandomForestRegressionModel(uid, newTrees, numFeatures)
  }
}
