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

import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree._
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree.impl.RandomForest
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util._
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.{ PredictionModel, Predictor }
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.configuration.{ Algo => OldAlgo, Strategy => OldStrategy }
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.model.{ DecisionTreeModel => OldDecisionTreeModel }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.json4s.JsonDSL._
import org.json4s.{ DefaultFormats, JObject }

/**
 * <a href="http://en.wikipedia.org/wiki/Decision_tree_learning">Decision tree</a>
 * learning algorithm for regression.
 * It supports both continuous and categorical features.
 */
@Since("1.4.0")
class DecisionTreeRegressor @Since("1.4.0") (@Since("1.4.0") override val uid: String)
    extends Predictor[Vector, DecisionTreeRegressor, DecisionTreeRegressionModel]
    with DecisionTreeRegressorParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("dtr"))

  // Override parameter setters from parent trait for Java API compatibility.
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

  /** @group setParam */
  @Since("1.6.0")
  override def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("2.0.0")
  def setVarianceCol(value: String): this.type = set(varianceCol, value)

  override protected def train(dataset: DataFrame): DecisionTreeRegressionModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val strategy = getOldStrategy(categoricalFeatures)

    val trees = RandomForest.run(oldDataset, strategy, numTrees = 1, featureSubsetStrategy = "all",
      seed = $(seed), parentUID = Some(uid))

    val m = trees.head.asInstanceOf[DecisionTreeRegressionModel]
    m
  }

  /** (private[ml]) Train a decision tree on an RDD */
  private[ml] def train(data: RDD[LabeledPoint],
                        oldStrategy: OldStrategy): DecisionTreeRegressionModel = {

    val trees = RandomForest.run(data, oldStrategy, numTrees = 1, featureSubsetStrategy = "all",
      seed = $(seed), parentUID = Some(uid))

    val m = trees.head.asInstanceOf[DecisionTreeRegressionModel]
    m
  }

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(categoricalFeatures: Map[Int, Int]): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses = 0, OldAlgo.Regression, getOldImpurity,
      subsamplingRate = 1.0)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): DecisionTreeRegressor = defaultCopy(extra)
}

@Since("1.4.0")
object DecisionTreeRegressor extends DefaultParamsReadable[DecisionTreeRegressor] {
  /** Accessor for supported impurities: variance */
  final val supportedImpurities: Array[String] = TreeRegressorParams.supportedImpurities

  @Since("2.0.0")
  override def load(path: String): DecisionTreeRegressor = super.load(path)
}

/**
 * <a href="http://en.wikipedia.org/wiki/Decision_tree_learning">
 * Decision tree (Wikipedia)</a> model for regression.
 * It supports both continuous and categorical features.
 * @param rootNode  Root of the decision tree
 */
@Since("1.4.0")
class DecisionTreeRegressionModel private[ml] (
  override val uid: String,
  override val rootNode: Node,
  override val numFeatures: Int)
    extends PredictionModel[Vector, DecisionTreeRegressionModel]
    with DecisionTreeModel with DecisionTreeRegressorParams with MLWritable with Serializable {

  /** @group setParam */
  def setVarianceCol(value: String): this.type = set(varianceCol, value)

  require(rootNode != null,
    "DecisionTreeRegressionModel given null rootNode, but it requires a non-null rootNode.")

  /**
   * Construct a decision tree regression model.
   * @param rootNode  Root node of tree, with other nodes attached.
   */
  private[ml] def this(rootNode: Node, numFeatures: Int) =
    this(Identifiable.randomUID("dtr"), rootNode, numFeatures)

  override def predict(features: Vector): Double = {
    rootNode.predictImpl(features).prediction
  }

  /** We need to update this function if we ever add other impurity measures. */
  protected def predictVariance(features: Vector): Double = {
    rootNode.predictImpl(features).impurityStats.calculate()
  }

  @Since("2.0.0")
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    transformImpl(dataset)
  }

  override protected def transformImpl(dataset: DataFrame): DataFrame = {
    val predictUDF = udf { (features: Vector) => predict(features) }
    val predictVarianceUDF = udf { (features: Vector) => predictVariance(features) }
    var output = dataset
    if ($(predictionCol).nonEmpty) {
      output = output.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
    }
    if (isDefined(varianceCol) && $(varianceCol).nonEmpty) {
      output = output.withColumn($(varianceCol), predictVarianceUDF(col($(featuresCol))))
    }
    output
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): DecisionTreeRegressionModel = {
    copyValues(new DecisionTreeRegressionModel(uid, rootNode, numFeatures), extra).setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"DecisionTreeRegressionModel (uid=$uid) of depth $depth with $numNodes nodes"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * This generalizes the idea of "Gini" importance to other losses,
   * following the explanation of Gini importance from "Random Forests" documentation
   * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
   *
   * This feature importance is calculated as follows:
   *   - importance(feature j) = sum (over nodes which split on feature j) of the gain,
   *     where gain is scaled by the number of instances passing through node
   *   - Normalize importances for tree to sum to 1.
   *
   * @note Feature importance for single decision trees can have high variance due to
   * correlated predictor variables. Consider using a SparkRandomForestRegressionModelRandomForestRegressor
   * to determine feature importance instead.
   */
  @Since("2.0.0")
  lazy val featureImportances: Vector = TreeEnsembleModel.featureImportances(this, numFeatures)

  /** Convert to spark.mllib DecisionTreeModel (losing some information) */
  override private[spark] def toOld: OldDecisionTreeModel = {
    new OldDecisionTreeModel(rootNode.toOld(1), OldAlgo.Regression)
  }

  @Since("2.0.0")
  override def write: MLWriter =
    new DecisionTreeRegressionModel.DecisionTreeRegressionModelWriter(this)
}

@Since("2.0.0")
object DecisionTreeRegressionModel extends MLReadable[DecisionTreeRegressionModel] {

  @Since("2.0.0")
  override def read: MLReader[DecisionTreeRegressionModel] =
    new DecisionTreeRegressionModelReader

  @Since("2.0.0")
  override def load(path: String): DecisionTreeRegressionModel = super.load(path)

  private[DecisionTreeRegressionModel] class DecisionTreeRegressionModelWriter(instance: DecisionTreeRegressionModel)
      extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata: JObject = Map(
        "numFeatures" -> instance.numFeatures)
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val (nodeData, _) = NodeData.build(instance.rootNode, 0)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(nodeData).write.parquet(dataPath)
    }
  }

  private class DecisionTreeRegressionModelReader
      extends MLReader[DecisionTreeRegressionModel] {
    import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree.DecisionTreeModelReadWrite._
    /** Checked against metadata when loading model */
    private val className = classOf[DecisionTreeRegressionModel].getName

    override def load(path: String): DecisionTreeRegressionModel = {
      implicit val format = DefaultFormats
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val numFeatures = (metadata.metadata \ "numFeatures").extract[Int]
      val root = loadTreeNodes(path, metadata, sqlContext)
      val model = new DecisionTreeRegressionModel(metadata.uid, root, numFeatures)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  /** Convert a model from the old API */
  private[ml] def fromOld(
    oldModel: OldDecisionTreeModel,
    parent: DecisionTreeRegressor,
    categoricalFeatures: Map[Int, Int],
    numFeatures: Int = -1): DecisionTreeRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression,
      s"Cannot convert non-regression DecisionTreeModel (old API) to" +
        s" DecisionTreeRegressionModel (new API).  Algo is: ${oldModel.algo}")
    val rootNode = Node.fromOld(oldModel.topNode, categoricalFeatures)
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("dtr")
    new DecisionTreeRegressionModel(uid, rootNode, numFeatures)
  }
}
