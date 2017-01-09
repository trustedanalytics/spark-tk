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
package org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.param.Param
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util.{ DefaultParamsReader, DefaultParamsWriter }
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.model.{ DecisionTreeModel => OldDecisionTreeModel }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.collection.OpenHashMap
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.reflect.ClassTag

/**
 * Abstraction for Decision Tree models.
 *
 * TODO: Add support for predicting probabilities and raw predictions  SPARK-3727
 */
private[spark] trait DecisionTreeModel {

  /** Root of the decision tree */
  def rootNode: Node

  /** Number of nodes in tree, including leaf nodes. */
  def numNodes: Int = {
    1 + rootNode.numDescendants
  }

  /**
   * Depth of the tree.
   * E.g.: Depth 0 means 1 leaf node.  Depth 1 means 1 internal node and 2 leaf nodes.
   */
  lazy val depth: Int = {
    rootNode.subtreeDepth
  }

  /** Summary of the model */
  override def toString: String = {
    // Implementing classes should generally override this method to be more descriptive.
    s"DecisionTreeModel of depth $depth with $numNodes nodes"
  }

  /** Full description of model */
  def toDebugString: String = {
    val header = toString + "\n"
    header + rootNode.subtreeToString(2)
  }

  /**
   * Trace down the tree, and return the largest feature index used in any split.
   *
   * @return  Max feature index used in a split, or -1 if there are no splits (single leaf node).
   */
  private[ml] def maxSplitFeatureIndex(): Int = rootNode.maxSplitFeatureIndex()

  /** Convert to spark.mllib DecisionTreeModel (losing some information) */
  private[spark] def toOld: OldDecisionTreeModel
}

/**
 * Abstraction for models which are ensembles of decision trees
 *
 * TODO: Add support for predicting probabilities and raw predictions  SPARK-3727
 *
 * @tparam M  Type of tree model in this ensemble
 */
private[ml] trait TreeEnsembleModel[M <: DecisionTreeModel] {

  // Note: We use getTrees since subclasses of TreeEnsembleModel will store subclasses of
  //       DecisionTreeModel.

  /** Trees in this ensemble. Warning: These have null parent Estimators. */
  def trees: Array[M]

  /** Weights for each tree, zippable with SparkRandomForestRegressionModeltrees */
  def treeWeights: Array[Double]

  /** Weights used by the python wrappers. */
  // Note: An array cannot be returned directly due to serialization problems.
  private[spark] def javaTreeWeights: Vector = Vectors.dense(treeWeights)

  /** Summary of the model */
  override def toString: String = {
    // Implementing classes should generally override this method to be more descriptive.
    s"TreeEnsembleModel with ${trees.length} trees"
  }

  /** Full description of model */
  def toDebugString: String = {
    val header = toString + "\n"
    header + trees.zip(treeWeights).zipWithIndex.map {
      case ((tree, weight), treeIndex) =>
        s"  Tree $treeIndex (weight $weight):\n" + tree.rootNode.subtreeToString(4)
    }.fold("")(_ + _)
  }

  /** Total number of nodes, summed over all trees in the ensemble. */
  lazy val totalNumNodes: Int = trees.map(_.numNodes).sum
}

private[ml] object TreeEnsembleModel {

  /**
   * Given a tree ensemble model, compute the importance of each feature.
   * This generalizes the idea of "Gini" importance to other losses,
   * following the explanation of Gini importance from "Random Forests" documentation
   * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
   *
   * For collections of trees, including boosting and bagging, Hastie et al.
   * propose to use the average of single tree importances across all trees in the ensemble.
   *
   * This feature importance is calculated as follows:
   *  - Average over trees:
   *     - importance(feature j) = sum (over nodes which split on feature j) of the gain,
   *       where gain is scaled by the number of instances passing through node
   *     - Normalize importances for tree to sum to 1.
   *  - Normalize feature importance vector to sum to 1.
   *
   *  References:
   *  - Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.
   *
   * @param trees  Unweighted collection of trees
   * @param numFeatures  Number of features in model (even if not all are explicitly used by
   *                     the model).
   *                     If -1, then numFeatures is set based on the max feature index in all trees.
   * @return  Feature importance values, of length numFeatures.
   */
  def featureImportances[M <: DecisionTreeModel](trees: Array[M], numFeatures: Int): Vector = {
    val totalImportances = new OpenHashMap[Int, Double]()
    trees.foreach { tree =>
      // Aggregate feature importance vector for this tree
      val importances = new OpenHashMap[Int, Double]()
      computeFeatureImportance(tree.rootNode, importances)
      // Normalize importance vector for this tree, and add it to total.
      // TODO: In the future, also support normalizing by tree.rootNode.impurityStats.count?
      val treeNorm = importances.map(_._2).sum
      if (treeNorm != 0) {
        importances.foreach {
          case (idx, impt) =>
            val normImpt = impt / treeNorm
            totalImportances.changeValue(idx, normImpt, _ + normImpt)
        }
      }
    }
    // Normalize importances
    normalizeMapValues(totalImportances)
    // Construct vector
    val d = if (numFeatures != -1) {
      numFeatures
    }
    else {
      // Find max feature index used in trees
      val maxFeatureIndex = trees.map(_.maxSplitFeatureIndex()).max
      maxFeatureIndex + 1
    }
    if (d == 0) {
      assert(totalImportances.size == 0, s"Unknown error in computing feature" +
        s" importance: No splits found, but some non-zero importances.")
    }
    val (indices, values) = totalImportances.iterator.toSeq.sortBy(_._1).unzip
    Vectors.sparse(d, indices.toArray, values.toArray)
  }

  /**
   * Given a Decision Tree model, compute the importance of each feature.
   * This generalizes the idea of "Gini" importance to other losses,
   * following the explanation of Gini importance from "Random Forests" documentation
   * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
   *
   * This feature importance is calculated as follows:
   *  - importance(feature j) = sum (over nodes which split on feature j) of the gain,
   *    where gain is scaled by the number of instances passing through node
   *  - Normalize importances for tree to sum to 1.
   *
   * @param tree  Decision tree to compute importances for.
   * @param numFeatures  Number of features in model (even if not all are explicitly used by
   *                     the model).
   *                     If -1, then numFeatures is set based on the max feature index in all trees.
   * @return  Feature importance values, of length numFeatures.
   */
  def featureImportances[M <: DecisionTreeModel: ClassTag](tree: M, numFeatures: Int): Vector = {
    featureImportances(Array(tree), numFeatures)
  }

  /**
   * Recursive method for computing feature importances for one tree.
   * This walks down the tree, adding to the importance of 1 feature at each node.
   *
   * @param node  Current node in recursion
   * @param importances  Aggregate feature importances, modified by this method
   */
  def computeFeatureImportance(
    node: Node,
    importances: OpenHashMap[Int, Double]): Unit = {
    node match {
      case n: InternalNode =>
        val feature = n.split.featureIndex
        val scaledGain = n.gain * n.impurityStats.count
        importances.changeValue(feature, scaledGain, _ + scaledGain)
        computeFeatureImportance(n.leftChild, importances)
        computeFeatureImportance(n.rightChild, importances)
      case n: LeafNode =>
      // do nothing
    }
  }

  /**
   * Normalize the values of this map to sum to 1, in place.
   * If all values are 0, this method does nothing.
   *
   * @param map  Map with non-negative values.
   */
  def normalizeMapValues(map: OpenHashMap[Int, Double]): Unit = {
    val total = map.map(_._2).sum
    if (total != 0) {
      val keys = map.iterator.map(_._1).toArray
      keys.foreach { key => map.changeValue(key, 0.0, _ / total) }
    }
  }
}

/** Helper classes for tree model persistence */
object DecisionTreeModelReadWrite {

  /**
   * Load a decision tree from a file.
   * @return  Root node of reconstructed tree
   */
  def loadTreeNodes(
    path: String,
    metadata: DefaultParamsReader.Metadata,
    sqlContext: SQLContext): Node = {
    import sqlContext.implicits._
    implicit val format = DefaultFormats

    // Get impurity to construct ImpurityCalculator for each node
    val impurityType: String = {
      val impurityJson: JValue = metadata.getParamValue("impurity")
      Param.jsonDecode[String](compact(render(impurityJson)))
    }

    val dataPath = new Path(path, "data").toString
    val data = sqlContext.read.parquet(dataPath).as[NodeData]
    buildTreeFromNodes(data.collect(), impurityType)
  }

  /**
   * Given all data for all nodes in a tree, rebuild the tree.
   * @param data  Unsorted node data
   * @param impurityType  Impurity type for this tree
   * @return Root node of reconstructed tree
   */
  def buildTreeFromNodes(data: Array[NodeData], impurityType: String): Node = {
    // Load all nodes, sorted by ID.
    val nodes = data.sortBy(_.id)
    // Sanity checks; could remove
    assert(nodes.head.id == 0, s"Decision Tree load failed.  Expected smallest node ID to be 0," +
      s" but found ${nodes.head.id}")
    assert(nodes.last.id == nodes.length - 1, s"Decision Tree load failed.  Expected largest" +
      s" node ID to be ${nodes.length - 1}, but found ${nodes.last.id}")
    // We fill `finalNodes` in reverse order.  Since node IDs are assigned via a pre-order
    // traversal, this guarantees that child nodes will be built before parent nodes.
    val finalNodes = new Array[Node](nodes.length)
    nodes.reverseIterator.foreach {
      case n: NodeData =>
        val impurityStats = ImpurityCalculator.getCalculator(impurityType, n.impurityStats)
        val node = if (n.leftChild != -1) {
          val leftChild = finalNodes(n.leftChild)
          val rightChild = finalNodes(n.rightChild)
          new InternalNode(n.prediction, n.impurity, n.gain, leftChild, rightChild,
            n.split.getSplit, impurityStats)
        }
        else {
          new LeafNode(n.prediction, n.impurity, impurityStats)
        }
        finalNodes(n.id) = node
    }
    // Return the root node
    finalNodes.head
  }
}

private[ml] object EnsembleModelReadWrite {

  /**
   * Helper method for saving a tree ensemble to disk.
   *
   * @param instance  Tree ensemble model
   * @param path  Path to which to save the ensemble model.
   * @param extraMetadata  Metadata such as numFeatures, numClasses, numTrees.
   */
  def saveImpl[M <: Params with TreeEnsembleModel[_ <: DecisionTreeModel]](
    instance: M,
    path: String,
    sql: SQLContext,
    extraMetadata: JObject): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sql.sparkContext, Some(extraMetadata))
    val treesMetadataWeights: Array[(Int, String, Double)] = instance.trees.zipWithIndex.map {
      case (tree, treeID) =>
        (treeID,
          DefaultParamsWriter.getMetadataToSave(tree.asInstanceOf[Params], sql.sparkContext),
          instance.treeWeights(treeID))
    }
    val treesMetadataPath = new Path(path, "treesMetadata").toString
    sql.createDataFrame(treesMetadataWeights).toDF("treeID", "metadata", "weights")
      .write.parquet(treesMetadataPath)
    val dataPath = new Path(path, "data").toString
    val nodeDataRDD = sql.sparkContext.parallelize(instance.trees.zipWithIndex).flatMap {
      case (tree, treeID) => EnsembleNodeData.build(tree, treeID)
    }
    sql.createDataFrame(nodeDataRDD).write.parquet(dataPath)
  }

  /**
   * Helper method for loading a tree ensemble from disk.
   * This reconstructs all trees, returning the root nodes.
   * @param path  Path given to `saveImpl`
   * @param className  Class name for ensemble model type
   * @param treeClassName  Class name for tree model type in the ensemble
   * @return  (ensemble metadata, array over trees of (tree metadata, root node)),
   *          where the root node is linked with all descendents
   * @see `saveImpl` for how the model was saved
   */
  def loadImpl(
    path: String,
    sql: SQLContext,
    className: String,
    treeClassName: String): (Metadata, Array[(Metadata, Node)], Array[Double]) = {
    import sql.implicits._
    implicit val format = DefaultFormats
    val metadata = DefaultParamsReader.loadMetadata(path, sql.sparkContext, className)

    // Get impurity to construct ImpurityCalculator for each node
    val impurityType: String = {
      val impurityJson: JValue = metadata.getParamValue("impurity")
      Param.jsonDecode[String](compact(render(impurityJson)))
    }

    val treesMetadataPath = new Path(path, "treesMetadata").toString
    val treesMetadataRDD: RDD[(Int, (Metadata, Double))] = sql.read.parquet(treesMetadataPath)
      .select("treeID", "metadata", "weights").as[(Int, String, Double)].rdd.map {
        case (treeID: Int, json: String, weights: Double) =>
          treeID -> (DefaultParamsReader.parseMetadata(json, treeClassName), weights)
      }

    val treesMetadataWeights = treesMetadataRDD.sortByKey().values.collect()
    val treesMetadata = treesMetadataWeights.map(_._1)
    val treesWeights = treesMetadataWeights.map(_._2)

    val dataPath = new Path(path, "data").toString
    val nodeData = sql.read.parquet(dataPath).as[EnsembleNodeData]
    val rootNodesRDD: RDD[(Int, Node)] =
      nodeData.rdd.map(d => (d.treeID, d.nodeData)).groupByKey().map {
        case (treeID: Int, nodeData: Iterable[NodeData]) =>
          treeID -> DecisionTreeModelReadWrite.buildTreeFromNodes(nodeData.toArray, impurityType)
      }
    val rootNodes: Array[Node] = rootNodesRDD.sortByKey().values.collect()
    (metadata, treesMetadata.zip(rootNodes), treesWeights)
  }

}
