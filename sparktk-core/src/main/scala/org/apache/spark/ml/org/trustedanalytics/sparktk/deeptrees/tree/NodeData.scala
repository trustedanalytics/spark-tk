/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree

/**
 * Info for a SparkRandomForestRegressionModelorg.apache.spark.ml.tree.Split
 *
 * Migrated class from SparkRandomForestRegressionModelDecisionTreeModelReadWrite due to scoping issues
 *
 * @param featureIndex  Index of feature split on
 * @param leftCategoriesOrThreshold  For categorical feature, set of leftCategories.
 *                                   For continuous feature, threshold.
 * @param numCategories  For categorical feature, number of categories.
 *                       For continuous feature, -1.
 */
private[spark] case class SplitData(
    featureIndex: Int,
    leftCategoriesOrThreshold: Array[Double],
    numCategories: Int) {

  def getSplit: Split = {
    if (numCategories != -1) {
      new CategoricalSplit(featureIndex, leftCategoriesOrThreshold, numCategories)
    }
    else {
      assert(leftCategoriesOrThreshold.length == 1, s"DecisionTree split data expected" +
        s" 1 threshold for ContinuousSplit, but found thresholds: " +
        leftCategoriesOrThreshold.mkString(", "))
      new ContinuousSplit(featureIndex, leftCategoriesOrThreshold(0))
    }
  }
}

private[spark] object SplitData {
  def apply(split: Split): SplitData = split match {
    case s: CategoricalSplit =>
      SplitData(s.featureIndex, s.leftCategories, s.numCategories)
    case s: ContinuousSplit =>
      SplitData(s.featureIndex, Array(s.threshold), -1)
  }
}

/**
 * Info for a SparkRandomForestRegressionModelNode
 *
 * Migrated class from SparkRandomForestRegressionModelDecisionTreeModelReadWrite due to scoping issues
 *
 * @param id  Index used for tree reconstruction.  Indices follow a pre-order traversal.
 * @param impurityStats  Stats array.  Impurity type is stored in metadata.
 * @param gain  Gain, or arbitrary value if leaf node.
 * @param leftChild  Left child index, or arbitrary value if leaf node.
 * @param rightChild  Right child index, or arbitrary value if leaf node.
 * @param split  Split info, or arbitrary value if leaf node.
 */
private[spark] case class NodeData(
  id: Int,
  prediction: Double,
  impurity: Double,
  impurityStats: Array[Double],
  gain: Double,
  leftChild: Int,
  rightChild: Int,
  split: SplitData)

private[spark] object NodeData {
  /**
   * Create SparkRandomForestRegressionModelNodeData instances for this node and all children.
   *
   * @param id  Current ID.  IDs are assigned via a pre-order traversal.
   * @return (sequence of nodes in pre-order traversal order, largest ID in subtree)
   *         The nodes are returned in pre-order traversal (root first) so that it is easy to
   *         get the ID of the subtree's root node.
   */
  def build(node: Node, id: Int): (Seq[NodeData], Int) = node match {
    case n: InternalNode =>
      val (leftNodeData, leftIdx) = build(n.leftChild, id + 1)
      val (rightNodeData, rightIdx) = build(n.rightChild, leftIdx + 1)
      val thisNodeData = NodeData(id, n.prediction, n.impurity, n.impurityStats.stats,
        n.gain, leftNodeData.head.id, rightNodeData.head.id, SplitData(n.split))
      (thisNodeData +: (leftNodeData ++ rightNodeData), rightIdx)
    case _: LeafNode =>
      (Seq(NodeData(id, node.prediction, node.impurity, node.impurityStats.stats,
        -1.0, -1, -1, SplitData(-1, Array.empty[Double], -1))),
        id)
  }
}

/**
 * Info for one SparkRandomForestRegressionModelNode]private[spark] ] in a tree ensemble
 *
 * Migrated class from SparkRandomForestRegressionModelDecisionTreeModelReadWrite due to scoping issues
 *
 * @param treeID  Tree index
 * @param nodeData  Data for this node
 */
private[spark] case class EnsembleNodeData(
  treeID: Int,
  nodeData: NodeData)

private[spark] object EnsembleNodeData {
  /**
   * Create SparkRandomForestRegressionModelEnsembleNodeData instances for the given tree.
   *
   * @return Sequence of nodes for this tree
   */
  def build(tree: DecisionTreeModel, treeID: Int): Seq[EnsembleNodeData] = {
    val (nodeData: Seq[NodeData], _) = NodeData.build(tree.rootNode, 0)
    nodeData.map(nd => EnsembleNodeData(treeID, nd))
  }
}
