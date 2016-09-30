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
package org.trustedanalytics.sparktk.graph

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.graph.internal.ops.orientdb.ExportToOrientdbSummarization
import org.trustedanalytics.sparktk.graph.internal.{ GraphSchema, GraphState, BaseGraph }
import org.trustedanalytics.sparktk.graph.internal.ops._
import org.trustedanalytics.sparktk.saveload.TkSaveableObject

class Graph(graphFrame: GraphFrame) extends BaseGraph with Serializable
    with ConnectedComponentsSummarization
    with ClusteringCoefficientSummarization
    with DegreeSummarization
    with ExportToOrientdbSummarization
    with GlobalClusteringCoefficientSummarization
    with LabelPropagationSummarization
    with LoopyBeliefPropagationSummarization
    with PageRankSummarization
    with SaveSummarization
    with TriangleCountSummarization
    with VertexCountSummarization
    with WeightedDegreeSummarization {

  def this(verticesFrame: DataFrame, edgesFrame: DataFrame) = {
    this(GraphFrame(verticesFrame, edgesFrame))
  }

  def this(verticesFrame: Frame, edgesFrame: Frame) = {
    this(verticesFrame.dataframe, edgesFrame.dataframe)
  }

  graphState = GraphState(graphFrame)

  override def toString: String = {
    // (Copied almost exactly from GraphFrame)
    import GraphFrame._
    val v = graphState.graphFrame.vertices.select(ID, graphState.graphFrame.vertices.columns.filter(_ != ID): _*).toString()
    val e = graphState.graphFrame.edges.select(SRC, DST +: graphState.graphFrame.edges.columns.filter(c => c != SRC && c != DST): _*).toString()
    "Graph(v:" + v + ", e:" + e + ")"
  }

}

object Graph extends TkSaveableObject {

  val tkFormatVersion = 1

  /**
   * Loads the parquet files (the vertices and edges dataframes) found at the given path and returns a Graph
   *
   * @param sc active SparkContext
   * @param path path to the file
   * @param formatVersion TK metadata formatVersion
   * @param tkMetadata TK metadata
   * @return
   */
  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int = tkFormatVersion, tkMetadata: JValue = null): Any = {
    require(tkFormatVersion == formatVersion, s"Graph load only supports version $tkFormatVersion.  Got version $formatVersion")
    // no extra metadata in version 1
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val verticesDf = sqlContext.read.parquet(path + "/vertices")
    val edgesDf = sqlContext.read.parquet(path + "/edges")
    new Graph(GraphFrame(verticesDf, edgesDf))
  }
}
