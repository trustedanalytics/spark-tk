package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }
import org.trustedanalytics.sparktk.saveload.TkSaveLoad

trait SaveSummarization extends BaseGraph {
  /**
   * Save the current frame.
   *
   * @param path The destination path.
   */
  def save(path: String): Unit = {
    execute(Save(path))
  }
}

case class Save(path: String) extends GraphSummarization[Unit] {

  override def work(state: GraphState): Unit = {
    state.graphFrame.vertices.write.parquet(path + "/vertices")
    state.graphFrame.edges.write.parquet(path + "/edges")
    val formatId = Graph.formatId
    val formatVersion = Graph.tkFormatVersion
    TkSaveLoad.saveTk(state.graphFrame.vertices.sqlContext.sparkContext, path, formatId, formatVersion, "No Metadata")
  }
}
