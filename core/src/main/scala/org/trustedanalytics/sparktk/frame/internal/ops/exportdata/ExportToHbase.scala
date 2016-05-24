package org.trustedanalytics.sparktk.frame.internal.ops.exportdata

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ExportToHbaseSummarization extends BaseFrame {

  /**
   * Write current frame to HBase table.
   *
   * Table must exist in HBase.
   *
   * @param tableName The name of the HBase table that will contain the exported frame
   * @param keyColumnName The name of the column to be used as row key in hbase table
   * @param familyName The family name of the HBase table that will contain the exported frame
   */
  def exportToHbase(tableName: String, keyColumnName: Option[String] = None, familyName: String = "familyColumn") = {
    execute(ExportToHbase(tableName, keyColumnName, familyName))
  }
}

case class ExportToHbase(tableName: String, keyColumnName: Option[String], familyName: String) extends FrameSummarization[Unit] {

  require(tableName != null, "Hbase table name is required")
  override def work(state: FrameState): Unit = {
    ExportToHbase.exportToHbaseTable(state, tableName, keyColumnName, familyName)
  }
}

object ExportToHbase {

  def exportToHbaseTable(frameRdd: FrameRdd,
                         tableName: String,
                         keyColumnName: Option[String],
                         familyName: String) = {
  }
}