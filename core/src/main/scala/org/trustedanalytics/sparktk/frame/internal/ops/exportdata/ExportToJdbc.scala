package org.trustedanalytics.sparktk.frame.internal.ops.exportdata

import java.sql.SQLException
import java.util.Properties
import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ExportToJdbcSummarization extends BaseFrame {

  /**
   * Write current frame to JDBC table.
   *
   * Table will be created or appended to.Export of Vectors is not currently supported.
   *
   * @param connectionUrl JDBC connection url to database server
   * @param tableName     JDBC table name
   */
  def exportToJdbc(connectionUrl: String, tableName: String) = {
    execute(ExportToJdbc(connectionUrl, tableName))
  }
}

case class ExportToJdbc(connectionUrl: String, tableName: String) extends FrameSummarization[Unit] {

  require(StringUtils.isNotEmpty(tableName), "table name is required")
  require(StringUtils.isNotEmpty(connectionUrl), "connection url is required")

  override def work(state: FrameState): Unit = {
    ExportToJdbc.exportToJdbcTable(state, connectionUrl, tableName)
  }
}

object ExportToJdbc {

  def exportToJdbcTable(frameRdd: FrameRdd,
                        connectionUrl: String,
                        tableName: String) = {
    val frame: FrameRdd = frameRdd
    val dataFrame = frame.toDataFrame
    try {
      dataFrame.write.jdbc(connectionUrl, tableName, new Properties)
    }
    catch {
      case e: SQLException => throw e
    }
  }
}