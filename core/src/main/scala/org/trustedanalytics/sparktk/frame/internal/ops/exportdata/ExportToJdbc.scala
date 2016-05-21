package org.trustedanalytics.sparktk.frame.internal.ops.exportdata

import java.sql.SQLException
import java.util.Properties
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ExportToJdbcSummarization extends BaseFrame {

  //  /**
  //    * Write current frame to JDBC table.
  //    *
  //    * @param tableName     JDBC table name
  //    * @param connectorType (optional) JDBC connector type
  //    * @param driverName    (optional) driver name
  //    * @param query         (optional) query for filtering. Not supported yet.
  //    *
  //    */
  //  def exportToJdbc(tableName: String, connectorType: Option[String] = None, driverName: Option[String] = None, query: Option[String] = None) = {
  //    execute(ExportToJdbc(tableName, connectorType, driverName, query))
  //  }

  /**
   * Write current frame to JDBC table.
   *
   * @param connectionUrl JDBC connection url to connection to postgres server
   * @param tableName JDBC table name
   */
  def exportToJdbc(connectionUrl: String, tableName: String) = {
    execute(ExportToJdbc(connectionUrl, tableName))
  }
}

//case class ExportToJdbc(tableName: String,
//                        connectorType: Option[String],
//                        driverName: Option[String],
//                        query: Option[String]) extends FrameSummarization[Unit] {
//
//  require(tableName != null, "table name is required")
//
//  override def work(state: FrameState): Unit = {
//    ExportToJdbc.exportToJdbcFile(state, tableName, connectorType, driverName, query)
//  }
//}

case class ExportToJdbc(connectionUrl: String, tableName: String) extends FrameSummarization[Unit] {

  require(tableName != null, "table name is required")

  override def work(state: FrameState): Unit = {
    ExportToJdbc.exportToJdbcFile(state, connectionUrl, tableName)
  }
}

object ExportToJdbc {

  def exportToJdbcFile(frameRdd: FrameRdd,
                       connectionUrl: String,
                       tableName: String) = {
    val frame: FrameRdd = frameRdd
    val dataFrame = frame.toDataFrame
    try {
      dataFrame.write.jdbc(connectionUrl, tableName, new Properties)
    }
    catch {
      case e: SQLException =>
        dataFrame.write.jdbc(connectionUrl, tableName, new Properties)
      //dataFrame.insertIntoJDBC(connectionArgs(JdbcFunctions.urlKey), connectionArgs(JdbcFunctions.dbTableKey), false)
    }
  }
}

//object ExportToJdbc {
//
//  def exportToJdbcFile(frameRdd: FrameRdd,
//                       tableName: String,
//                       connectorType: Option[String],
//                       driverName: Option[String],
//                       query: Option[String]) = {
//    val frame: FrameRdd = frameRdd
//    val connectionArgs = JdbcFunctions.buildConnectionArgs(tableName, connectorType, driverName)
//    val dataFrame = frame.toDataFrame
//    try {
////      dataFrame.createJDBCTable(connectionArgs(JdbcFunctions.urlKey), connectionArgs(JdbcFunctions.dbTableKey), false)
//      dataFrame.write.jdbc(connectionArgs(JdbcFunctions.urlKey), connectionArgs(JdbcFunctions.dbTableKey), new Properties)
//    }
//    catch {
//      case e: SQLException =>
//        dataFrame.insertIntoJDBC(connectionArgs(JdbcFunctions.urlKey), connectionArgs(JdbcFunctions.dbTableKey), false)
//    }
//  }
//}