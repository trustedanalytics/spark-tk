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
package org.trustedanalytics.sparktk.frame.internal.ops.exportdata

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ExportToHiveSummarization extends BaseFrame {

  /**
   * Write current frame to Hive table.
   *
   * Table must not exist in Hive. Hive does not support case sensitive table names and columns names.
   * Hence column names with uppercase letters will be converted to lower case by Hive.
   *
   * @param hiveTableName The name of the Hive table that will contain the exported frame
   */
  def exportToHive(hiveTableName: String) = {
    execute(ExportToHive(hiveTableName))
  }
}

case class ExportToHive(tableName: String) extends FrameSummarization[Unit] {

  require(StringUtils.isNotEmpty(tableName), "Hive table name required")

  override def work(state: FrameState): Unit = {
    ExportToHive.exportToHiveTable(state, tableName)
  }
}

object ExportToHive {

  def exportToHiveTable(frameRdd: FrameRdd,
                        hiveTableName: String) = {

    val dataFrame = frameRdd.toDataFrameUsingHiveContext
    dataFrame.registerTempTable("mytable")

    val beginString = "{\"name\": \"" + hiveTableName + "\",\"type\": \"record\",\"fields\": "
    val array = FrameRdd.schemaToAvroType(frameRdd.frameSchema).map(column => "{\"name\":\"" + column._1 + "\", \"type\":[\"null\",\"" + column._2 + "\"]}")
    val colSchema = array.mkString("[", ",", "]")
    val endString = "}"
    val schema = beginString + colSchema + endString

    dataFrame.sqlContext.asInstanceOf[org.apache.spark.sql.hive.HiveContext].sql(s"CREATE TABLE " + hiveTableName +
      s" ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS AVRO TBLPROPERTIES ('avro.schema.literal'= '${schema}' ) AS SELECT * FROM mytable")
  }
}