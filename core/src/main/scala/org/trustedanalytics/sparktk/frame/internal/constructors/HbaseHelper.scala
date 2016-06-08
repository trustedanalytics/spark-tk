/**
 *  Copyright (c) 2015 Intel Corporation 
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

package org.trustedanalytics.sparktk.frame.internal.constructors

import _root_.org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.trustedanalytics.sparktk.frame.DataTypes
import org.apache.spark._

/**
 * Helper class for creating an RDD from hBase
 */
object HbaseHelper extends Serializable {

  /**
   *
   * @param sc default spark context
   * @param tableName hBase table to read from
   * @param schema hBase schema for the table above
   * @param startTag optional start tag to filter the database rows
   * @param endTag optional end tag to filter the database rows
   * @return an RDD of converted hBase values
   */
  def createRdd(sc: SparkContext, tableName: String, schema: Vector[HBaseSchemaArgs], startTag: Option[String], endTag: Option[String]): RDD[Array[Any]] = {
    val hBaseRDD = sc.newAPIHadoopRDD(createConfig(tableName, startTag, endTag),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    hbaseRddToRdd(hBaseRDD, schema)
  }

  def hbaseRddToRdd(hbaseRDD: RDD[(ImmutableBytesWritable, Result)], schema: Vector[HBaseSchemaArgs]): RDD[Array[Any]] =
    hbaseRDD.map {
      case (key, row) => {
        val values = for { element <- schema }
          yield getValue(row, element.columnFamily, element.columnName, element.dataType)

        values.toArray
      }
    }

  /**
   * Get value for cell
   *
   * @param row hBase data
   * @param columnFamily hBase column family
   * @param columnName hBase column name
   * @param dataType internal data type of the cell
   * @return the value for the cell as per the specified datatype
   */
  private def getValue(row: Result, columnFamily: String, columnName: String, dataType: DataType): Any = {
    val value = row.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))
    if (value == null)
      null
    else
      DataTypes.valAsDataType(Bytes.toString(value), dataType)
  }

  /**
   * Create initial configuration for bHase reader
   *
   * @param tableName name of hBase table
   * @return hBase configuration
   */
  private def createConfig(tableName: String, startTag: Option[String], endTag: Option[String]): Configuration = {
    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    if (startTag.isDefined) {
      conf.set(TableInputFormat.SCAN_ROW_START, startTag.get)
    }
    if (endTag.isDefined) {
      conf.set(TableInputFormat.SCAN_ROW_STOP, endTag.get)
    }

    conf
  }
}

/**
 * Arguments for the schema
 *
 * @param columnFamily hbase column family
 * @param columnName hbase column name
 * @param dataType data type for the cell
 */
case class HBaseSchemaArgs(columnFamily: String, columnName: String, dataType: DataType)
