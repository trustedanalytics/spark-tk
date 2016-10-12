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
package org.trustedanalytics.sparktk.frame.internal.constructors

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.trustedanalytics.sparktk.frame._
import org.apache.spark.sql.types.{ StructType, StructField }
import org.trustedanalytics.sparktk.frame.{ DataTypes, Schema, Frame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

object Import {

  /**
   * Creates a frame by importing data from a CSV file
   *
   * @param path Full path to the csv file
   * @param delimiter A string which indicates the separation of data fields.  This is usually a single
   *                  character and could be a non-visible character, such as a tab. The default delimiter
   *                  is a comma (,).
   * @param header Boolean value indicating if the first line of the file will be used to name columns,
   *               and not be included in the data.  The default value is false.
   * @param inferSchema Boolean value indicating if the column types will be automatically inferred.  It
   *                    requires one extra pass over the data and is false by default.
   * @param dateFormat String specifying how date/time columns are formatted, using the java.text.SimpleDateFormat
                       specified at https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
   * @return Frame with data from the csv file
   */
  def importCsv(sc: SparkContext,
                path: String,
                delimiter: String = ",",
                header: Boolean = false,
                inferSchema: Boolean = false,
                schema: Option[Schema] = None,
                dateFormat: String = "yyyy-MM-dd'T'HH:mm:ss.SSSX"): Frame = {

    // If a custom schema is provided there's no reason to infer the schema during the load
    val loadWithInferSchema = if (schema.isDefined) false else inferSchema

    // Load from csv
    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)
    val headerStr = header.toString.toLowerCase
    val inferSchemaStr = inferSchema.toString.toLowerCase

    var dfr = sqlContext.read.format("com.databricks.spark.csv.org.trustedanalytics.sparktk")
      .option("header", headerStr)
      .option("inferSchema", inferSchemaStr)
      .option("delimiter", delimiter)
      .option("dateFormat", dateFormat)

    if (!inferSchema && schema.isDefined) {
      dfr = dfr.schema(StructType(schema.get.columns.map(column =>
        StructField(column.name, FrameRdd.schemaDataTypeToSqlDataType(column.dataType), true))))
    }

    val df = dfr.load(path)
    val frameRdd = FrameRdd.toFrameRdd(df)

    if (schema.isDefined) {
      val numSpecifiedColumns = schema.get.columns.length
      val numColumnsFromLoad = frameRdd.frameSchema.columns.length
      if (numSpecifiedColumns != numColumnsFromLoad)
        throw new IllegalArgumentException("""The number of columns specified in the schema ($numSpecifiedColumns) does
                                           not match the number of columns found in the csv file ($numColumnsFromLoad).""")
    }
    val frameSchema = if (schema.isDefined) schema.get else frameRdd.frameSchema

    new Frame(frameRdd, frameSchema)
  }

  /**
   * Creates a frame by importing data from a parquet file
   *
   * @param path Full path to the parquet file
   */
  def importParquet(sc: SparkContext, path: String): FrameRdd = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet(path)
    FrameRdd.toFrameRdd(df)
  }

  /**
   * Load data from hbase table into frame
   *
   * @param tableName hbase table name
   * @param schema hbase schema as a list of tuples (columnFamily, columnName, dataType for cell value)
   * @param startTag optional start tag for filtering
   * @param endTag optional end tag for filtering
   * @return frame with data from hbase table
   */
  def importHbase(sc: SparkContext,
                  tableName: String,
                  schema: Seq[Seq[String]],
                  startTag: Option[String] = None,
                  endTag: Option[String] = None): Frame = {

    require(StringUtils.isNotEmpty(tableName), "hbase name is required")
    require(schema != null, "hbase table schema cannot be null")
    require(startTag != null, "hbase table startTag cannot be null")
    require(endTag != null, "hbase table endTag cannot be null")

    //Map seq[seq[string]] to List[HBaseSchemaArgs]
    val finalSchema: Seq[HBaseSchemaArgs] = schema.map(item => HBaseSchemaArgs(item(0), item(1), DataTypes.toDataType(item(2))))
    val hBaseRdd = HbaseHelper.createRdd(sc, tableName, finalSchema.toVector, startTag, endTag).map(DataTypes.parseMany(finalSchema.map(_.dataType).toArray))
    val hBaseSchema = new FrameSchema(finalSchema.toVector.map {
      case x => Column(x.columnFamily + "_" + x.columnName, x.dataType)
    })
    val frameRdd = FrameRdd.toFrameRdd(hBaseSchema, hBaseRdd)
    new Frame(frameRdd, frameRdd.frameSchema)
  }

  /**
   * Loads data from given jdbc table into frame
   *
   * @param connectionUrl : Jdbc connection url to connect to database
   * @param tableName :Jdbc table name to import
   * @return Frame with data from jdbc table
   */
  def importJdbc(sc: SparkContext, connectionUrl: String, tableName: String): Frame = {

    require(StringUtils.isNotEmpty(connectionUrl), "connection url is required")
    require(StringUtils.isNotEmpty(tableName), "table name is required")

    // Load from jdbc table
    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)

    val sqlDataframe = sqlContext.read.format("jdbc")
      .option("url", connectionUrl)
      .option("dbtable", tableName)
      .load()

    val frameRdd = FrameRdd.toFrameRdd(sqlDataframe)
    new Frame(frameRdd, frameRdd.frameSchema)
  }

  /**
   * Loads data from hive using given hive query
   *
   * @param hiveQuery hive query
   * @return Frame with data based on hiveQL query
   */
  def importHive(sc: SparkContext, hiveQuery: String): Frame = {

    require(StringUtils.isNotEmpty(hiveQuery), "hive query is required")

    //Load data from hive using given hiveQL query
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val sqlDataframe = sqlContext.sql(hiveQuery)
    val frameRdd = FrameRdd.toFrameRdd(sqlDataframe)
    new Frame(frameRdd, frameRdd.frameSchema)
  }
}
