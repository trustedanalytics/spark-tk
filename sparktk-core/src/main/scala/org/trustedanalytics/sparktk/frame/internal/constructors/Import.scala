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
   * Creates a frame by importing the data as strings from the specified csv file.  If the csv file has
   * a header row, those values will be used as column names.  Otherwise, columns will be named generically,
   * like 'C0', 'C1', 'C2', etc.
   *
   * @param path Full path to the csv file
   * @param delimiter A string which indicates the separation of data fields.  This is usually a single
   *                  character and could be a non-visible character, such as a tab. The default delimiter
   *                  is a comma (,).
   * @param header Boolean value indicating if the first line of the file will be used to name columns,
   *               and not be included in the data.  The default value is false.
   * @return Frame with data from the csv file
   */
  def importCsvRaw(sc: SparkContext,
                   path: String,
                   delimiter: String = ",",
                   header: Boolean = false): Frame = {
    require(StringUtils.isNotEmpty(path), "path should not be null or empty.")
    require(StringUtils.isNotEmpty(delimiter), "delimiter should not be null or empty.")

    // Load from csv
    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)
    val dfr = sqlContext.read.format("com.databricks.spark.csv.org.trustedanalytics.sparktk")
      .option("header", header.toString.toLowerCase)
      .option("inferSchema", "false")
      .option("delimiter", delimiter)
    val df = dfr.load(path)
    val frameRdd = FrameRdd.toFrameRdd(df)
    new Frame(frameRdd, frameRdd.frameSchema)
  }

  /**
   * Creates a frame by importing data from a CSV file
   *
   * @param path Full path to the csv file
   * @param delimiter A string which indicates the separation of data fields.  This is usually a single
   *                  character and could be a non-visible character, such as a tab. The default delimiter
   *                  is a comma (,).
   * @param header Boolean value indicating if the first line of the file will be used to name columns (unless a schema
   *               is provided), and not be included in the data.  The default value is false.
   * @param schema Optionally specify the schema or column names for the dataset. If a schema with data types is
   *               provided, and the value from the csv file cannot be converted to the data type specified by the
   *               schema (for example, if the csv file has a string, and the schema specifies an int), the value will
   *               show up as missing (None) in the frame.  If just column names or no schema is defined, the column
   *               types will be automatically inferred based on the data found in the file (this requires an extra pass
   *               over the data).  If column names are specified, it will override the header values.  If there is no
   *               header, and no column names are specified, the columns will be named generically ("C0", C1", "C2", etc).
   * @param dateTimeFormat String specifying how date/time columns are formatted, using the java.text.SimpleDateFormat
   * specified at https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
   * @return Frame with data from the csv file
   */
  def importCsv(sc: SparkContext,
                path: String,
                delimiter: String = ",",
                header: Boolean = false,
                schema: Option[Either[Schema, List[String]]] = None,
                dateTimeFormat: String = "yyyy-MM-dd'T'HH:mm:ss.SSSX"): Frame = {
    require(StringUtils.isNotEmpty(path), "path should not be null or empty.")
    require(StringUtils.isNotEmpty(delimiter), "delimiter should not be null or empty.")

    // Load from csv
    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)
    val headerStr = header.toString.toLowerCase
    var overrideColumnNames = List[String]()
    var fullSchema: Option[Schema] = None

    val inferSchema: Boolean = if (schema.isDefined) {
      schema.get match {
        case Left(s) =>
          require(s != null, "schema must not be null.")
          fullSchema = Some(s)
          false
        case Right(c) =>
          require(c != null && c.length > 0, "schema column name list must not be null or empty.")
          overrideColumnNames = c
          true
      }
    }
    else true

    var dfr = sqlContext.read.format("com.databricks.spark.csv.org.trustedanalytics.sparktk")
      .option("header", headerStr)
      .option("inferSchema", inferSchema.toString.toLowerCase)
      .option("delimiter", delimiter)
      .option("dateFormat", dateTimeFormat)

    if (fullSchema.isDefined) {
      dfr = dfr.schema(StructType(fullSchema.get.columns.map(column =>
        StructField(column.name, FrameRdd.schemaDataTypeToSqlDataType(column.dataType), true))))
    }

    val df = dfr.load(path)
    val frameRdd = FrameRdd.toFrameRdd(df)

    if (fullSchema.isDefined) {
      val numSpecifiedColumns = fullSchema.get.columns.length
      val numColumnsFromLoad = frameRdd.frameSchema.columns.length
      if (numSpecifiedColumns != numColumnsFromLoad)
        throw new IllegalArgumentException("""The number of columns specified in the schema ($numSpecifiedColumns) does
                                           not match the number of columns found in the csv file ($numColumnsFromLoad).""")
    }

    val frameSchema = if (fullSchema.isDefined) {
      fullSchema.get
    }
    else {
      val numFrameColumns = frameRdd.frameSchema.columnNames.length
      var tempSchema = frameRdd.frameSchema
      // Override column names
      for ((newName, i) <- overrideColumnNames.zipWithIndex) {
        if (i < numFrameColumns) {
          tempSchema = tempSchema.renameColumn(tempSchema.columnNames(i), newName)
        }
      }

      tempSchema
    }

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
