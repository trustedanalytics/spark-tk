package org.trustedanalytics.sparktk.frame.internal.constructors

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.trustedanalytics.sparktk.frame.{ Schema, Frame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.apache.commons.lang.StringUtils

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
   * @return Frame with data from the csv file
   */
  def importCsv(sc: SparkContext,
                path: String,
                delimiter: String = ",",
                header: Boolean = false,
                inferSchema: Boolean = false,
                schema: Option[Schema] = None): Frame = {

    // If a custom schema is provided there's no reason to infer the schema during the load
    val loadWithInferSchema = if (schema.isDefined) false else inferSchema

    // Load from csv
    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)
    val headerStr = header.toString.toLowerCase
    val inferSchemaStr = inferSchema.toString.toLowerCase

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", headerStr)
      .option("inferSchema", inferSchemaStr)
      .option("delimiter", delimiter)
      .load(path)
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
