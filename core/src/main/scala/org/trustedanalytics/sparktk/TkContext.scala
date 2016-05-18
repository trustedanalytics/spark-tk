package org.trustedanalytics.sparktk

import org.apache.spark.api.java.JavaSparkContext
import org.trustedanalytics.sparktk.frame.{ Frame, Schema }
import org.trustedanalytics.sparktk.frame.internal.ops.Load
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

class TkContext(jsc: JavaSparkContext) extends Serializable {

  private val sc = jsc.sc

  def helloWorld(): String = "Hello from TK"

  def loadFrame(path: String): Frame = {
    val frameRdd: FrameRdd = Load.loadParquet(path, sc)
    new Frame(frameRdd, frameRdd.frameSchema)
  }

  /**
   * Creates a frame and loads it with data from the specified csv file.
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
  def loadFrameFromCsv(path: String,
                       delimiter: String = ",",
                       header: Boolean = false,
                       inferSchema: Boolean = false,
                       schema: Option[Schema] = None): Frame = {
    // If a custom schema is provided there's no reason to infer the schema during the load
    val loadWithInferSchema = if (schema.isDefined) false else inferSchema

    // Load from csv
    val frameRdd: FrameRdd = Load.loadFromCsv(path, delimiter, header, loadWithInferSchema, sc)

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

}
