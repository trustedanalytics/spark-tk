package org.trustedanalytics.at.frame

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.at.frame.ops._
import org.trustedanalytics.at.frame.ops.binning.BinColumnTrait
import org.trustedanalytics.at.frame.ops.export.ExportToCsvTrait
import org.trustedanalytics.at.frame.rdd.PythonJavaRdd
import org.trustedanalytics.at.frame.schema.Schema

class Frame(frameRdd: RDD[Row], frameSchema: Schema) extends BaseFrame // params named "frameRdd" and "frameSchema" because naming them "rdd" and "schema" masks the base members "rdd" and "schema" in this scope
    with AddColumnsTrait
    with BinColumnTrait
    with CountTrait
    with ExportToCsvTrait
    with SaveTrait
    with TakeTrait {
  init(frameRdd, frameSchema)

  /**
   * (typically called from pyspark, with jrdd)
   * @param jrdd java array of Any
   * @param schema frame schema
   */
  def this(jrdd: JavaRDD[Array[Any]], schema: Schema) = {
    this(PythonJavaRdd.toRowRdd(jrdd.rdd, schema), schema)
  }
}
