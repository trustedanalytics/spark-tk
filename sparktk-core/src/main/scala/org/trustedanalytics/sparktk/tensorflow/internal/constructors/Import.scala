package org.trustedanalytics.sparktk.tensorflow.internal.constructors

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.tensorflow.hadoop.io.TFRecordFileInputFormat
import org.tensorflow.example._
import org.trustedanalytics.sparktk.tensorflow.internal.DefaultTfRecordRowDecoder

object Import extends Serializable{

  def importTfRecord(sc:SparkContext, sourcePath: String, schema: Option[StructType] = None):RDD[Row]={
    val rdd = sc.newAPIHadoopFile(sourcePath, classOf[TFRecordFileInputFormat], classOf[BytesWritable], classOf[NullWritable])
    val rows = rdd.map{
      case(bytesWritable, nullWritable) =>
        val example = Example.parseFrom(bytesWritable.getBytes)
        val row =  DefaultTfRecordRowDecoder.decodeTfRecord(example, schema)
        row
    }
    rows.count()
    rows
  }
}