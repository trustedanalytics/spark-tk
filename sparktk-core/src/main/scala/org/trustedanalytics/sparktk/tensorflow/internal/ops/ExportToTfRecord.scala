package org.trustedanalytics.sparktk.tensorflow.internal.ops

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import org.apache.spark.sql.DataFrame
import org.trustedanalytics.sparktk.tensorflow.internal.DefaultTfRecordRowEncoder


object ExportToTfRecord extends Serializable{

  def exportToTfRecord(sourceDataframe: DataFrame, destPath: String)={

    val features = sourceDataframe.map(row => {
      val example = DefaultTfRecordRowEncoder.encodeTfRecord(row)
      (new BytesWritable(example.toByteArray), NullWritable.get())
    })
    features.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](destPath)
  }
}
