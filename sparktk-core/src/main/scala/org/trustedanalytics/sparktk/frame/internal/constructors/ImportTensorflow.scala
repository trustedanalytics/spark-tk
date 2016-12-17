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
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.tensorflow.example.Example
import org.tensorflow.hadoop.io.TFRecordFileInputFormat
import org.trustedanalytics.sparktk.frame.{DataTypes, Frame, Schema, _}
import org.trustedanalytics.sparktk.frame.internal.serde.DefaultTfRecordRowDecoder

object ImportTensorflow {

  /**
   * Import tensorflow records as frame
   *
   */
  def importTensorflow(sc: SparkContext, sourceTfRecordsPath: String, schema: Option[StructType] = None): Frame = {
    require(StringUtils.isNotEmpty(sourceTfRecordsPath), "path should not be null or empty.")

    val rdd = sc.newAPIHadoopFile(sourceTfRecordsPath, classOf[TFRecordFileInputFormat], classOf[BytesWritable], classOf[NullWritable])
    val resultRdd = rdd.map {
      case (bytesWritable, nullWritable) =>
        val example = Example.parseFrom(bytesWritable.getBytes)



        val row = DefaultTfRecordRowDecoder.decodeTfRecord(example, schema)
        row
    }
    val frame = new Frame(resultRdd, tfSchema)
    frame
  }

}
