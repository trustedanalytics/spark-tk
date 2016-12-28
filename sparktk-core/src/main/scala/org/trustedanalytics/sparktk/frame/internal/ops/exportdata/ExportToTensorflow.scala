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

import org.apache.hadoop.io.{ BytesWritable, NullWritable }
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.serde.DefaultTfRecordRowEncoder
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameState, FrameSummarization }

trait ExportToTensorflowSummarization extends BaseFrame {
  /**
   *
   * Export the frame as tensorflow records
   *
   */
  def exportToTensorflow(destinationPath: String) = {
    execute(ExportToTensorflow(destinationPath))
  }
}

case class ExportToTensorflow(destinationPath: String) extends FrameSummarization[Unit] {

  override def work(state: FrameState): Unit = {
    val sourceFrame = new Frame(state.rdd, state.schema)
    val features = sourceFrame.dataframe.map(row => {
      val example = DefaultTfRecordRowEncoder.encodeTfRecord(row)
      (new BytesWritable(example.toByteArray), NullWritable.get())
    })
    features.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](destinationPath)
  }
}

