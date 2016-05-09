package org.trustedanalytics.sparktk.frame.internal.ops.exportdata

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.sparktk.frame.internal.rdd.{ FrameRdd, MiscFrameFunctions }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

import scala.collection.mutable.ArrayBuffer

trait ExportToJsonSummarization extends BaseFrame {

  def exportToJson(folderName: String, count: Int = 0, offset: Int = 0) = {
    execute(ExportToJson(folderName, count, offset))
  }
}

/**
 * *
 * Write current frame to HDFS in JSON format.
 *
 * @param folderName : The HDFS folder path where the files will be created.
 * @param count : The number of records you want. Default (0), or a non-positive value, is the whole frame.
 * @param offset : The number of rows to skip before exporting to the file. Default is zero (0).
 */
case class ExportToJson(folderName: String, count: Int = 0, offset: Int = 0) extends FrameSummarization[Unit] {

  require(folderName != null, "folder name is required")
  override def work(state: FrameState): Unit = {
    ExportToJson.exportToJsonFile(state, folderName, count, offset)
  }
}

object ExportToJson {

  def exportToJsonFile(frameRdd: FrameRdd,
                       filename: String,
                       count: Int,
                       offset: Int) = {

    val filterRdd = if (count > 0) MiscFrameFunctions.getPagedRdd(frameRdd, offset, count, -1) else frameRdd
    val headers = frameRdd.frameSchema.columnNames
    val jsonRDD = filterRdd.map {
      row =>
        {
          val strArray = row.toSeq.zip(headers).map {
            case (value, header) =>
              val str = value match {
                case null => "null"
                case s: String => "\"" + s + "\""
                case arr: ArrayBuffer[_] => arr.mkString("[", ",", "]")
                case seq: Seq[_] => seq.mkString("[", ",", "]")
                case x => x.toString
              }
              new String("\"" + header + "\":" + str)
          }

          strArray.mkString("{", ",", "}")
        }
    }
    jsonRDD.saveAsTextFile(filename)
    if (jsonRDD.isEmpty()) StringUtils.EMPTY else jsonRDD.first()
  }
}