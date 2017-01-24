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

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame._
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text

/**
 * Functions for loading records which possibly span multiple lines
 */
object ImportMultiLineRecords extends Serializable {

  /**
   * Imports a file of JSON records
   *
   * JSON records can span multiple lines.  Returns a Frame of one column containing a JSON string per row
   *
   *
   * @param sc Active SparkContext
   * @param fileName location of the file to import
   * @return new Frame
   *
   * Examples
   * --------
   *
   * Consider a file of JSON records:
   *
   *        { "obj": {
   *            "color": "blue",
   *            "size": 4,
   *            "shape": "square" }
   *          }
   *          { "obj": {
   *          "color": "green",
   *          "size": 3,
   *          "shape": "triangle" }
   *          }
   *          { "obj": { "color": "yellow", "size": 5, "shape": "pentagon" } }
   *          { "obj": {
   *          "color": "orange",
   *          "size": 2,
   *          "shape": "lentil" }
   *        }
   *
   *  importJson creates a Frame with one column where each row contains a JSON string, something like...
   *
   *  [#]  records
   *  =====================================================================
   *  [0]  { "obj": {
   *       "color": "blue",
   *       "size": 4,
   *       "shape": "square" }
   *       }
   *  [1]  { "obj": {
   *       "color": "green",
   *       "size": 3,
   *       "shape": "triangle" }
   *       }
   *  [2]  { "obj": { "color": "yellow", "size": 5, "shape": "pentagon" } }
   *  [3]  { "obj": {
   *        "color": "orange",
   *        "size": 2,
   *        "shape": "lentil" }
   *       }
   */
  def importJson(sc: SparkContext, fileName: String): Frame = {
    parseMultiLineRecords(sc, fileName, startTag = List("{"), endTag = List("}"))
  }

  /**
   *
   * Imports a file of XML records
   *
   * XML records can span multiple lines.  Returns a Frame of one column containing a XML string per row
   *
   * Note: Only records which start with the given tag will be included (multiple different tags not supported)
   *
   * @param sc Active SparkContext
   * @param fileName location of the file to import
   * @param recordTag value of the XML element which contains a record
   * @return new Frame
   *
   * Examples
   * --------
   *
   * Consider a file of XML records:
   *
   *     <?xml version="1.0" encoding="UTF-8"?>
   *     <table>
   *         <shape type="triangle">
   *             <x>0</x>
   *             <y>0</y>
   *             <size>12</size>
   *         </shape>
   *         <shape type="square">
   *             <x>8</x>
   *             <y>0</y>
   *             <size>4</size>
   *         </shape>
   *         <shape color="blue" type="pentagon">
   *             <x>0</x>
   *             <y>10</y>
   *             <size>2</size>
   *         </shape>
   *         <shape type="square">
   *             <x>-4</x>
   *             <y>6</y>
   *             <size>7</size>
   *         </shape>
   *     </table>
   *
   *  importXml("shapes.xml", "shape") creates a Frame with one column where each row contains an XML string,
   *  something like...
   *
   *     [#]  records
   *     =========================================
   *     [0]  <shape type="triangle">
   *                  <x>0</x>
   *                  <y>0</y>
   *                  <size>12</size>
   *              </shape>
   *     [1]  <shape type="square">
   *                  <x>8</x>
   *                  <y>0</y>
   *                  <size>4</size>
   *              </shape>
   *     [2]  <shape color="blue" type="pentagon">
   *                  <x>0</x>
   *                  <y>10</y>
   *                  <size>2</size>
   *              </shape>
   *     [3]  <shape type="square">
   *                  <x>-4</x>
   *                  <y>6</y>
   *                  <size>7</size>
   *              </shape>
   *
   */
  def importXml(sc: SparkContext, fileName: String, recordTag: String): Frame = {
    parseMultiLineRecords(sc,
      fileName,
      startTag = List(s"<$recordTag>", s"<$recordTag "),
      endTag = List(s"</$recordTag>"))
  }

  /**
   * Load records consisting of possibly multiple lines from file into a Frame.
   *
   * Uses a start and end tagging scheme to determine what should be considered a "record"
   *
   * Typically used for JSON or XML records which may span multiple lines in a given file
   *
   * @param sc SparkContext used for textFile reading
   * @param fileName name of file to parse
   * @param startTag Start tag for XML or JSON parsers
   * @param endTag Start tag for XML or JSON parsers
   * @param isXml True for XML input files
   * @return Frame of records (one of column of strings)
   */
  def parseMultiLineRecords(sc: SparkContext,
                            fileName: String,
                            startTag: List[String],
                            endTag: List[String],
                            isXml: Boolean = false): Frame = {

    require(startTag != null && startTag.nonEmpty, s"startTag was not provided (null or length of 0)")
    require(endTag != null && endTag.nonEmpty, s"endTag was not provided (null or length of 0)")

    val conf = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration)
    conf.setStrings(MultiLineTaggedInputFormat.START_TAG_KEY, startTag: _*) //Treat s as a Varargs parameter
    conf.setStrings(MultiLineTaggedInputFormat.END_TAG_KEY, endTag: _*)
    conf.setBoolean(MultiLineTaggedInputFormat.IS_XML_KEY, isXml)

    val rdd: RDD[Row] =
      sc.newAPIHadoopFile[LongWritable, Text, MultiLineTaggedInputFormat](fileName, classOf[MultiLineTaggedInputFormat], classOf[LongWritable], classOf[Text], conf)
        .map(row => row._2.toString)
        .filter(_.trim() != "")
        .map(s => Row(s))
    val listColumn = List(Column("records", DataTypes.str))
    new Frame(rdd, new FrameSchema(listColumn))
  }
}
