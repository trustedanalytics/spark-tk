package org.trustedanalytics.sparktk.models.clustering.lda

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.frame.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

/**
 * Assigns unique long Ids to words
 *
 * @param edgeFrame Frame of edges between documents and words
 * @param wordColumnName Word column name
 * @param wordCountColumnName Word count column name
 */
case class LdaWordIdAssigner(edgeFrame: FrameRdd,
                             wordColumnName: String,
                             wordCountColumnName: String) {
  require(edgeFrame != null, "edge frame is required")
  require(wordColumnName != null, "word column is required")
  require(wordCountColumnName != null, "word count column is required")

  val LdaWordPrefix: String = "_lda_word_"
  val ldaWordIdColumnName: String = LdaWordPrefix + "id_" + wordColumnName
  val ldaWordColumnName: String = LdaWordPrefix + wordColumnName
  val ldaWordCountColumnName: String = LdaWordPrefix + "total_" + wordCountColumnName

  /**
   * Assign unique Ids to words, and count total occurrences of each word in documents.
   *
   * @return Frame with word Id, word, and total count
   */
  def assignUniqueIds(): FrameRdd = {

    val wordsWithIndex: RDD[Row] = edgeFrame.mapRows(row => {
      (row.stringValue(wordColumnName), row.longValue(wordCountColumnName))
    }).reduceByKey(_ + _)
      .zipWithIndex()
      .map {
        case ((word, count), index) =>
          new GenericRow(Array[Any](index, word, count))
      }

    val schema = FrameSchema(List(
      Column(ldaWordIdColumnName, DataTypes.int64),
      Column(ldaWordColumnName, DataTypes.string),
      Column(ldaWordCountColumnName, DataTypes.int64))
    )

    new FrameRdd(schema, wordsWithIndex)
  }
}
