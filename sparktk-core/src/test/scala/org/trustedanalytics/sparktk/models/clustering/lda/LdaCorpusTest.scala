package org.trustedanalytics.sparktk.models.clustering.lda

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Frame, Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class LdaCorpusTest extends TestingSparkContextWordSpec with Matchers {

  val edgeData: Array[Row] = Array(
    Row("nytimes", "harry", 3L),
    Row("nytimes", "economy", 35L),
    Row("nytimes", "jobs", 40L),
    Row("nytimes", "magic", 1L),
    Row("nytimes", "realestate", 15L),
    Row("nytimes", "movies", 6L),
    Row("economist", "economy", 50L),
    Row("economist", "jobs", 35L),
    Row("economist", "realestate", 20L),
    Row("economist", "movies", 1L),
    Row("economist", "harry", 1L),
    Row("economist", "magic", 1L),
    Row("harrypotter", "harry", 40L),
    Row("harrypotter", "magic", 30L),
    Row("harrypotter", "chamber", 20L),
    Row("harrypotter", "secrets", 30L)
  )

  val edgeSchema = FrameSchema(List(
    Column("document", DataTypes.string),
    Column("word", DataTypes.string),
    Column("word_count", DataTypes.int64)
  ))

  "LDA corpus" should {

    "add word Ids to edge frame" in {
      val rows = sparkContext.parallelize(edgeData)
      val frame = new Frame(rows, edgeSchema)
      val trainArgs = LdaTrainArgs(frame, "document", "word", "word_count", numTopics = 2)

      val ldaCorpus = LdaCorpus(trainArgs)
      val edgesWithWordIds = ldaCorpus.addWordIdsToEdgeFrame().collect()
      val wordIdMap = ldaCorpus.uniqueWordsFrame.map(row => {
        (row(1).asInstanceOf[String], row(0).asInstanceOf[Long])
      }).collectAsMap()

      edgesWithWordIds.foreach(row => {
        val word = row(1).asInstanceOf[String]
        val wordId = row(3).asInstanceOf[Long]
        assert(wordId == wordIdMap(word))
      })
    }

    "create corpus of documents for training LDA model" in {
      val rows = sparkContext.parallelize(edgeData)
      val frame = new Frame(rows, edgeSchema)
      val trainArgs = LdaTrainArgs(frame, "document", "word", "word_count", numTopics = 2)

      val ldaCorpus = LdaCorpus(trainArgs)
      val idWordMap = ldaCorpus.uniqueWordsFrame.map(row => {
        (row(0).asInstanceOf[Long], row(1).asInstanceOf[String])
      }).collectAsMap()

      val trainCorpus = ldaCorpus.createCorpus().collect()
      val docWordCountMap = edgeData.map(row => {
        val document = row(0).asInstanceOf[String]
        val word = row(1).asInstanceOf[String]
        val wordCount = row(2).asInstanceOf[Long]
        ((document, word), wordCount)
      }).toMap

      trainCorpus.foreach {
        case (docId, (doc, wordCountVector)) =>
          for (i <- wordCountVector.toArray.indices) {
            val wordCount = wordCountVector.toArray(i)
            val word = idWordMap(i.toLong)

            if (docWordCountMap.contains(doc, word)) {
              assert(wordCount == docWordCountMap(doc, word))
            }
            else {
              assert(wordCount == 0)
            }
          }
      }
    }

    "return empty frame" in {
      val rows = sparkContext.parallelize(Array.empty[Row])
      val frame = new Frame(rows, edgeSchema)
      val trainArgs = LdaTrainArgs(frame, "document", "word", "word_count", numTopics = 2)

      val ldaCorpus = LdaCorpus(trainArgs)
      val trainCorpus = ldaCorpus.createCorpus().collect()

      assert(trainCorpus.isEmpty)
    }

    "throw an IllegalArgumentException if edge frame is null" in {
      intercept[IllegalArgumentException] {
        val trainArgs = LdaTrainArgs(null, "document", "word", "word_count", numTopics = 2)
        LdaCorpus(trainArgs)
      }
    }

    "throw an IllegalArgumentException if train arguments are null" in {
      intercept[IllegalArgumentException] {
        LdaCorpus(null)
      }
    }

    "throw a SparkException for invalid column names" in {
      intercept[SparkException] {
        val rows = sparkContext.parallelize(edgeData)
        val frame = new Frame(rows, edgeSchema)
        val trainArgs = LdaTrainArgs(frame, "document", "word", "word_count", numTopics = 2)
        val invalidTrainArgs = LdaTrainArgs(frame, "invalid_document", "invalid_word", "invalid_count")
        LdaCorpus(invalidTrainArgs).createCorpus()
      }
    }
  }
}
