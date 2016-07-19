package org.trustedanalytics.sparktk.models.classification.naive_bayes

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ FrameSchema, Frame, DataTypes, Column }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class NaiveBayesModelTest extends TestingSparkContextWordSpec with Matchers {

  val labeledPoint: Array[Row] = Array(
    new GenericRow(Array[Any](1, 16.8973559126, 2.6933495054)),
    new GenericRow(Array[Any](1, 5.5548729596, 2.7777687995)),
    new GenericRow(Array[Any](0, 46.1810010826, 3.1611961917)),
    new GenericRow(Array[Any](0, 44.3117586448, 3.3458963222)))
  val schema = new FrameSchema(List(Column("col0", DataTypes.int32), Column("col1", DataTypes.float64), Column("col2", DataTypes.float64)))

  "NaiveBayesModel" should {
    "create a NaiveBayesModel" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = NaiveBayesModel.train(frame, "col0", List("col1", "col2"))

      model shouldBe a[NaiveBayesModel]
    }

    "throw an IllegalArgumentException for empty observationColumns" in {
      intercept[IllegalArgumentException] {
        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)

        val model = NaiveBayesModel.train(frame, "", List("col1", "col2"))
      }
    }

    "throw an IllegalArgumentException for empty labelColumn" in {
      intercept[IllegalArgumentException] {
        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)

        val model = NaiveBayesModel.train(frame, "col0", List())
      }
    }
  }

}
