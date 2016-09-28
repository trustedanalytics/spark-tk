package org.trustedanalytics.sparktk.models.classification.random_forest_classifier

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class RandomForestClassifierModelTest extends TestingSparkContextWordSpec with Matchers {

  val labeledPoint: Array[Row] = Array(
    new GenericRow(Array[Any](1, 16.8973559126, 2.6933495054)),
    new GenericRow(Array[Any](1, 5.5548729596, 2.7777687995)),
    new GenericRow(Array[Any](0, 46.1810010826, 3.1611961917)),
    new GenericRow(Array[Any](0, 44.3117586448, 3.3458963222)))
  val schema = new FrameSchema(List(Column("label", DataTypes.int32), Column("obs1", DataTypes.float64), Column("obs2", DataTypes.float64)))

  "RandomForestClassifierModel" should {
    "create a RandomForestClassifierModel" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)

      val model = RandomForestClassifierModel.train(frame, "label", List("obs1", "obs2"), 2, 1, "gini", 4, 100, 10, None, None)
      model shouldBe a[RandomForestClassifierModel]
    }

    "throw an IllegalArgumentException for empty observationColumns" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)

        val model = RandomForestClassifierModel.train(frame, "label", List(), 2, 1, "gini", 4, 100, 10, None, None)
      }
    }

    "throw an IllegalArgumentException for empty labelColumn" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)

        val model = RandomForestClassifierModel.train(frame, "", List("obs1", "obs2"), 2, 1, "gini", 4, 100, 10, None, None)
      }
    }

    "return predictions when calling the random forest classifier model score" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestClassifierModel.train(frame, "label", List("obs1", "obs2"), 2, 1, "gini", 4, 100, 10, None, None)

      // Test values for scoring
      val inputValues = Array[Any](44.3117586448, 3.3458963222)
      val classLabel = 0.0

      // Score and check the results
      val scoreResult = model.score(inputValues)
      assert(inputValues.length == model.input.length)
      assert(scoreResult.length == model.output.length)
      assert(scoreResult.slice(0, inputValues.length).sameElements(inputValues))
      scoreResult(inputValues.length) match {
        case prediction: Double => assert(prediction == classLabel)
        case _ => throw new RuntimeException(s"Expected prediction to be a Double but is ${scoreResult(1).getClass.getSimpleName}")
      }
    }

    "throw IllegalArgumentExceptions for invalid scoring parameters" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestClassifierModel.train(frame, "label", List("obs1", "obs2"), 2, 1, "gini", 4, 100, 10, None, None)

      intercept[IllegalArgumentException] {
        model.score(null)
      }

      intercept[IllegalArgumentException] {
        model.score(Array[Any]())
      }

      intercept[IllegalArgumentException] {
        model.score(Array[Any](44.3117586448, 3.3458963222, "bogus"))
      }
    }
  }

}
