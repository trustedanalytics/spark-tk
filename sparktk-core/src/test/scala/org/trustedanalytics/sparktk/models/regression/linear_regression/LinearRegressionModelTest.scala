package org.trustedanalytics.sparktk.models.regression.linear_regression

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class LinearRegressionModelTest extends TestingSparkContextWordSpec with Matchers {

  val rows: Array[Row] = Array(new GenericRow(Array[Any](0, 0)),
    new GenericRow(Array[Any](1, 2.5)),
    new GenericRow(Array[Any](2, 5.0)),
    new GenericRow(Array[Any](3, 7.5)),
    new GenericRow(Array[Any](4, 10)),
    new GenericRow(Array[Any](5, 12.5)),
    new GenericRow(Array[Any](6, 13.0)),
    new GenericRow(Array[Any](7, 17.15)),
    new GenericRow(Array[Any](8, 18.5)),
    new GenericRow(Array[Any](9, 23.5)))
  val schema = new FrameSchema(List(Column("x1", DataTypes.float32), Column("y", DataTypes.float32)))

  "LinearRegressionModel train" should {
    "create a LinearRegressionModel from training" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = LinearRegressionModel.train(frame, "y", List("x1"))

      model shouldBe a[LinearRegressionModel]
    }

    "throw an IllegalArgumentException for empty observationColumns during train" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      intercept[IllegalArgumentException] {
        LinearRegressionModel.train(frame, "y", List())
      }
    }

    "thow an IllegalArgumentException for empty labelColumn during train" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      intercept[IllegalArgumentException] {
        LinearRegressionModel.train(frame, "", List("x1"))
      }
    }
  }

  "LinearRegressionModel score" should {
    "return predictions when calling the linear regression model score" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = LinearRegressionModel.train(frame, "y", List("x1"))

      // Test values, just grabbed from the second row the of the training frame
      val x1 = 1.0
      val y = 2.5

      val inputArray = Array[Any](x1)
      assert(model.input().length == inputArray.length)
      val scoreResult = model.score(inputArray)
      assert(scoreResult.length == model.output().length)
      assert(scoreResult(0) == x1)
      scoreResult(1) match {
        case prediction: Double => assertAlmostEqual(prediction, y, 0.5)
        case _ => throw new RuntimeException(s"Expected prediction to be a Double but is ${scoreResult(1).getClass.getSimpleName}")
      }
    }

    "throw IllegalArgumentExceptions for invalid score parameters" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = LinearRegressionModel.train(frame, "y", List("x1"))

      // Wrong number of args (should match the number of observation columns)
      intercept[IllegalArgumentException] {
        model.score(Array[Any](0.0, 1.0, 2.0))
      }

      // Wrong type of arg (should be a double)
      intercept[IllegalArgumentException] {
        model.score(Array[Any]("a"))
      }
    }
  }
}
