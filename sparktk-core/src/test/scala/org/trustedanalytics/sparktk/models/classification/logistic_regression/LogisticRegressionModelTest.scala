package org.trustedanalytics.sparktk.models.classification.logistic_regression

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class LogisticRegressionModelTest extends TestingSparkContextWordSpec with Matchers {

  // Test training data and schema
  val rows: Array[Row] = Array(new GenericRow(Array[Any](4.9, 1.4, 0)),
    new GenericRow(Array[Any](4.7, 1.3, 0)),
    new GenericRow(Array[Any](4.6, 1.5, 0)),
    new GenericRow(Array[Any](6.3, 4.9, 1)),
    new GenericRow(Array[Any](6.1, 4.7, 1)),
    new GenericRow(Array[Any](6.4, 4.3, 1)),
    new GenericRow(Array[Any](6.6, 4.4, 1)),
    new GenericRow(Array[Any](7.2, 6.0, 2)),
    new GenericRow(Array[Any](7.2, 5.8, 2)),
    new GenericRow(Array[Any](7.4, 6.1, 2)),
    new GenericRow(Array[Any](7.9, 6.4, 2)))
  val schema = new FrameSchema(List(Column("Sepal_Length", DataTypes.float32),
    Column("Petal_Length", DataTypes.float32),
    Column("Class", DataTypes.int32)))
  val obsColumns = List("Sepal_Length", "Petal_Length")
  val labelColumn = "Class"

  "LogisticRegressionModel train" should {
    "return a LogisticRegressionModel after training" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = LogisticRegressionModel.train(frame, obsColumns, labelColumn, None, 3)

      model shouldBe a[LogisticRegressionModel]
    }

    "throw exceptions for invalid training parameters" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      // empty observation column list
      intercept[IllegalArgumentException] {
        LogisticRegressionModel.train(frame, List(), labelColumn)
      }

      // invalid observation column
      intercept[IllegalArgumentException] {
        LogisticRegressionModel.train(frame, List("bogus"), labelColumn)
      }

      // invalid label column
      intercept[IllegalArgumentException] {
        LogisticRegressionModel.train(frame, obsColumns, "bogus")
      }

      // empty label column
      intercept[IllegalArgumentException] {
        LogisticRegressionModel.train(frame, obsColumns, "")
      }

      // invalid optimizer
      intercept[IllegalArgumentException] {
        LogisticRegressionModel.train(frame, obsColumns, labelColumn, None, 3, "bogus")
      }
    }
  }

  "LogisticRegressionModel score" should {
    "return predictions when calling the linear regression model score" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = LogisticRegressionModel.train(frame, obsColumns, labelColumn, None, 3)

      // Test observation and label data for scoring
      val inputArray = Array[Any](4.9, 1.4)
      val classLabel = 0

      // Score and check the results
      val scoreResult = model.score(inputArray)
      assert(model.input().length == inputArray.length)
      assert(scoreResult.length == model.output().length)
      assert(scoreResult.slice(0, inputArray.length).sameElements(inputArray))
      scoreResult(2) match {
        case prediction: Int => assert(prediction == classLabel)
        case _ => throw new RuntimeException(s"Expected prediction to be a Int but is ${scoreResult(1).getClass.getSimpleName}")
      }
    }

    "throw IllegalArgumentException for invalid score parameters" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = LogisticRegressionModel.train(frame, obsColumns, labelColumn, None, 3)

      // Null/empty data arrays
      intercept[IllegalArgumentException] {
        model.score(null)
      }
      intercept[IllegalArgumentException] {
        model.score(Array[Any]())
      }

      // Invalid number of input values
      intercept[IllegalArgumentException] {
        model.score(Array[Any](4.9, 1.4, 3.5))
      }

      // Invalid data types for the scoring input
      intercept[IllegalArgumentException] {
        model.score(Array[Any](4.9, "bogus"))
      }
    }
  }
}
