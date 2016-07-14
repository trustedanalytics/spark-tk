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

    "thow an IllegalArgumentException for empty observationColumns" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)

        val model = RandomForestClassifierModel.train(frame, "label", List(), 2, 1, "gini", 4, 100, 10, None, None)
      }
    }

    "thow an IllegalArgumentException for empty labelColumn" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)

        val model = RandomForestClassifierModel.train(frame, "", List("obs1", "obs2"), 2, 1, "gini", 4, 100, 10, None, None)
      }
    }
  }

}
