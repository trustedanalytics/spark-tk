package org.trustedanalytics.sparktk.models.survivalanalysis

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{Column, DataTypes, Frame, FrameSchema}
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class CoxPhTest extends TestingSparkContextWordSpec with Matchers {
  val rows: Array[Row] = Array(new GenericRow(Array[Any](18, 42, 6, 1)),
    new GenericRow(Array[Any](19, 79, 5, 1)),
    new GenericRow(Array[Any](6, 46, 4, 1)),
    new GenericRow(Array[Any](4, 66, 3, 1)),
    new GenericRow(Array[Any](0, 90, 2, 1)),
    new GenericRow(Array[Any](12, 20, 1, 1)),
    new GenericRow(Array[Any](0, 73, 0, 1)))
  val schema = new FrameSchema(List(Column("x1", DataTypes.float64), Column("x2", DataTypes.float64),
    Column("time", DataTypes.float64), Column("censor", DataTypes.float64)))

  "SparktkCoxPhModel train" should {
    "create a SparktkCoxPhModel from training" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = SparktkCoxPhModel.train(frame, "time", List("x1", "x2"), "censor")

      model shouldBe a[SparktkCoxPhModel]
    }

}}
