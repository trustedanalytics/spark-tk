package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ DataTypes, Column, FrameSchema, Frame }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class BoxCoxTest extends TestingSparkContextWordSpec with Matchers {

  val boxCoxRows: Array[Row] = Array(new GenericRow(Array[Any](7.71320643267, 1.2)),
    new GenericRow(Array[Any](0.207519493594, 2.3)),
    new GenericRow(Array[Any](6.33648234926, 3.4)),
    new GenericRow(Array[Any](7.48803882539, 4.5)),
    new GenericRow(Array[Any](4.98507012303, 5.6)))

  val boxCoxSchema = FrameSchema(List(Column("A", DataTypes.float64), Column("B", DataTypes.float64)))

  "boxCox" should {
    "compute the box-cox transformation for the given column" in {

      val rdd = sparkContext.parallelize(boxCoxRows)
      val frameRdd = new FrameRdd(boxCoxSchema, rdd)

      val results = BoxCox.boxCox(frameRdd, "A", 0.3)
      val boxCox = results.map(row => row(2).asInstanceOf[Double]).collect()

      assert(boxCox == Array(2.81913279907, -1.25365381375, 2.46673638752, 2.76469126003, 2.06401101556))
    }
  }

  "boxCox" should {
    "create a new column in the frame storing the box-cox computation with lambda 0.3" in {

      val rdd = sparkContext.parallelize(boxCoxRows)
      val frameRdd = new FrameRdd(boxCoxSchema, rdd)

      val result = BoxCox.boxCox(frameRdd, "A", 0.3).collect()

      assert(result.length == 5)
      assert(result.apply(0) == Row(7.71320643267, 1.2, 2.8191327990706947))
      assert(result.apply(1) == Row(0.207519493594, 2.3, -1.253653813751204))
      assert(result.apply(2) == Row(6.33648234926, 3.4, 2.4667363875200685))
      assert(result.apply(3) == Row(7.48803882539, 4.5, 2.7646912600285254))
      assert(result.apply(4) == Row(4.98507012303, 5.6, 2.064011015565803))
    }
  }

  "computeBoxCox" should {
    "compute BoxCox transformation with lambda 0.0" in {

      val input = 4.98507012303
      val lambda = 0.0
      val boxCoxTransform = BoxCox.computeBoxCoxTransformation(input, lambda)

      assert(boxCoxTransform == 1.60644747012)
    }
  }
}