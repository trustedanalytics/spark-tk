package org.apache.spark.org.trustedanalytics.at.frame

import org.trustedanalytics.at.frame.internal.FrameState
import org.trustedanalytics.at.testutils._
import org.scalatest.Matchers
import org.trustedanalytics.at.frame.{ FrameSchema, Column, DataTypes }
import org.apache.spark.sql.types.{ StringType, IntegerType }

class FrameRddTest extends TestingSparkContextWordSpec with Matchers {

  "FrameRdd" should {

    /**
     * Method that accepts FrameState as a parameter (for testing implicit conversion).
     * @return Returns schema column column and rdd row count.
     */
    def frameStateColumnCount(frameState: FrameState): (Int, Long) = {
      (frameState.schema.columns.length, frameState.rdd.count())
    }

    /**
     * Method that accepts FrameRdd as a parameter (for testing implicit conversion)
     * @return Returns schema column column and rdd row count.
     */
    def frameRddColumnCount(frameRdd: FrameRdd): (Int, Long) = {
      (frameRdd.frameSchema.columns.length, frameRdd.count())
    }

    "implicitly convert between FrameState and FrameRdd" in {
      val schema = FrameSchema(Vector(Column("num", DataTypes.int32), Column("name", DataTypes.string)))
      val rows = FrameRdd.toRowRDD(schema, sparkContext.parallelize((1 to 100).map(i => Array(i.toLong, i.toString))).repartition(3))

      val frameRdd = new FrameRdd(schema, rows)
      val frameState = FrameState(rows, schema)

      // Call both methods with FrameState
      assert(frameStateColumnCount(frameState) == (2, 100))
      assert(frameRddColumnCount(frameState) == (2, 100))

      // Call both methods with FrameRdd
      assert(frameRddColumnCount(frameRdd) == (2, 100))
      assert(frameStateColumnCount(frameRdd) == (2, 100))
    }
  }
}