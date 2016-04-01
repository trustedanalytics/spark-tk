package org.apache.spark.org.trustedanalytics.at.frame

import org.trustedanalytics.at.frame.internal.FrameState
import org.trustedanalytics.at.testutils._
import org.scalatest.Matchers
import org.trustedanalytics.at.frame.{ FrameSchema, Column, DataTypes }
import org.apache.spark.sql.types.{ StringType, IntegerType }

class FrameRddTest extends TestingSparkContextWordSpec with Matchers {

  "FrameRdd" should {

    /**
     * Method that accepts FrameState as a parameter (for testing implicit conversion)
     */
    def frameStateColumnCount(frameState: FrameState): Int = {
      frameState.schema.columns.length
    }

    /**
     * Method that accepts FrameRdd as a parameter (for testing implicit conversion)
     */
    def frameRddColumnCount(frameRdd: FrameRdd): Int = {
      frameRdd.frameSchema.columns.length
    }

    "implicitly convert between FrameState and FrameRdd" in {
      val schema = FrameSchema(Vector(Column("num", DataTypes.int32), Column("name", DataTypes.string)))
      val rows = FrameRdd.toRowRDD(schema, sparkContext.parallelize((1 to 100).map(i => Array(i.toLong, i.toString))).repartition(3))

      val frameRdd = new FrameRdd(schema, rows)
      val frameState = FrameState(rows, schema)

      // Call both methods with FrameState
      assert(frameStateColumnCount(frameState) == 2)
      assert(frameRddColumnCount(frameState) == 2)

      // Call both methods with FrameRdd
      assert(frameRddColumnCount(frameRdd) == 2)
      assert(frameStateColumnCount(frameRdd) == 2)
    }
  }
}