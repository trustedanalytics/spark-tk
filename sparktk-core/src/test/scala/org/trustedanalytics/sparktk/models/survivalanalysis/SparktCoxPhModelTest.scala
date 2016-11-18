/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.sparktk.models.survivalanalysis

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, Frame, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class SparktCoxPhModelTest extends TestingSparkContextWordSpec with Matchers {
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
    "throw an IllegalArgumentException for empty covariatesColumn during train" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      intercept[IllegalArgumentException] {
        SparktkCoxPhModel.train(frame, "time", List(), "censor")
      }
    }

    "throw an IllegalArgumentException for empty time during train" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      intercept[IllegalArgumentException] {
        SparktkCoxPhModel.train(frame, "", List("x1", "x2"), "censor")
      }
    }

    "throw an IllegalArgumentException for empty censor during train" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      intercept[IllegalArgumentException] {
        SparktkCoxPhModel.train(frame, "time", List("x1", "x2"), "")
      }
    }

  }

  "SparktkCoxPhModel predict" should {
    "return a predict frame when comparison frame provided" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = SparktkCoxPhModel.train(frame, "time", List("x1", "x2"), "censor")
      val pred_out = model.predict(frame, Some(List("x1", "x2")), Some(frame))
      pred_out shouldBe a[Frame]
    }

    "return a predict frame when comparison frame not provided" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = SparktkCoxPhModel.train(frame, "time", List("x1", "x2"), "censor")
      val predicted_frame = model.predict(frame, None, None)
      predicted_frame shouldBe a[Frame]
      val resultArray = predicted_frame.rdd.map(row => {
        for (i <- 0 until row.length) yield row.getDouble(i)
      }).collect()

      println(resultArray.length)
    }

  }

  "SparktkCoxPhModel score" should {
    "return predictions when calling the coxPh model score" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val model = SparktkCoxPhModel.train(frame, "time", List("x1", "x2"), "censor")

      // Test values, just grabbed from the first row the of the training frame
      val x1 = 18
      val x2 = 42
      val hazard_ratio = 0.179627832028

      val inputArray = Array[Any](x1, x2)
      assert(model.input().length == inputArray.length)
      val scoreResult = model.score(inputArray)
      assert(scoreResult.length == model.output().length)
      assert(scoreResult(0) == x1)
      assert(scoreResult(1) == x2)

      scoreResult(2) match {
        case prediction: Double => assertAlmostEqual(prediction, hazard_ratio, 0.001)
        case _ => throw new RuntimeException(s"Expected prediction to be a Double but is ${scoreResult(2).getClass.getSimpleName}")
      }
    }
  }
}
