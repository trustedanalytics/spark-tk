/**
 *  Copyright (c) 2015 Intel Corporation 
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

package org.trustedanalytics.sparktk.models.timeseries.arx

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.ops.timeseries.TimeSeriesFunctions
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ArxModelTest extends TestingSparkContextWordSpec with Matchers {
  val rows: Array[Row] = Array(
    new GenericRow(Array[Any](68, 278, 0, 28, 0.015132758079119)),
    new GenericRow(Array[Any](89, 324, 0, 28, 0.0115112433251418)),
    new GenericRow(Array[Any](96, 318, 0, 28, 0.0190129524583803)),
    new GenericRow(Array[Any](98, 347, 0, 28, 0.0292307976571017)),
    new GenericRow(Array[Any](70, 345, 1, 28, 0.0232811662755677)),
    new GenericRow(Array[Any](88, 335, 1, 29, 0.0306535355961641)),
    new GenericRow(Array[Any](76, 309, 0, 29, 0.0278080597180392)),
    new GenericRow(Array[Any](104, 318, 0, 29, 0.0305241957835221)),
    new GenericRow(Array[Any](64, 308, 0, 29, 0.0247039042146302)),
    new GenericRow(Array[Any](89, 320, 0, 29, 0.0269026810295449)),
    new GenericRow(Array[Any](76, 292, 0, 29, 0.0283254189686074)),
    new GenericRow(Array[Any](66, 295, 1, 29, 0.0230224866502836)),
    new GenericRow(Array[Any](84, 383, 1, 21, 0.0279373995306813)),
    new GenericRow(Array[Any](49, 237, 0, 21, 0.0263853217789767)),
    new GenericRow(Array[Any](47, 210, 0, 21, 0.0230224866502836))
  )

  val schema = FrameSchema(Vector(Column("y", DataTypes.float64),
    Column("visitors", DataTypes.float64),
    Column("wkends", DataTypes.float64),
    Column("incidentRate", DataTypes.float64),
    Column("seasonality", DataTypes.float64)))

  "train" should {
    "throw an exception for a null frame" in {
      intercept[IllegalArgumentException] {
        // Frame is null, so this should fail
        ArxModel.train(null, "ts", List("x1", "x2"), 0, 0)
      }
    }

    "throw an exception for bad ts column name" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      intercept[IllegalArgumentException] {
        // There is no column named "ts" so this should fail
        ArxModel.train(frame, "ts", List("visitors", "wkends", "incidentRate", "seasonality"), 0, 0)
      }
    }

    "throw an exception for bad x column name" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      intercept[IllegalArgumentException] {
        // There is no column named "bogus" so this should fail
        ArxModel.train(frame, "y", List("visitors", "bogus", "incidentRate", "seasonality"), 0, 0)
      }
    }

    "model can be trained with valid arguements" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      // Train
      val trainResult = ArxModel.train(frame, "y", List("visitors", "wkends", "incidentRate", "seasonality"), 0, 0, true)
    }
  }

  "predict" should {

    "throw an exception for a null frame" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = ArxModel.train(frame, "y", List("visitors", "wkends", "incidentRate", "seasonality"), 0, 0, true)

      intercept[IllegalArgumentException] {
        // This should throw an excpetion since frame is null
        trainResult.predict(null, "y", List("visitors", "wkends", "incidentRate", "seasonality"))
      }
    }

    "throw an exception for a bad ts column name" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = ArxModel.train(frame, "y", List("visitors", "wkends", "incidentRate", "seasonality"), 0, 0, true)

      intercept[IllegalArgumentException] {
        // This should throw an excpetion since there is no "ts" column
        trainResult.predict(frame, "ts", List("visitors", "wkends", "incidentRate", "seasonality"))
      }
    }

    "throw an exception for a bad x column name" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = ArxModel.train(frame, "y", List("visitors", "wkends", "incidentRate", "seasonality"), 0, 0, true)

      intercept[IllegalArgumentException] {
        // This should throw an excpetion since there is no "bogus" column
        trainResult.predict(frame, "ts", List("visitors", "bogus", "incidentRate", "seasonality"))
      }
    }

    "throw an exception for a different number of x columns, compared to training" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = ArxModel.train(frame, "y", List("visitors", "wkends", "incidentRate", "seasonality"), 0, 0, true)

      intercept[IllegalArgumentException] {
        // This should throw an excpetion since there is no "bogus" column
        trainResult.predict(frame, "ts", List("visitors", "wkends", "incidentRate"))
      }
    }

    "add a predicted_y column when passed valid parameters" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = ArxModel.train(frame, "y", List("visitors", "wkends", "incidentRate", "seasonality"), 0, 0, true)

      // Predict
      val predictResult = trainResult.predict(frame, "y", List("visitors", "wkends", "incidentRate", "seasonality"))

      // Check for predicted_y column
      assert(predictResult.schema.columnNames.sameElements(Vector("y", "visitors", "wkends", "incidentRate", "seasonality", "predicted_y")))
    }
  }

}
