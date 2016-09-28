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

package org.trustedanalytics.sparktk.models.timeseries.max

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, Frame, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class MaxModelTest extends TestingSparkContextWordSpec with Matchers {
  /**
   * Air quality data from:
   *
   * https://archive.ics.uci.edu/ml/datasets/Air+Quality.
   *
   * Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml].
   * Irvine, CA: University of California, School of Information and Computer Science.
   */
  val rows: Array[Row] = Array(
    new GenericRow(Array[Any]("10/03/2004", "18.00.00", 2.6, 1360, 150, 11.9, 1046, 166, 1056, 113, 1692, 1268, 13.6, 48.9, 0.7578)),
    new GenericRow(Array[Any]("10/03/2004", "19.00.00", 2, 1292, 112, 9.4, 955, 103, 1174, 92, 1559, 972, 13.3, 47.7, 0.7255)),
    new GenericRow(Array[Any]("10/03/2004", "20.00.00", 2.2, 1402, 88, 9.0, 939, 131, 1140, 114, 1555, 1074, 11.9, 54.0, 0.7502)),
    new GenericRow(Array[Any]("10/03/2004", "21.00.00", 2.2, 1376, 80, 9.2, 948, 172, 1092, 122, 1584, 1203, 11.0, 60.0, 0.7867)),
    new GenericRow(Array[Any]("10/03/2004", "22.00.00", 1.6, 1272, 51, 6.5, 836, 131, 1205, 116, 1490, 1110, 11.2, 59.6, 0.7888)),
    new GenericRow(Array[Any]("10/03/2004", "23.00.00", 1.2, 1197, 38, 4.7, 750, 89, 1337, 96, 1393, 949, 11.2, 59.2, 0.7848)),
    new GenericRow(Array[Any]("11/03/2004", "00.00.00", 1.2, 1185, 31, 3.6, 690, 62, 1462, 77, 1333, 733, 11.3, 56.8, 0.7603)),
    new GenericRow(Array[Any]("11/03/2004", "01.00.00", 1, 1136, 31, 3.3, 672, 62, 1453, 76, 1333, 730, 10.7, 60.0, 0.7702)),
    new GenericRow(Array[Any]("11/03/2004", "02.00.00", 0.9, 1094, 24, 2.3, 609, 45, 1579, 60, 1276, 620, 10.7, 59.7, 0.7648)),
    new GenericRow(Array[Any]("11/03/2004", "03.00.00", 0.6, 1010, 19, 1.7, 561, -200, 1705, -200, 1235, 501, 10.3, 60.2, 0.7517)),
    new GenericRow(Array[Any]("11/03/2004", "04.00.00", -200, 1011, 14, 1.3, 527, 21, 1818, 34, 1197, 445, 10.1, 60.5, 0.7465)),
    new GenericRow(Array[Any]("11/03/2004", "05.00.00", 0.7, 1066, 8, 1.1, 512, 16, 1918, 28, 1182, 422, 11.0, 56.2, 0.7366)),
    new GenericRow(Array[Any]("11/03/2004", "06.00.00", 0.7, 1052, 16, 1.6, 553, 34, 1738, 48, 1221, 472, 10.5, 58.1, 0.7353)),
    new GenericRow(Array[Any]("11/03/2004", "07.00.00", 1.1, 1144, 29, 3.2, 667, 98, 1490, 82, 1339, 730, 10.2, 59.6, 0.7417)),
    new GenericRow(Array[Any]("11/03/2004", "08.00.00", 2, 1333, 64, 8.0, 900, 174, 1136, 112, 1517, 1102, 10.8, 57.4, 0.7408)),
    new GenericRow(Array[Any]("11/03/2004", "09.00.00", 2.2, 1351, 87, 9.5, 960, 129, 1079, 101, 1583, 1028, 10.5, 60.6, 0.7691)),
    new GenericRow(Array[Any]("11/03/2004", "10.00.00", 1.7, 1233, 77, 6.3, 827, 112, 1218, 98, 1446, 860, 10.8, 58.4, 0.7552)),
    new GenericRow(Array[Any]("11/03/2004", "11.00.00", 1.5, 1179, 43, 5.0, 762, 95, 1328, 92, 1362, 671, 10.5, 57.9, 0.7352)),
    new GenericRow(Array[Any]("11/03/2004", "12.00.00", 1.6, 1236, 61, 5.2, 774, 104, 1301, 95, 1401, 664, 9.5, 66.8, 0.7951)),
    new GenericRow(Array[Any]("11/03/2004", "13.00.00", 1.9, 1286, 63, 7.3, 869, 146, 1162, 112, 1537, 799, 8.3, 76.4, 0.8393)),
    new GenericRow(Array[Any]("11/03/2004", "14.00.00", 2.9, 1371, 164, 11.5, 1034, 207, 983, 128, 1730, 1037, 8.0, 81.1, 0.8736)),
    new GenericRow(Array[Any]("11/03/2004", "15.00.00", 2.2, 1310, 79, 8.8, 933, 184, 1082, 126, 1647, 946, 8.3, 79.8, 0.8778)),
    new GenericRow(Array[Any]("11/03/2004", "16.00.00", 2.2, 1292, 95, 8.3, 912, 193, 1103, 131, 1591, 957, 9.7, 71.2, 0.8569)),
    new GenericRow(Array[Any]("11/03/2004", "17.00.00", 2.9, 1383, 150, 11.2, 1020, 243, 1008, 135, 1719, 1104, 9.8, 67.6, 0.8185)))

  val schema = FrameSchema(Vector(Column("Date", DataTypes.str),
    Column("Time", DataTypes.str),
    Column("CO_GT", DataTypes.float64),
    Column("PT08_S1_CO", DataTypes.int32),
    Column("NMHC_GT", DataTypes.int32),
    Column("C6H6_GT", DataTypes.float32),
    Column("PT08_S2_NMHC", DataTypes.int32),
    Column("NOx_GT", DataTypes.int32),
    Column("PT08_S3_NOx", DataTypes.int32),
    Column("NO2_GT", DataTypes.int32),
    Column("PT08_S4_NO2", DataTypes.int32),
    Column("PT08_S5_O3_", DataTypes.int32),
    Column("T", DataTypes.float32),
    Column("RH", DataTypes.float32),
    Column("AH", DataTypes.float32)))

  val y_column = "T"
  val x_columns = List("CO_GT", "PT08_S1_CO", "NMHC_GT", "C6H6_GT", "PT08_S2_NMHC", "NOx_GT", "PT08_S3_NOx", "NO2_GT", "PT08_S4_NO2", "PT08_S5_O3_")

  "train" should {
    "throw an exception for a null frame" in {
      intercept[IllegalArgumentException] {
        // Frame is null, so this should fail
        MaxModel.train(null, y_column, x_columns, 1, 0)
      }
    }

    "throw an exception for bad ts column name" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      intercept[IllegalArgumentException] {
        // There is no column named "ts" so this should fail
        MaxModel.train(frame, "ts", x_columns, 1, 0)
      }
    }

    "throw an exception for bad x column name" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      intercept[IllegalArgumentException] {
        // There is no column named "bogus" so this should fail
        MaxModel.train(frame, y_column, List("CO_GT", "bogus", "NMHC_GT", "C6H6_GT"), 1, 0)
      }
    }

    "model can be trained with valid arguements" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      // Train
      val trainResult = MaxModel.train(frame, y_column, x_columns, 1, 0)
    }
  }
  "predict" should {

    "throw an exception for a null frame" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = MaxModel.train(frame, y_column, x_columns, 1, 0)

      intercept[IllegalArgumentException] {
        // This should throw an excpetion since frame is null
        trainResult.predict(null, y_column, x_columns)
      }
    }

    "throw an exception for a bad ts column name" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = MaxModel.train(frame, y_column, x_columns, 1, 0)

      intercept[IllegalArgumentException] {
        // This should throw an excpetion since there is no "ts" column
        trainResult.predict(frame, "ts", x_columns)
      }
    }

    "throw an exception for a bad x column name" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = MaxModel.train(frame, y_column, x_columns, 1, 0)

      intercept[IllegalArgumentException] {
        // This should throw an excpetion since there is no "bogus" column
        trainResult.predict(frame, y_column, List("CO_GT", "bogus", "NMHC_GT", "C6H6_GT"))
      }
    }

    "throw an exception for a different number of x columns, compared to training" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = MaxModel.train(frame, y_column, x_columns, 1, 0)

      intercept[IllegalArgumentException] {
        // This should throw an excpetion since there is no "bogus" column
        trainResult.predict(frame, y_column, List("CO_GT", "NMHC_GT", "C6H6_GT"))
      }
    }

    "add a predicted_y column when passed valid parameters" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val trainResult = MaxModel.train(frame, y_column, x_columns, 1, 0)

      // Predict
      val predictResult = trainResult.predict(frame, y_column, x_columns)

      // Check for predicted_y column
      assert(predictResult.schema.columnNames.length == (frame.schema.columnNames.length + 1))
      assert(predictResult.schema.hasColumn("predicted_y"))
    }
  }

  "score" should {

    val trainData: Array[Row] = Array(new GenericRow(Array[Any](2.6, 11.9, 1046.0, 13.6)),
      new GenericRow(Array[Any](2.0, 9.4, 955.0, 13.3)),
      new GenericRow(Array[Any](2.2, 9.0, 939.0, 11.9)),
      new GenericRow(Array[Any](2.2, 9.2, 948.0, 11.0)),
      new GenericRow(Array[Any](1.6, 6.5, 836.0, 11.2)),
      new GenericRow(Array[Any](1.2, 4.7, 750.0, 11.2)),
      new GenericRow(Array[Any](1.2, 3.6, 690.0, 11.3)),
      new GenericRow(Array[Any](1.0, 3.3, 672.0, 10.7)),
      new GenericRow(Array[Any](2.9, 2.3, 609.0, 10.7)),
      new GenericRow(Array[Any](2.6, 1.7, 561.0, 10.3)),
      new GenericRow(Array[Any](2.0, 1.3, 527.0, 10.1)),
      new GenericRow(Array[Any](2.7, 1.1, 512.0, 11.0)),
      new GenericRow(Array[Any](2.7, 1.6, 553.0, 10.5)))
    val trainSchema = FrameSchema(Vector(Column("CO_GT", DataTypes.float64),
      Column("C6H6_GT", DataTypes.float32),
      Column("PT08_S2_NMHC", DataTypes.int32),
      Column("T", DataTypes.float32)))

    "return predictions when calling the MAX model score" in {
      val rdd = sparkContext.parallelize(trainData)
      val frame = new Frame(rdd, trainSchema)
      val model = MaxModel.train(frame, "CO_GT", List("C6H6_GT", "PT08_S2_NMHC", "T"), 2, 1)

      val y = Array(2.6, 2.0)
      val x = Array(11.9, 9.4, 1046.0, 955.0, 13.6, 13.3)
      val inputArray = Array[Any](y, x)
      assert(model.input().length == inputArray.length)
      val scoreResult = model.score(inputArray)
      assert(scoreResult.length == model.output().length)
      assert(scoreResult(0) == y)
      assert(scoreResult(1) == x)
      scoreResult(2) match {
        case result: Array[Double] => {
          result.foreach(r => println(r.toString))
          assert(result.length == y.length)
          (y, result).zipped.map { (actual, prediction) => assertAlmostEqual(actual, prediction, 0.5) }
        }
        case _ => throw new RuntimeException("Expected Array[Double] from MAX scoring")
      }
    }

    "throw an IllegalArgumentException for invalid score parameters" in {
      val rdd = sparkContext.parallelize(trainData)
      val frame = new Frame(rdd, trainSchema)
      val model = MaxModel.train(frame, "CO_GT", List("C6H6_GT", "PT08_S2_NMHC", "T"), 2, 1)

      // Null or empty input data
      intercept[IllegalArgumentException] {
        model.score(null)
      }
      intercept[IllegalArgumentException] {
        model.score(Array[Any]())
      }

      // Wrong number of values in the input array
      intercept[IllegalArgumentException] {
        model.score(Array[Any](13.6, 2.6, 2.7, 1.6, 553.0, 10.5))
      }

      // Wrong data type for the y values (Array[str])
      intercept[IllegalArgumentException] {
        model.score(Array[Any](Array("a"), Array(11.9, 9.4, 1046.0, 955.0, 13.6, 13.3)))
      }

      // Wrong data type for the y values (double)
      intercept[IllegalArgumentException] {
        model.score(Array[Any](2.6, Array(11.9, 9.4, 1046.0, 955.0, 13.6, 13.3)))
      }

      // Less x columns than we trained with
      intercept[IllegalArgumentException] {
        model.score(Array[Any](Array(2.6, 2.0), Array(11.9, 9.4, 1046.0, 955.0)))
      }

      // Wrong data type for x columns
      intercept[IllegalArgumentException] {
        model.score(Array[Any](Array(2.6, 2.0), Array(11.9, 9.4, "bogus", 955.0, 13.6, 13.3)))
      }
    }
  }
}
