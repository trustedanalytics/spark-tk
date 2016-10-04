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
package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class FilterDropRowsTest extends TestingSparkContextWordSpec with Matchers {

  "Frame" should {

    "filter based on predicate" in {

      //setup test data
      val favoriteMovies = List(
        Row("John", 1, "Titanic"),
        Row("Kathy", 2, "Jurassic Park"),
        Row("Kathy", 2, "Jurassic Park"),
        Row("John", 1, "The kite runner"),
        Row("Kathy", 2, "Toy Story 3"),
        Row("Peter", 3, "Star War"),
        Row("Peter", 3, "Star War"))

      val schema = FrameSchema(Vector(
        Column("name", DataTypes.string),
        Column("id", DataTypes.int32),
        Column("movie", DataTypes.string)))

      val rdd = sparkContext.parallelize(favoriteMovies)
      val frame = new Frame(rdd, schema)

      //filter frame by predicate
      frame.filter(row => row(0) == "Kathy")
      frame.rowCount() shouldBe 3

    }

    "drop rows based on predicate" in {

      //setup test data
      val favoriteMovies = List(
        Row("John", 1, "Titanic"),
        Row("Kathy", 2, "Jurassic Park"),
        Row("Kathy", 2, "Jurassic Park"),
        Row("John", 1, "The kite runner"),
        Row("Kathy", 2, "Toy Story 3"),
        Row("Peter", 3, "Star War"),
        Row("Peter", 3, "Star War"))

      val schema = FrameSchema(Vector(
        Column("name", DataTypes.string),
        Column("id", DataTypes.int32),
        Column("movie", DataTypes.string)))

      val rdd = sparkContext.parallelize(favoriteMovies)
      val frame = new Frame(rdd, schema)

      //drop rows of frame by predicate
      frame.dropRows(row => row(0) == "Kathy")
      frame.rowCount() shouldBe 4

    }
  }
}
