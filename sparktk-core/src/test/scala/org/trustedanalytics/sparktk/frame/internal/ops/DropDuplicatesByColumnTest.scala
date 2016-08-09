package org.trustedanalytics.sparktk.frame.internal.ops

import org.scalatest.Matchers
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class DropDuplicatesByColumnITest extends TestingSparkContextWordSpec with Matchers {

  "dropDuplicatesByColumn" should {

    "keep only unique rows for subset of columns" in {

      //setup test data
      val favoriteMovies = List(
        Row("John", 1, "Titanic"),
        Row("Kathy", 2, "Jurassic Park"),
        Row("John", 1, "The kite runner"),
        Row("Kathy", 2, "Toy Story 3"),
        Row("Peter", 3, "Star War"))

      val schema = FrameSchema(Vector(
        Column("name", DataTypes.string),
        Column("id", DataTypes.int32),
        Column("movie", DataTypes.string)))

      val rdd = sparkContext.parallelize(favoriteMovies)
      val frameRdd = new FrameRdd(schema, rdd)

      frameRdd.count() shouldBe 5

      //remove duplicates identified by column names
      val duplicatesRemoved = frameRdd.dropDuplicatesByColumn(Vector("name", "id")).collect()

      val expectedResults = Array(
        Row("John", 1, "Titanic"),
        Row("Kathy", 2, "Jurassic Park"),
        Row("Peter", 3, "Star War")
      )

      duplicatesRemoved should contain theSameElementsAs (expectedResults)
    }

    "keep only unique rows" in {

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
      val frameRdd = new FrameRdd(schema, rdd)

      frameRdd.count() shouldBe 7

      //remove duplicates identified by column names
      val duplicatesRemoved = frameRdd.dropDuplicatesByColumn(Vector("name", "id", "movie")).collect()

      val expectedResults = Array(
        Row("John", 1, "Titanic"),
        Row("John", 1, "The kite runner"),
        Row("Kathy", 2, "Jurassic Park"),
        Row("Kathy", 2, "Toy Story 3"),
        Row("Peter", 3, "Star War")
      )

      duplicatesRemoved should contain theSameElementsAs (expectedResults)
    }

    "throw an IllegalArgumentException for invalid column names" in {
      intercept[IllegalArgumentException] {
        //setup test data
        val favoriteMovies = List(
          Row("John", 1, "Titanic"),
          Row("Kathy", 2, "Jurassic Park"),
          Row("John", 1, "The kite runner"),
          Row("Kathy", 2, "Toy Story 3"),
          Row("Peter", 3, "Star War"))

        val schema = FrameSchema(Vector(
          Column("name", DataTypes.string),
          Column("id", DataTypes.int32),
          Column("movie", DataTypes.string)))

        val rdd = sparkContext.parallelize(favoriteMovies)
        val frameRdd = new FrameRdd(schema, rdd)
        frameRdd.dropDuplicatesByColumn(Vector("name", "invalidCol1", "invalidCol2")).collect()
      }

    }
  }
}
