package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{Frame, DataTypes, Column, FrameSchema}
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
