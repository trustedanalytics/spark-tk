package org.trustedanalytics.sparktk.models.clustering.kmeans

import org.apache.spark.mllib.MllibAliases
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

import org.apache.spark.sql._

class KMeansModelTest extends TestingSparkContextWordSpec with Matchers {

  "KMeansModel getDenseVectorMaker" should {

    // todo: this test should go somewhere more generic, as should the code it's testing
    // (this is more POC for unit testing KMeans)

    val schema = FrameSchema(Vector(Column("word", DataTypes.string),
      Column("d1", DataTypes.float64),
      Column("d2", DataTypes.float64)))
    val rowWrapper = new RowWrapper(schema)
    val row = Row.fromSeq(Vector[Any]("jump", 3.14, 2.72))
    rowWrapper.apply(row)

    "be able to make dense vectors" in {

      val vectorMaker = KMeansModel.getDenseVectorMaker(List("d1", "d2"), None)
      val mllibVector = vectorMaker.apply(rowWrapper)

      mllibVector match {
        case v: MllibAliases.MllibVector => assert(v == MllibAliases.MllibVectors.dense(3.14, 2.72))
        case imposter => fail(s"$imposter is not expected type MllibVector")
      }
    }

    "be able to make dense vectors with weights" in {

      val vectorMaker = KMeansModel.getDenseVectorMaker(List("d1", "d2"), Some(List(2.0, 1.0)))
      val mllibVector = vectorMaker.apply(rowWrapper)

      mllibVector match {
        case v: MllibAliases.MllibVector => assert(v == MllibAliases.MllibVectors.dense(6.28, 2.72))
        case imposter => fail(s"$imposter is not expected type MllibVector")
      }
    }
  }

}