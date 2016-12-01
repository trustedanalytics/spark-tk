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
package org.apache.spark.ml.regression.org.trustedanalytics.sparktk

import breeze.linalg.{ *, DenseVector => BDV }
import breeze.linalg.{ *, DenseMatrix => BDM }
import java.nio.file.{ Files, Path }
import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.linalg.{ DenseVector, VectorUDT }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{ DoubleType, StructField, StructType }
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.MatcherUtils._
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class CoxPhTest extends TestingSparkContextWordSpec with Matchers {
  val tolerance: Double = 1E-6

  val sortedCoxPointArray = Array(new CoxPhPoint(new DenseVector(Array(18d, 42d)), 6d, 1d),
    new CoxPhPoint(new DenseVector(Array(19d, 79d)), 5d, 1d),
    new CoxPhPoint(new DenseVector(Array(6d, 46d)), 4d, 1d),
    new CoxPhPoint(new DenseVector(Array(4d, 66d)), 3d, 1d),
    new CoxPhPoint(new DenseVector(Array(0d, 90d)), 2d, 1d),
    new CoxPhPoint(new DenseVector(Array(12d, 20d)), 1d, 1d),
    new CoxPhPoint(new DenseVector(Array(0d, 73d)), 0d, 1d))

  "extractCoxPointsWithMetaData in CoxCostFun with 0 beta" should {
    "compute correct Rdd" in {
      val coxRdd = sparkContext.parallelize(sortedCoxPointArray, 3)

      val currentBeta = BDV(0d, 0d)
      val coxCostFun = new CoxPhCostFun(coxRdd)
      val coxWithMetaData = coxCostFun.extractCoxPhPointsWithMetaData(coxRdd, currentBeta)
      val coxWithMetaDataArray = coxWithMetaData.collect()

      coxPhPointsEqualWithTolerance(coxWithMetaDataArray)
    }
  }

  "computeGradientVector" should {
    "compute correct Gradient vector" in {
      val data = new CoxPhPointWithMetaData(new DenseVector(Array(4d, 66d)), 3d, 1d, 4d,
        BDV(4d, 66d), 1d, BDV(47d, 233d), new BDM(2, 2, Array(737d, 2797d, 2797d, 14477d)))
      val estimatedGradientVector = BDV(-7.75, 7.75)

      val coxAgg = new CoxPhAggregator(BDV(0.0, 0.0))
      val computedGradientVector = coxAgg.computeGradientVector(data)

      computedGradientVector.toArray should equalWithTolerance(estimatedGradientVector.toArray)
    }
  }

  "computeInformationMatrix" should {
    "compute Information Matrix" in {
      val data = new CoxPhPointWithMetaData(new DenseVector(Array(0d, 73d)), 0d, 1d, 7d, BDV(0d, 73d), 1d, BDV(59d, 416d), new BDM(2, 2, Array(881d, 3037d, 3037d, 28306d)))
      val estimatedInformationMatrix = BDM(-54.816326, 67.040816, 67.040816, -511.959183)

      val coxAgg = new CoxPhAggregator(BDV(0.0, 0.0))
      val computedInformationMatrix = coxAgg.computeInformationMatrix(data)

      computedInformationMatrix.toArray should equalWithTolerance(estimatedInformationMatrix.toArray)
    }
  }

  "calculate in CoxCostFun" should {
    "compute loss and gradient" in {
      val coxRdd = sparkContext.parallelize(sortedCoxPointArray, 4)

      val currentBeta = BDV(0d, 0d)
      val estimatedLoss = -8.52516136
      val estimatedGradient = BDV(-31.24523, 18.38809)
      val estimatedInformationMatrix = BDM(-245.321604, 100.3460941, 100.3460941, -2258.997794)

      val coxCostFun = new CoxPhCostFun(coxRdd)
      val (loss, gradient, informationMatrix) = coxCostFun.calculate(currentBeta)

      loss shouldBe estimatedLoss +- tolerance
      gradient.toArray should equalWithTolerance(estimatedGradient.toArray)
      informationMatrix.toArray should equalWithTolerance(estimatedInformationMatrix.toArray)
    }
  }

  "coxPh fit" should {
    "train on multivariate data" in {
      val data = Array((new DenseVector(Array(18d, 42d)), 6d, 1d),
        (new DenseVector(Array(19d, 79d)), 5d, 1d),
        (new DenseVector(Array(6d, 46d)), 4d, 1d),
        (new DenseVector(Array(4d, 66d)), 3d, 1d),
        (new DenseVector(Array(0d, 90d)), 2d, 1d),
        (new DenseVector(Array(12d, 20d)), 1d, 1d),
        (new DenseVector(Array(0d, 73d)), 0d, 1d))

      val univariateDF = generateCoxPhInput(data)
      val cox = new CoxPh()
      cox.setLabelCol("time")
      cox.setFeaturesCol("features")
      cox.setCensorCol("censor")
      val coxModel: CoxPhModel = cox.fit(univariateDF)

      coxModel.beta.toArray should equalWithTolerance(Array(-0.19214283727219947, -0.007012237038116714))
      coxModel.meanVector.toArray should equalWithTolerance(Array(8.428571428571429, 59.42857142857142))
    }
  }

  "coxPh fit" should {
    "train on univariate data" in {
      val data = Array((new DenseVector(Array(18d)), 6d, 1d),
        (new DenseVector(Array(19d)), 5d, 1d),
        (new DenseVector(Array(6d)), 4d, 1d),
        (new DenseVector(Array(4d)), 3d, 1d),
        (new DenseVector(Array(0d)), 2d, 1d),
        (new DenseVector(Array(12d)), 1d, 1d),
        (new DenseVector(Array(0d)), 0d, 1d))

      val univariateDF = generateCoxPhInput(data)
      val cox = new CoxPh()
      cox.setLabelCol("time")
      cox.setFeaturesCol("features")
      cox.setCensorCol("censor")
      val coxModel: CoxPhModel = cox.fit(univariateDF)

      coxModel.beta(0) shouldBe -0.1787289380664636 +- tolerance
      coxModel.meanVector(0) shouldBe 8.428571428571429 +- tolerance
    }
  }

  "coxPh save/load" should {
    "return CoxPhModel upon loading a saved model" in {
      val data = Array((new DenseVector(Array(18d)), 6d, 1d),
        (new DenseVector(Array(19d)), 5d, 1d),
        (new DenseVector(Array(6d)), 4d, 1d),
        (new DenseVector(Array(4d)), 3d, 1d),
        (new DenseVector(Array(0d)), 2d, 1d),
        (new DenseVector(Array(12d)), 1d, 1d),
        (new DenseVector(Array(0d)), 0d, 1d))

      val univariateDF = generateCoxPhInput(data)
      val cox = new CoxPh()
      cox.setLabelCol("time")
      cox.setFeaturesCol("features")
      cox.setCensorCol("censor")
      val coxModel: CoxPhModel = cox.fit(univariateDF)

      val modelpath = "sandbox/coxph_save_test"
      coxModel.save(modelpath)

      //load the saved model and verify the beta and mean
      val restored_coxPhModel = CoxPhModel.load(modelpath)
      restored_coxPhModel.beta(0) shouldBe coxModel.beta(0)
      restored_coxPhModel.meanVector(0) shouldBe coxModel.meanVector(0)

      FileUtils.deleteQuietly(new java.io.File(modelpath))
    }
  }

  /**
   * Compares two array of type Array[CoxPhPointWithMetaData]
   * @param actualArray: (Array[CoxPhPointWithMetaData]) Actual ouput of CostFun
   * @param tolerance: (Double) Precision
   */
  def coxPhPointsEqualWithTolerance(actualArray: Array[CoxPhPointWithMetaData], tolerance: Double = 1E-6) = {
    val estimatedCoxMetaDataArray = Array(
      new CoxPhPointWithMetaData(new DenseVector(Array(18d, 42d)), 6d, 1d, 1d, BDV(18d, 42d), 1d, BDV(18d, 42d), new BDM(2, 2, Array(324.0, 756.0, 756.0, 1764.0))),
      new CoxPhPointWithMetaData(new DenseVector(Array(19d, 79d)), 5d, 1d, 2d, BDV(19d, 79d), 1d, BDV(37d, 121d), new BDM(2, 2, Array(685d, 2257d, 2257d, 8005d))),
      new CoxPhPointWithMetaData(new DenseVector(Array(6d, 46d)), 4d, 1d, 3d, BDV(6d, 46d), 1d, BDV(43d, 167d), new BDM(2, 2, Array(721d, 2533d, 2533d, 10121d))),
      new CoxPhPointWithMetaData(new DenseVector(Array(4d, 66d)), 3d, 1d, 4d, BDV(4d, 66d), 1d, BDV(47d, 233d), new BDM(2, 2, Array(737d, 2797d, 2797d, 14477d))),
      new CoxPhPointWithMetaData(new DenseVector(Array(0d, 90d)), 2d, 1d, 5d, BDV(0d, 90d), 1d, BDV(47d, 323d), new BDM(2, 2, Array(737d, 2797d, 2797d, 22577d))),
      new CoxPhPointWithMetaData(new DenseVector(Array(12d, 20d)), 1d, 1d, 6d, BDV(12d, 20d), 1d, BDV(59d, 343d), new BDM(2, 2, Array(881d, 3037d, 3037d, 22977d))),
      new CoxPhPointWithMetaData(new DenseVector(Array(0d, 73d)), 0d, 1d, 7d, BDV(0d, 73d), 1d, BDV(59d, 416d), new BDM(2, 2, Array(881d, 3037d, 3037d, 28306d))))

    for (i <- 0 to actualArray.length - 1) {
      actualArray(i).time shouldBe estimatedCoxMetaDataArray(i).time +- tolerance
      actualArray(i).censor shouldBe estimatedCoxMetaDataArray(i).censor +- tolerance
      actualArray(i).eBetaX shouldBe estimatedCoxMetaDataArray(i).eBetaX +- tolerance
      actualArray(i).features.toArray should equalWithTolerance(estimatedCoxMetaDataArray(i).features.toArray)
      actualArray(i).xDotEBetaX.toArray should equalWithTolerance(estimatedCoxMetaDataArray(i).xDotEBetaX.toArray)
      actualArray(i).sumXDotEBetaX.toArray should equalWithTolerance(estimatedCoxMetaDataArray(i).sumXDotEBetaX.toArray)
      actualArray(i).sumXiXjEBetaX should equalWithToleranceMatrix(estimatedCoxMetaDataArray(i).sumXiXjEBetaX)
    }
  }

  /**
   * Transforms input DataFrame for CoxPhModel.fit()
   * @param data: Array[(DenseVector, Double, Double)] Input data
   * @return DataFrame
   */
  def generateCoxPhInput(data: Array[(DenseVector, Double, Double)]): DataFrame = {
    val rdd = sparkContext.parallelize(data, 4)
    val rowRdd: RDD[Row] = rdd.map(entry => new GenericRow(Array[Any](entry._1, entry._2, entry._3)))
    val schema = StructType(Seq(StructField("features", new VectorUDT, true), StructField("time", DoubleType, true), StructField("censor", DoubleType, true)))
    new SQLContext(this.sparkContext).createDataFrame(rowRdd, schema)
  }

}