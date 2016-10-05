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
package org.trustedanalytics.sparktk.models.timeseries.arima

import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ArimaModelTest extends TestingSparkContextWordSpec with Matchers {
  // Time series values for testing
  val ts = List(12.88969427, 13.54964408, 13.8432745, 12.13843611, 12.81156092, 14.2499628, 15.12102595)

  "train" should {
    "throw an exception for a null or empty ts vector" in {
      intercept[IllegalArgumentException] {
        // null ts list should throw an exception
        ArimaModel.train(null, 0, 0, 0)
      }

      intercept[IllegalArgumentException] {
        // empty ts list should throw an exception
        ArimaModel.train(List[Double](), 0, 0, 0)
      }
    }

    "throw an exception for an invalid method" in {
      intercept[IllegalArgumentException] {
        // null method string should throw an exception
        ArimaModel.train(ts, 1, 0, 1, true, null)
      }
      intercept[IllegalArgumentException] {
        // empty string method should throw an exception
        ArimaModel.train(ts, 1, 0, 1, true, "")
      }
      intercept[IllegalArgumentException] {
        // "bogus" string shoudl throw an exception
        ArimaModel.train(ts, 1, 0, 1, true, "bogus")
      }
    }

    "train model when passed valid parameters" in {
      val trainResult = ArimaModel.train(ts, 1, 0, 1)
      // Check coefficients
      assert(trainResult.arimaModel.coefficients.sameElements(Array(9.864444620964322, 0.2848511106449633, 0.47346114378593795)))
    }
  }

  "predict" should {
    "throw an exception for a negative value for futurePeriods" in {
      val trainResult = ArimaModel.train(ts, 1, 0, 1)

      intercept[IllegalArgumentException] {
        trainResult.predict(-1)
      }
    }

    "predict the same amount of values as the ts when futurePeriods is zero" in {
      val trainResult = ArimaModel.train(ts, 1, 0, 1)

      val predictResult = trainResult.predict(0)
      assert(predictResult.length == ts.length)
      assert(predictResult.sameElements(Seq(12.674342627141744,
        13.638048984791693,
        13.682219498657313,
        13.883970022400577,
        12.49564914570843,
        13.66340392811346,
        14.201275185574925)))
    }

    "predict more values when the futurePeriods is greater than zero" in {
      val trainResult = ArimaModel.train(ts, 1, 0, 1)

      val predictResult = trainResult.predict(5)
      assert(predictResult.length == (ts.length + 5)) // we should get back 5 extra values
      assert(predictResult.sameElements(Seq(12.674342627141744,
        13.638048984791693,
        13.682219498657313,
        13.883970022400577,
        12.49564914570843,
        13.66340392811346,
        14.201275185574925,
        14.345159879072785,
        13.950679344897772,
        13.838311126610202,
        13.806302914829793,
        13.797185340154384)))
    }
  }

}
