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
package org.trustedanalytics.sparktk.frame.internal

import org.joda.time.DateTime
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.DataTypes
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class DataTypesTest extends TestingSparkContextWordSpec with Matchers {
  "toDateTime" should {
    "parse datetime from string" in {
      val dateTimeString = "2016-06-15T11:22:25.218Z"
      val ms = DataTypes.toDateTime(dateTimeString).getMillis
      assert(DataTypes.datetime.asString(ms) == dateTimeString)

      val dateString = "2016-06-15"
      val dateMs = DataTypes.toDateTime(dateString).getMillis
      // Just check that the string starts with the date, since no time was provided
      assert(DataTypes.datetime.asString(dateMs).startsWith(dateString))

      intercept[RuntimeException] {
        // Bad string should fail
        DataTypes.toDateTime("hello world")
      }

      val bday = DataTypes.toDateTime("1950-05-12T03:25:21.123000Z")
      val bday_ms = bday.getMillis
      val value = 5
    }

    "parse datetime from long" in {
      val currentDateTime = DateTime.now()
      val dateTimeMs = currentDateTime.getMillis
      assert(DataTypes.toDateTime(dateTimeMs).getMillis == dateTimeMs)
    }

    "parse datetime from invalid types should fail" in {
      intercept[RuntimeException] {
        DataTypes.toDateTime(2.5)
      }
      intercept[RuntimeException] {
        DataTypes.toDateTime(List(1, 2, 3))
      }
      assert(DataTypes.datetime.parse(2.5).isFailure)
      assert(DataTypes.datetime.parse(List(1, 2, 3)).isFailure)
    }
  }
}