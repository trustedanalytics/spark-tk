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
package org.trustedanalytics.sparktk.frame.internal.ops.flatten

import org.apache.spark.sql.Row
import org.scalatest.{ WordSpec, Matchers }
import org.trustedanalytics.sparktk.frame.DataTypes

class FlattenColumnArgsTest extends WordSpec with Matchers {
  "flatten column" should {
    "create multiple rows by splitting a column" in {
      val row = Row(1, "dog,cat")
      val flattened = FlattenColumnsFunctions.flattenRowByColumnIndices(List((1, DataTypes.string, ",")))(row)
      flattened shouldBe Array(Row(1, "dog"), Row(1, "cat"))
    }

    "not produce anything else if column is empty" in {
      val row = Row(1, "")
      val flattened = FlattenColumnsFunctions.flattenRowByColumnIndices(List((1, DataTypes.string, ",")))(row)
      flattened shouldBe Array(Row(1, ""))
    }
  }
}
