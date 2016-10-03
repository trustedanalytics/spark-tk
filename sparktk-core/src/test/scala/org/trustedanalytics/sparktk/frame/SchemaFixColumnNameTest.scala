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
package org.trustedanalytics.sparktk.frame

import org.scalatest.{ WordSpec, Matchers }

class SchemaFixColumnNameTest extends WordSpec with Matchers {
  "addColumnFixName adds numeric suffix for duplicate names" in {
    val schema = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val mod = schema.addColumnFixName(Column("a", DataTypes.str)).addColumnFixName(Column("a", DataTypes.str))
    assert(mod.columnNames == List("a", "b", "a_0", "a_1"))
  }

  "addColumnFixName adds numeric suffix for duplicate names that end in numbers" in {
    val schema = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val mod = schema.addColumnFixName(Column("a", DataTypes.str))
      .addColumnFixName(Column("a_0", DataTypes.str))
      .addColumnFixName(Column("a", DataTypes.str))
      .addColumnFixName(Column("a_0", DataTypes.str))
    assert(mod.columnNames == List("a", "b", "a_0", "a_0_0", "a_1", "a_0_1"))
  }
}