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

class SchemaHelperTest extends WordSpec with Matchers {
  "isMergeable (2 schemas) should return true" in {
    val schema1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val schema2 = FrameSchema(List(Column("c", DataTypes.str), Column("d", DataTypes.str)))
    assert(SchemaHelper.validateIsMergeable(schema1, schema2))
  }

  "isMergeable (multiple schemas) should return true" in {
    val schema1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val schema2 = FrameSchema(List(Column("c", DataTypes.str), Column("d", DataTypes.str)))
    val schema3 = FrameSchema(List(Column("e", DataTypes.str), Column("f", DataTypes.str)))
    val schema4 = FrameSchema(List(Column("g", DataTypes.str), Column("h", DataTypes.str)))
    assert(SchemaHelper.validateIsMergeable(schema1, schema2, schema3, schema4))
  }

  "isMergeable (2 schemas) should throw Exception" in {
    val schema1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val schema2 = FrameSchema(List(Column("b", DataTypes.str), Column("c", DataTypes.str)))
    intercept[IllegalArgumentException] {
      assert(SchemaHelper.validateIsMergeable(schema1, schema2))
    }
  }

  "isMergeable (multiple schemas) should throw Exception" in {
    val schema1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val schema2 = FrameSchema(List(Column("c", DataTypes.str), Column("d", DataTypes.str)))
    val schema3 = FrameSchema(List(Column("a", DataTypes.str), Column("d", DataTypes.str)))
    intercept[IllegalArgumentException] {
      assert(SchemaHelper.validateIsMergeable(schema1, schema2, schema3))
    }
  }

}
