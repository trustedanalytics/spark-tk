package org.trustedanalytics.sparktk.frame

import org.scalatest.{ WordSpec, Matchers }

class SchemaHelperTest extends WordSpec with Matchers {
  "isMergeable (2 schemas) should return true" in {
    val schema1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val schema2 = FrameSchema(List(Column("c", DataTypes.str), Column("d", DataTypes.str)))
    assert(SchemaHelper.isMergeable(schema1, schema2))
  }

  "isMergeable (multiple schemas) should return true" in {
    val schema1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val schema2 = FrameSchema(List(Column("c", DataTypes.str), Column("d", DataTypes.str)))
    val schema3 = FrameSchema(List(Column("e", DataTypes.str), Column("f", DataTypes.str)))
    val schema4 = FrameSchema(List(Column("g", DataTypes.str), Column("h", DataTypes.str)))
    assert(SchemaHelper.isMergeable(schema1, schema2, schema3, schema4))
  }

  "isMergeable (2 schemas) should throw Exception" in {
    val schema1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val schema2 = FrameSchema(List(Column("b", DataTypes.str), Column("c", DataTypes.str)))
    intercept[IllegalArgumentException] {
      assert(SchemaHelper.isMergeable(schema1, schema2))
    }
  }

  "isMergeable (multiple schemas) should throw Exception" in {
    val schema1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val schema2 = FrameSchema(List(Column("c", DataTypes.str), Column("d", DataTypes.str)))
    val schema3 = FrameSchema(List(Column("a", DataTypes.str), Column("d", DataTypes.str)))
    intercept[IllegalArgumentException] {
      assert(SchemaHelper.isMergeable(schema1, schema2, schema3))
    }
  }

}
