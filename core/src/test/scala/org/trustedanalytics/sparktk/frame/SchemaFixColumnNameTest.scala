package org.trustedanalytics.sparktk.frame

import org.scalatest.{ WordSpec, Matchers }

class SchemaFixColumnNameTest extends WordSpec with Matchers {
  "addColumnFixName adds numeric suffix for duplicate names" in {
    val schema = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val mod = schema.addColumnFixName(Column("a", DataTypes.str)).addColumnFixName(Column("a", DataTypes.str))
    mod.columnNames
    assert(mod.columnNames == List("a", "b", "a_0", "a_1"))
  }

  "addColumnFixName adds numeric suffix for duplicate names that end in numbers" in {
    val schema = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val mod = schema.addColumnFixName(Column("a", DataTypes.str))
      .addColumnFixName(Column("a_0", DataTypes.str))
      .addColumnFixName(Column("a", DataTypes.str))
      .addColumnFixName(Column("a_0", DataTypes.str))
    mod.columnNames
    assert(mod.columnNames == List("a", "b", "a_0", "a_0_0", "a_1", "a_0_1"))
  }
}