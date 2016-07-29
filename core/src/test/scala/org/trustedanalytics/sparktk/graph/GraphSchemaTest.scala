package org.trustedanalytics.sparktk.graph

import org.scalatest.{ WordSpec, Matchers }
import org.trustedanalytics.sparktk.frame.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.graph.internal.GraphSchema

class GraphSchemaTest extends WordSpec with Matchers {

  "validateVerticesSchema accepts valid columns" in {
    val good1 = FrameSchema(List(Column("id", DataTypes.str), Column("b", DataTypes.str)))
    val good2 = FrameSchema(List(Column("c", DataTypes.str), Column("id", DataTypes.int)))
    GraphSchema.validateSchemaForVerticesFrame(good1)
    GraphSchema.validateSchemaForVerticesFrame(good2)
  }

  "validateVerticesSchema should throw exception on schemas missing requisite column names" in {
    val bad1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    intercept[IllegalArgumentException] {
      GraphSchema.validateSchemaForVerticesFrame(bad1)
    }
  }

  "validateEdgesSchema accepts valid columns" in {
    val good1 = FrameSchema(List(Column("src", DataTypes.str), Column("dst", DataTypes.str)))
    val good2 = FrameSchema(List(Column("dst", DataTypes.str), Column("id", DataTypes.int), Column("src", DataTypes.int)))
    GraphSchema.validateSchemaForEdgesFrame(good1)
    GraphSchema.validateSchemaForEdgesFrame(good2)
  }

  "validateEdgesSchema should throw exception on schemas missing requisite column names" in {
    val bad1 = FrameSchema(List(Column("a", DataTypes.str), Column("b", DataTypes.str)))
    val bad2 = FrameSchema(List(Column("src", DataTypes.str), Column("b", DataTypes.str)))
    val bad3 = FrameSchema(List(Column("source", DataTypes.str), Column("dst", DataTypes.str)))
    intercept[IllegalArgumentException] {
      GraphSchema.validateSchemaForEdgesFrame(bad1)
    }
    intercept[IllegalArgumentException] {
      GraphSchema.validateSchemaForEdgesFrame(bad2)
    }
    intercept[IllegalArgumentException] {
      GraphSchema.validateSchemaForEdgesFrame(bad3)
    }
  }
}
