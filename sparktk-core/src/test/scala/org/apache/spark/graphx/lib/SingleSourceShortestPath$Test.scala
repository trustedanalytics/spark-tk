package org.apache.spark.graphx.lib

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class SingleSourceShortestPath$Test extends TestingSparkContextWordSpec with Matchers {

  "Shortest paths" should{

    "find SPs" in {

      // create vertices RDD with ID and Name
      val vertices=Array((1L, ("SFO")),(2L, ("ORD")),(3L,("DFW")))
      val vRDD= sparkContext.parallelize(vertices)

      // create routes RDD with srcid, destid, distance
      val edges = Array(Edge(1L,2L,1800.0),Edge(2L,3L,800.0),Edge(3L,1L,1400.0))
      val eRDD= sparkContext.parallelize(edges)

      // define the graph
      val graph = Graph(vRDD,eRDD)
      // graph vertices
      //graph.vertices.collect.foreach(println)
      // (2,ORD)
      // (1,SFO)
      // (3,DFW)

      // graph edges
      //graph.edges.collect.foreach(println)

      // Edge(1,2,1800)
      // Edge(2,3,800)
      // Edge(3,1,1400)

      val results = SingleSourceShortestPath.run(graph,1,None,None,None)
      println(results.vertices.collect.mkString("\n"))

      }

  }

}
