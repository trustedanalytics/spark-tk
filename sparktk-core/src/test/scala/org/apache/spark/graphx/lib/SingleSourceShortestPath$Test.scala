package org.apache.spark.graphx.lib

import org.apache.spark.graphx._
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class SingleSourceShortestPath$Test extends TestingSparkContextWordSpec{

  "Shortest paths" should{

    "find SPs" in {
      var results:Graph[(Double, List[VertexId]),Double] = null
      // create vertices RDD with ID and Name
      val vertices=Array((1L, ("SFO")),(2L, ("ORD")),(3L,("DFW")),(4L,("PDX")),(5L,("LAX")),(6L,("LLL")),(7L,("IAF")))
      val vRDD= sparkContext.parallelize(vertices)

      // create routes RDD with srcid, destid, distance
      val edges = Array(Edge(1L,2L,1800.0),Edge(2L,3L,800.0),Edge(3L,1L,1400.0),Edge(2L,4L,900.0),Edge(1L,7L,1100.0),Edge(3L,5L,700.0), Edge(4L,6L,600.0),Edge(1L,6L,500.0))
      val eRDD= sparkContext.parallelize(edges)

      // define the graph
      val graph = Graph(vRDD,eRDD)
      results = SingleSourceShortestPath.run(graph,1,false,Some(Seq(3)),Some(6.0))
      println(results.vertices.collect.mkString("\n"))
      println(results.edges.collect.mkString("\n"))
      }

  }

}
