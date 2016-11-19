/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Computes shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 */
object SingleSourceShortestPath {
 /* /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Double]

  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Double.MaxValue), spmap2.getOrElse(k, Double.MaxValue))
    }.toMap

  /**
   * Computes shortest paths to the given set of landmark vertices.
   *
   * @tparam ED the edge attribute type (not used in the computation)
    * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
    * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId],weights:Boolean): Graph[SPMap, Double] = {
    /*val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }*/

   /* val spGraph = graph.mapTriplets(tri =>{
      if (landmarks.contains(tri.dstId)) {
        if (weights)
          makeMap(tri.dstId -> 0) else makeMap(tri.dstId -> 0)
      }else{
        makeMap()
    }
    })*/

    val spGraph = graph.mapVertices { (vid, attr) =>
      graph.mapTriplets(tri => {
        if (landmarks.contains(vid)) {
          if (weights)
            if (tri.dstId == vid)
              val vid = graph.vertices.
              makeMap(tri -> 0)
            else makeMap(vid -> 0)
        } else {
          makeMap()
        }
      })
    }



    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, Double]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }*/


 // initialize all vertices except the root to have distance infinity
  type SpTuple= (Double, List[VertexId])

  private def updateSp(edge: EdgeTriplet[SpTuple,Double],edgeWeightAttribute: Option[String] = None):(SpTuple,Double) ={
    val dist = edge.srcAttr._1
    val path = edge.srcAttr._2
    val newPath = path :+ edge.dstId
    val weight = if(edgeWeightAttribute.isDefined) edge.attr else 1.0
    val newDist = dist + weight
    ((newDist,newPath),weight)
  }


  def run[VD, ED: ClassTag](graph: Graph[VD,ED],
                            srcVertexId: VertexId,
                            edgeWeightAttribute: Option[String] = None,
                            landmarks: Option[Seq[VertexId]] = None,
                            maxPathLength: Option[Double] = None): Graph[SpTuple, Double] = {

    def sendMessage(edge: EdgeTriplet[SpTuple,Double]): Iterator[(VertexId, SpTuple)] = {
      val (newAttr,weight) = updateSp(edge,edgeWeightAttribute)
      val distAtSrc = edge.srcAttr._1
      val distAtDest = edge.dstAttr._1
      //TODO: check how to separate max path length from weight and how to address them in each case (these conditions need debug)
      if ((landmarks.isDefined && landmarks.get.contains(edge.dstId)) ||
        (maxPathLength.isDefined && distAtDest >= maxPathLength.get + 1) ||
        (distAtSrc > distAtDest - weight)) {
        Iterator.empty
      }else{
        Iterator((edge.dstId, newAttr))
      }
    }

    //Initial graph
    val SpGraph = graph.mapVertices((id, _) => {
      if (id == srcVertexId) (0.0, List[VertexId](srcVertexId)) else (Double.PositiveInfinity, List[VertexId]())
    }).mapEdges(e => e.attr.asInstanceOf[Double])

    val initialMessage = (Double.PositiveInfinity, List[VertexId]())

    Pregel(SpGraph,initialMessage, Int.MaxValue, EdgeDirection.Out)(

      // vertex program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

      // send message
      sendMessage,

      // merge message
      (a, b) => if (a._1 < b._1) a else b)

  }
}
