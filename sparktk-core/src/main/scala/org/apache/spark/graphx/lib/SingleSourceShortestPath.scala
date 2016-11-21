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

  type SpTuple= (Double, List[VertexId])

  private def updateSp(edge: EdgeTriplet[SpTuple,Double],useEdgeWeight: Boolean = false):(SpTuple,Double) ={
    val dist = edge.srcAttr._1
    val path = edge.srcAttr._2
    val newPath = path :+ edge.dstId
    val weight = if(useEdgeWeight) edge.attr else 1.0
    val newDist = dist + weight
    ((newDist,newPath),weight)
  }


  def run[VD, ED: ClassTag](graph: Graph[VD,ED],
                            srcVertexId: VertexId,
                            edgeWeightAttribute: Boolean = false,
                            landmarks: Option[Seq[VertexId]] = None,
                            maxPathLength: Option[Double] = None): Graph[SpTuple, Double] = {

    def sendMessage(edge: EdgeTriplet[SpTuple,Double]): Iterator[(VertexId, SpTuple)] = {
      val (newAttr,weight) = updateSp(edge,edgeWeightAttribute)
      val distAtSrc = edge.srcAttr._1
      val distAtDest = edge.dstAttr._1
      //TODO: check how to separate max path length from weight and how to address them in each case (these conditions need debug)
      if ((landmarks.isDefined && !landmarks.get.contains(edge.dstId)) ||
        (maxPathLength.isDefined && distAtSrc == maxPathLength.get) ||
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
