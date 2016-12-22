package org.apache.spark.graphx.lib.org.trustedanalytics.sparktk

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  *
  */
object ClosenessCentrality {

  // closeness centrality using Spark Single source shortest path.(works good for directed graphs)
  def run[VD, ED: ClassTag](graph: Graph[VD, ED],
                            targets: Option[Seq[VertexId]] = None,
                            getEdgeWeight: Option[ED => Double] = None, normalized: Boolean = true):  Seq[ClosenessCalc] = {
    val numberOfVertices = graph.vertices.count.toDouble
    val verticesIds = graph.vertices.map{case(id,_) => id}.collect().toSeq
    val allVerticesCloseness =  calculateShortestPaths(graph,Some(verticesIds)).vertices.map{ case(id, spMap) =>
      val count = spMap.values.toSeq.length.toDouble
      val cost = spMap.values.sum.toDouble
      val closenessValue =  if (cost > 0.0 && numberOfVertices > 1.0) {
        if (normalized){
          val s = (count-1.0)/(numberOfVertices-1.0)
          (count -1.0)*s/cost
        }else {
          (count-1.0)/cost
        }
      }else{
        0.0
      }
      ClosenessCalc(id, closenessValue)}.collect().toSeq
    val closenessResults = if (targets.isDefined){
      val targetClosenes = allVerticesCloseness.filter(nn => targets.get.contains(nn.vertexId))
      targetClosenes
    }else{
      allVerticesCloseness
    }
    closenessResults
  }


  /** SPs using considering the edge weight */
  type SPMap = Map[VertexId, Double]

  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  private def incrementMap(edge:EdgeTriplet[SPMap,Double]): SPMap = {
    val weight = edge.attr
    edge.dstAttr.map { case (v, d) => v -> (d + weight) }
  }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Double.MaxValue), spmap2.getOrElse(k, Double.MaxValue))
    }.toMap


  def calculateShortestPaths[VD, ED: ClassTag](graph: Graph[VD, ED],
                            targets: Option[Seq[VertexId]] = None,
                            getEdgeWeight: Option[ED => Double] = None): Graph[SPMap,Double] ={
    //Initial graph
    val shortestPathGraph = graph.mapVertices((id, _) => {
      if (targets.isDefined){
        if(targets.get.contains(id)) makeMap(id -> 0) else makeMap()
      }
      else {
        makeMap(id -> 0)
      }
    }).mapEdges(e => getEdgeWeight match {
      case Some(func) => func(e.attr)
      case _ => 1.0
    })

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap,Double]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(shortestPathGraph, initialMessage)(vertexProgram, sendMessage, addMaps)

  }
}

case class ClosenessCalc(vertexId: VertexId, closenessCentrality: Double)


