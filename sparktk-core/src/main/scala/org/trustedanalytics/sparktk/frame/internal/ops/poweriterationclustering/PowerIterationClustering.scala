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
package org.trustedanalytics.sparktk.frame.internal.ops.poweriterationclustering

import org.apache.spark.mllib.clustering.{ PowerIterationClustering => SparkPowerIterationClustering }
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, FrameSchema }

trait PowerIterationClusteringSummarization extends BaseFrame {
  /**
   * *
   *
   * Performs Power Iteration Clustering to create less than or equal to 'k' clusters. Returns data classified
   * into clusters along with the number of clusters and a map of clusters and respective sizes.
   *
   * @param sourceColumn Name of the column containing the source node
   * @param destinationColumn Name of the column containing the destination node
   * @param similarityColumn Name of the column containing the similarity
   * @param k Number of clusters to cluster the graph into. Default is 2
   * @param maxIterations Maximum number of iterations of the power iteration loop. Default is 100
   * @param initializationMode Initialization mode of power iteration clustering. This can be either "random" to use a
   * random vector as vertex properties, or "degree" to use normalized sum similarities. Default is "random".
   * @return Returns a k and cluster size that belong to class ClusterDetails
   */
  def powerIterationClustering(sourceColumn: String,
                               destinationColumn: String,
                               similarityColumn: String,
                               k: Int = 2,
                               maxIterations: Int = 100,
                               initializationMode: String = "random"): ClusterDetails = {

    execute(PowerIterationClustering(sourceColumn, destinationColumn, similarityColumn, k, maxIterations, initializationMode))
  }

}

/**
 * *
 *
 * @param k : number of clusters as a result of running Power Iteration
 * @param clusterSizes : A map of cluster names and cluster sizes
 */
case class ClusterDetails(clusterMapFrame: Frame, k: Int, clusterSizes: Map[String, Int])

case class PowerIterationClustering(sourceColumn: String,
                                    destinationColumn: String,
                                    similarityColumn: String,
                                    k: Int,
                                    maxIterations: Int,
                                    initializationMode: String) extends FrameSummarization[ClusterDetails] {

  override def work(state: FrameState): ClusterDetails = {
    require(sourceColumn != null && sourceColumn.nonEmpty, "sourceColumn must not be null nor empty")
    require(destinationColumn != null && destinationColumn.nonEmpty, "destinationColumn must not be null nor empty")
    require(similarityColumn != null && similarityColumn.nonEmpty, "similarityColumn must not be null nor empty")
    require(k >= 2, "Number of clusters must be must be greater than 1")
    require(maxIterations >= 1, "Maximum number of iterations must be greater than 0")
    val sparkPowerIteration = new SparkPowerIterationClustering()
      .setInitializationMode(initializationMode)
      .setK(k)
      .setMaxIterations(maxIterations)
    val trainFrameRdd = new FrameRdd(state.schema, state.rdd)
    require(!trainFrameRdd.isEmpty(), "Frame is empty. Please run on a non-empty Frame.")
    trainFrameRdd.cache()
    val similaritiesRDD = trainFrameRdd.toSourceDestinationSimilarityRDD(sourceColumn, destinationColumn, similarityColumn)
    var model = sparkPowerIteration.run(similaritiesRDD)
    val assignments = model.assignments
    val clustersRdd = assignments.map(row => Row.apply(row.id.toInt, row.cluster.toInt + 1))

    val schema = FrameSchema(List(Column("id", DataTypes.int32), Column("cluster", DataTypes.int32)))
    val assignmentFrame = new Frame(clustersRdd, schema)

    trainFrameRdd.unpersist()

    val clusterSize = clustersRdd.map(row => (row(1).toString, 1)).reduceByKey(_ + _).collect().toMap
    // Return ClusterDetails(result_frame, k, cluster sizes)
    new ClusterDetails(assignmentFrame, model.k, clusterSize)
  }

}
