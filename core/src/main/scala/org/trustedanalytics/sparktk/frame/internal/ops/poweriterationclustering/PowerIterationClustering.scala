package org.trustedanalytics.sparktk.frame.internal.ops.poweriterationclustering

import org.apache.spark.mllib.clustering.{ PowerIterationClustering => SparkPowerIterationClustering }
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameState, FrameTransformReturn, FrameTransformWithResult }
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, FrameSchema }

trait PowerIterationClusteringTransformWithResult extends BaseFrame {
  /***
    *
    * Performs Power Iteration Clustering to create less than or equal to 'k' clusters. Returns data classified into clusters along with the number of clusters
    *
    * @param sourceColumn Name of the column containing the source node
    * @param destinationColumn Name of the column containing the destination node
    * @param similarityColumn Name of the column containing the similarity
    * @param k Number of clusters to cluster the graph into. Default is 2
    * @param maxIterations Maximum number of iterations of the power iteration loop. Default is 100
    * @param initializationMode Initialization mode of power iteration clustering. This can be either "random" to use a
     random vector as vertex properties, or "degree" to use normalized sum similarities. Default is "random".
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

case class ClusterDetails(k: Int, clusterSizes: Map[String, Int]) {

}

case class PowerIterationClustering(sourceColumn: String,
                                    destinationColumn: String,
                                    similarityColumn: String,
                                    k: Int = 2,
                                    maxIterations: Int = 100,
                                    initializationMode: String = "random") extends FrameTransformWithResult[ClusterDetails] {

  override def work(state: FrameState): FrameTransformReturn[ClusterDetails] = {
    require(sourceColumn != null && sourceColumn.nonEmpty, "sourceColumn must not be null nor empty")
    require(destinationColumn != null && destinationColumn.nonEmpty, "destinationColumn must not be null nor empty")
    require(similarityColumn != null && similarityColumn.nonEmpty, "similarityColumn must not be null nor empty")
    require(k >= 2, "Number of clusters must be must be greater than 1")
    require(maxIterations >= 1, "Maximum number of iterations must be greater than 0")
    val sparkPowerIteration = new SparkPowerIterationClustering()
    sparkPowerIteration.setInitializationMode(initializationMode)
    sparkPowerIteration.setK(k)
    sparkPowerIteration.setMaxIterations(maxIterations)
    val trainFrameRdd = new FrameRdd(state.schema, state.rdd)
    require(!trainFrameRdd.isEmpty(), "Frame is empty. Please run on a non-empty Frame.")
    trainFrameRdd.cache()
    val similaritiesRDD = trainFrameRdd.toSourceDestinationSimilarityRDD(sourceColumn, destinationColumn, similarityColumn)
    var model = sparkPowerIteration.run(similaritiesRDD)
    val assignments = model.assignments
    val clustersRdd = assignments.map(row => Row.apply(row.id.toInt, row.cluster + 1))
    //val schema = FrameSchema(Vector[Column](Column("cluster", DataTypes.str)))
    val clustersMap = assignments.map(row => (row.id.toInt, row.cluster + 1))

    val schema = FrameSchema(List(Column("id", DataTypes.int64), Column("cluster", DataTypes.int32)))
    val assignmentFrame = new FrameRdd(schema, clustersRdd)

    val result = trainFrameRdd.zipFrameRdd(assignmentFrame)

    trainFrameRdd.unpersist()
    //val clusterSize = clustersMap.reduceByKey(_ + _).collect().toMap

    val clusterSize = clustersRdd.map(row => ("Cluster:" + row(1).toString, 1)).reduceByKey(_ + _).collect().toMap
    val clusterDetails = new ClusterDetails(model.k, clusterSize)

    // Return frame state, and ClusterDetails(k, cluster sizes)
    FrameTransformReturn(FrameRdd.toFrameState(assignmentFrame), clusterDetails)
  }

}
