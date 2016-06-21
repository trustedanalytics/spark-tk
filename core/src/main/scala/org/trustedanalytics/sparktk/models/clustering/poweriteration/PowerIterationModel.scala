//package org.trustedanalytics.sparktk.models.clustering.poweriteration
//
//import org.apache.spark.mllib.clustering.{ PowerIterationClustering => SparkPowerIterationClustering, PowerIterationClusteringModel => SparkPowerIterationClusteringModel }
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.Row
//import org.json4s.JsonAST.JValue
//import org.trustedanalytics.sparktk.frame._
//import org.trustedanalytics.sparktk.frame.internal.RowWrapper
//import org.trustedanalytics.sparktk.frame.internal.rdd.{ FrameRdd, RowWrapperFunctions }
//import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
//
//object PowerIterationModel extends TkSaveableObject {
//
//  /**
//   *
//   * @param sc active spark context
//   * @param path the source path
//   * @param formatVersion the version of the format for the tk metadata that should be recorded.
//   * @param tkMetadata the data to save (should be a case class), must be serializable to JSON using json4s
//   * @return
//   */
//  def load(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {
//
//    validateFormatVersion(formatVersion, 1)
//    val m: PowerIterationModelTkMetaData = SaveLoad.extractFromJValue[PowerIterationModelTkMetaData](tkMetadata)
//    val sparkModel = SparkPowerIterationClusteringModel.load(sc, path)
//
//    PowerIterationModel(m.sourceColumn, m.destinationColumn, m.similarityColumn, m.k, m.maxIterations, m.initializationMode, sparkModel)
//
//  }
//
//}
//
//case class PowerIterationModel private[gmm] (sourceColumn: String,
//                                             destinationColumn: String,
//                                             similarityColumn: String,
//                                             k: Int = 2,
//                                             maxIterations: Int = 100,
//                                             initializationMode: String = "random",
//                                             sparkModel: SparkPowerIterationClusteringModel) extends Serializable {
//
//  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
//    new RowWrapperFunctions(rowWrapper)
//  }
//
//  /**
//   *
//   * @param frame
//   * @param sourceColumn
//   * @param destinationColumn
//   * @param similarityColumn
//   * @param k
//   * @param maxIterations
//   * @param initializationMode
//   * @return
//   */
//  def predict(frame: Frame,
//              sourceColumn: String,
//              destinationColumn: String,
//              similarityColumn: String,
//              k: Int = 2,
//              maxIterations: Int = 100,
//              initializationMode: String = "random"): PowerIterationModel = {
//
//    require(frame != null, "frame must not be null")
//    require(sourceColumn != null && sourceColumn.nonEmpty, "sourceColumn must not be null nor empty")
//    require(destinationColumn != null && destinationColumn.nonEmpty, "destinationColumn must not be null nor empty")
//    require(similarityColumn != null && similarityColumn.nonEmpty, "similarityColumn must not be null nor empty")
//    require(k > 1, "Number of clusters must be must be greater than 1")
//    require(maxIterations >= 1, "Maximum number of iterations must be greater than 0")
//
//    val sparkPowerIteration = new SparkPowerIterationClustering()
//    sparkPowerIteration.setInitializationMode(initializationMode)
//    sparkPowerIteration.setK(k)
//    sparkPowerIteration.setMaxIterations(maxIterations)
//
//    val trainFrameRdd = new FrameRdd(frame.schema, frame.rdd)
//    trainFrameRdd.cache()
//    val similartitiesRDD = trainFrameRdd.toSourceDestinationSimilarityRDD(sourceColumn, destinationColumn, similarityColumn)
//
//    var model = sparkPowerIteration.run(similartitiesRDD)
//    trainFrameRdd.unpersist()
//
//    val assignments = model.assignments
//    val rdd: RDD[Array[Any]] = assignments.map(x => Array(x.id, x.cluster + 1))
//    // val indexedPredictionsRdd = rdd.zipWithIndex().map { case (cluster, index) => (index.toLong, Row.apply(cluster+1)) }
//
//    val k = model.k
//    val clusterSize = rdd.map(row => ("Cluster:" + row(1).toString, 1)).reduceByKey(_ + _).collect().toMap
//    //new PowerIterationClusteringReturn(frameReference, k, clusterSize)
//
//    PowerIterationModel(sourceColumn, destinationColumn, similarityColumn, k, maxIterations, initializationMode, model)
//  }
//
//  /**
//   * Saves this model to a file
//   * @param sc active SparkContext
//   * @param path save to path
//   */
//  def save(sc: SparkContext, path: String): Unit = {
//    sparkModel.save(sc, path)
//    val formatVersion: Int = 1
//    val tkMetadata = PowerIterationModelTkMetaData(sourceColumn, destinationColumn, similarityColumn, k, maxIterations, initializationMode)
//    TkSaveLoad.saveTk(sc, path, PowerIterationModel.formatId, formatVersion, tkMetadata)
//  }
//}
//
//case class MyClass(gaussians: List[List[String]], computedGmmClusterSize: Map[String, Int])
//
//case class PowerIterationModelTkMetaData(sourceColumn: String,
//                                         destinationColumn: String,
//                                         similarityColumn: String,
//                                         k: Int = 2,
//                                         maxIterations: Int = 100,
//                                         initializationMode: String = "random") extends Serializable