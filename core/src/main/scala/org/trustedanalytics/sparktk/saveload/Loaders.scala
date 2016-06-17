package org.trustedanalytics.sparktk.saveload

import org.apache.spark.SparkContext
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.models.classification.naive_bayes.NaiveBayesModel
import org.trustedanalytics.sparktk.models.classification.svm.SvmModel
import org.trustedanalytics.sparktk.models.clustering.kmeans.KMeansModel

object Loaders {

  def load(sc: SparkContext, path: String): Any = {
    val result = TkSaveLoad.loadTk(sc, path)
    val loader = loaders.getOrElse(result.formatId, throw new RuntimeException(s"Could not find a registered loader for '${result.formatId}' stored at $path.\nRegistered loaders include: ${loaders.keys.mkString("\n")}"))
    loader(sc, path, result.formatVersion, result.data)
  }

  /**
   * required signature for a Loader
   *
   * sc:  SparkContext
   * path: String  the location of the file to load
   * formatVersion: Int  the version of SaveLoad format found in the accompanying tk/ folder
   * tkMetadata: JValue  the metadata loaded from the accompanying tk/ folder
   */
  type LoaderType = (SparkContext, String, Int, JValue) => Any

  // todo: use a fancier technique that probably involves reflections/macros
  /**
   * Registry of all the loaders
   *
   * If you have an class that wants to play TkSaveLoad, it needs an entry in here:
   *
   * formatId -> loader function
   */
  private lazy val loaders: Map[String, LoaderType] = {
    val entries: Seq[TkSaveableObject] = List(Frame,
      KMeansModel,
      NaiveBayesModel,
      SvmModel)
    entries.map(e => e.formatId -> e.load _).toMap
  }

}
