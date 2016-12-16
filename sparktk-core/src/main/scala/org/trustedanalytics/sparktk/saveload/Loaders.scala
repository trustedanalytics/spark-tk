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
package org.trustedanalytics.sparktk.saveload

import org.apache.spark.SparkContext
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.dicom.Dicom
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.graph.Graph
import org.trustedanalytics.sparktk.models.clustering.lda.LdaModel
import org.trustedanalytics.sparktk.models.classification.logistic_regression.LogisticRegressionModel
import org.trustedanalytics.sparktk.models.dimreduction.pca.PcaModel
import org.trustedanalytics.sparktk.models.classification.naive_bayes.NaiveBayesModel
import org.trustedanalytics.sparktk.models.classification.random_forest_classifier.RandomForestClassifierModel
import org.trustedanalytics.sparktk.models.classification.svm.SvmModel
import org.trustedanalytics.sparktk.models.clustering.kmeans.KMeansModel
import org.trustedanalytics.sparktk.models.clustering.gmm.GaussianMixtureModel
import org.trustedanalytics.sparktk.models.recommendation.collaborative_filtering.CollaborativeFilteringModel
import org.trustedanalytics.sparktk.models.timeseries.arima.ArimaModel
import org.trustedanalytics.sparktk.models.timeseries.arimax.ArimaxModel
import org.trustedanalytics.sparktk.models.timeseries.arx.ArxModel
import org.trustedanalytics.sparktk.models.timeseries.max.MaxModel
import org.trustedanalytics.sparktk.models.regression.random_forest_regressor.RandomForestRegressorModel
import org.trustedanalytics.sparktk.models.regression.linear_regression.LinearRegressionModel
import org.trustedanalytics.sparktk.models.survivalanalysis.cox_ph.CoxProportionalHazardsModel

object Loaders {

  def load(sc: SparkContext, path: String, otherLoaders: Option[Map[String, LoaderType]] = None): Any = {
    val result = TkSaveLoad.loadTk(sc, path)
    val loaderOption = loaders.get(result.formatId)

    // Find a loader that matches the specified formatId
    val loader = loaders.getOrElse(result.formatId, {
      otherLoaders.flatMap(_.get(result.formatId)).getOrElse({
        val otherLoaderStr: String = if (otherLoaders.isDefined) otherLoaders.get.keys.mkString("\n") else ""
        throw new RuntimeException(s"Could not find a registered loader for '${result.formatId}' stored at $path.\nRegistered loaders include: ${loaders.keys.mkString("\n")}\n${otherLoaderStr}")
      })
    })

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
    val entries: Seq[TkSaveableObject] = List(ArimaModel,
      ArxModel,
      ArimaxModel,
      MaxModel,
      CollaborativeFilteringModel,
      Dicom,
      Frame,
      GaussianMixtureModel,
      Graph,
      KMeansModel,
      LdaModel,
      LinearRegressionModel,
      LogisticRegressionModel,
      NaiveBayesModel,
      PcaModel,
      RandomForestClassifierModel,
      RandomForestRegressorModel,
      SvmModel,
      CoxProportionalHazardsModel)
    entries.map(e => e.formatId -> e.loadTkSaveableObject _).toMap
  }

}
