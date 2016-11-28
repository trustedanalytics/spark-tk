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
package org.trustedanalytics.sparktk.models.clustering.lda

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.sparktk.frame.Frame

/**
 * Arguments to the LDA train plugin - see user docs for more on the parameters
 *
 * @param frame Input frame data
 * @param documentColumnName Column Name for documents. Column should contain a str value.
 * @param wordColumnName Column name for words. Column should contain a str value.
 * @param wordCountColumnName Column name for word count. Column should contain an int32 or int64 value.
 * @param maxIterations The maximum number of iterations that the algorithm will execute.
 *                      The valid value range is all positive int. Default is 20.
 * @param alpha  The :term:`hyperparameter` for document-specific distribution over topics.
 *               Mainly used as a smoothing parameter in :term:`Bayesian inference`.
 *               If set to a singleton list List(-1d), then docConcentration is set automatically.
 *               If set to singleton list List(t) where t != -1, then t is replicated to a vector of length k during
 *               LDAOptimizer.initialize(). Otherwise, the alpha must be length k.
 *               Currently the EM optimizer only supports symmetric distributions, so all values in the vector should be the same.
 *               Values should be greater than 1.0. Default value is -1.0 indicating automatic setting.
 * @param beta   The :term:`hyperparameter` for word-specific distribution over topics.
 *               Mainly used as a smoothing parameter in :term:`Bayesian inference`.
 *               Larger value implies that topics contain all words more uniformly and
 *               smaller value implies that topics are more concentrated on a small
 *               subset of words.
 *               Valid value range is all positive float greater than or equal to 1.
 *               Default is 0.1.
 * @param numTopics The number of topics to identify in the LDA model.
 *                  Using fewer topics will speed up the computation, but the extracted topics
 *                  might be more abstract or less specific; using more topics will
 *                  result in more computation but lead to more specific topics.
 *                  Valid value range is all positive int.
 *                  Default is 10.
 * @param seed An optional random seed.
 *                   The random seed is used to initialize the pseudorandom number generator
 *                   used in the LDA model. Setting the random seed to the same value every
 *                   time the model is trained, allows LDA to generate the same topic distribution
 *                   if the corpus and LDA parameters are unchanged.
 * @param checkPointInterval Period (in iterations) between checkpoints (default = 10).
 *                           Checkpointing helps with recovery (when nodes fail). It also helps with eliminating
 *                           temporary shuffle files on disk, which can be important when LDA is run for many
 *                           iterations. If the checkpoint directory is not set, this setting is ignored.
 *
 */
case class LdaTrainArgs(frame: Frame,
                        documentColumnName: String,
                        wordColumnName: String,
                        wordCountColumnName: String,
                        maxIterations: Int = 20,
                        alpha: Option[List[Double]] = None,
                        beta: Float = 1.1f,
                        numTopics: Int = 10,
                        seed: Option[Long] = None,
                        checkPointInterval: Int = 10) {

  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(documentColumnName), "document column name is required")
  require(StringUtils.isNotBlank(wordColumnName), "word column name is required")
  require(maxIterations > 0, "Max iterations should be greater than 0")
  if (alpha.isDefined) {
    if (alpha.get.size == 1) {
      require(alpha.get.head == -1d || alpha.get.head > 1d, "Alpha should be greater than 1.0. Or -1.0 indicating default setting ")
    }
    else {
      require(alpha.get.forall(a => a > 1d), "All values of alpha should be greater than 0")
    }
  }
  require(beta > 0, "Beta should be greater than 0")
  require(numTopics > 0, "Number of topics (K) should be greater than 0")

  def columnNames: List[String] = {
    List(documentColumnName, wordColumnName, wordCountColumnName)
  }

  def getAlpha: List[Double] = {
    alpha.getOrElse(List(-1d))
  }
}
