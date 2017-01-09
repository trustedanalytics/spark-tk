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
package org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.model

import org.apache.spark.annotation.{ DeveloperApi, Since }

/**
 * Predicted value for a node
 * @param predict predicted value
 * @param prob probability of the label (classification only)
 */
@Since("1.2.0")
@DeveloperApi
class Predict @Since("1.2.0") (
    @Since("1.2.0") val predict: Double,
    @Since("1.2.0") val prob: Double = 0.0) extends Serializable {

  override def toString: String = s"$predict (prob = $prob)"

  override def equals(other: Any): Boolean = {
    other match {
      case p: Predict => predict == p.predict && prob == p.prob
      case _ => false
    }
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(predict: java.lang.Double, prob: java.lang.Double)
  }
}
