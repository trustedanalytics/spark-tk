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
package org.trustedanalytics.sparktk.models.survivalanalysis

import org.apache.spark.ml.regression.org.trustedanalytics.sparktk.CoxPhModel

case class CoxPhData(coxModel: CoxPhModel, featureColumns: List[String], timeColumn: String, censorColumn: String) {
  require(featureColumns != null && featureColumns.nonEmpty, "featureColumns must not be null nor empty")
  require(coxModel != null, "coxModel must not be null")
}