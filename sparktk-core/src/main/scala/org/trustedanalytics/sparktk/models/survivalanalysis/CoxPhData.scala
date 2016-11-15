package org.trustedanalytics.sparktk.models.survivalanalysis

import org.apache.spark.ml.regression.org.trustedanalytics.sparktk.CoxPhModel

case class CoxPhData(coxModel: CoxPhModel, featureColumns: List[String], timeColumn: String, censorColumn: String) {
  require(featureColumns != null && featureColumns.nonEmpty, "featureColumns must not be null nor empty")
  require(coxModel != null, "coxModel must not be null")
}