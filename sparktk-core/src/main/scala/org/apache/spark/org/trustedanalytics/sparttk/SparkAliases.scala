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
package org.apache.spark.org.trustedanalytics.sparktk

import org.apache.spark.api.python.{ SerDeUtil => SparkSerDeUtil, PythonUtils => SparkPythonUtils }
import org.apache.spark.util.{ BoundedPriorityQueue => SparkBoundedPriorityQueue }
import org.apache.spark.util.collection.{ Utils => SparkCollectionUtils }
import org.apache.spark.api.java.{ JavaUtils => SparkJavaUtils }

import org.apache.spark.mllib.api.python.{ SerDe => SparkMLLibSerDe }

object SparkAliases extends Serializable {

  val SerDeUtil = SparkSerDeUtil
  val MLLibSerDe = SparkMLLibSerDe

  type AutoBatchedPickler = SerDeUtil.AutoBatchedPickler
  type BoundedPriorityQueue[A] = SparkBoundedPriorityQueue[A]
  val CollectionUtils = SparkCollectionUtils
  val JavaUtils = SparkJavaUtils
  val PythonUtils = SparkPythonUtils
}
