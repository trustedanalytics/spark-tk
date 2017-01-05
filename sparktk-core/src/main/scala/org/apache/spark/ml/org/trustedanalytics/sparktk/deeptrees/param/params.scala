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
package org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.param

import org.json4s._
import org.json4s.jackson.JsonMethods._
//import org.json4s.JsonDSL._
import org.apache.spark.annotation.{ DeveloperApi, Experimental }
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector, Vector, Vectors }
import org.json4s.jackson.JsonMethods.{ parse => parseJson }

private[ml] object Param {

  /** Decodes a param value from JSON. */
  def jsonDecode[T](json: String): T = {
    parse(json) match {
      case JString(x) =>
        x.asInstanceOf[T]
      case JObject(v) =>
        val keys = v.map(_._1)
        assert(keys.contains("type") && keys.contains("values"),
          s"Expect a JSON serialized vector but cannot find fields 'type' and 'values' in $json.")
        JsonVectorConverter.fromJson(json).asInstanceOf[T]
      case _ =>
        throw new NotImplementedError(
          "The default jsonDecode only supports string and vector. " +
            s"${this.getClass.getName} must override jsonDecode to support its value type.")
    }
  }
}

