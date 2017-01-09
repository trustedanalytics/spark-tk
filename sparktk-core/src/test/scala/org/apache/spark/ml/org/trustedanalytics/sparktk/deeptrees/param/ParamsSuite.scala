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

import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util.SparkFunSuite
import org.apache.spark.ml.param._

object ParamsSuite extends SparkFunSuite {

  /**
   * Checks common requirements for [[Params.params]]:
   *   - params are ordered by names
   *   - param parent has the same UID as the object's UID
   *   - param name is the same as the param method name
   *   - obj.copy should return the same type as the obj
   */
  def checkParams(obj: Params): Unit = {
    val clazz = obj.getClass

    val params = obj.params
    val paramNames = params.map(_.name)
    require(paramNames === paramNames.sorted, "params must be ordered by names")
    params.foreach { p =>
      assert(p.parent === obj.uid)
      assert(obj.getParam(p.name) === p)
      // TODO: Check that setters return self, which needs special handling for generic types.
    }

    val copyMethod = clazz.getMethod("copy", classOf[ParamMap])
    val copyReturnType = copyMethod.getReturnType
    require(copyReturnType === obj.getClass,
      s"${clazz.getName}.copy should return ${clazz.getName} instead of ${copyReturnType.getName}.")
  }
}
