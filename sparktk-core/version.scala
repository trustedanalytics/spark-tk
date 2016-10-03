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
import scala.util.Properties

val EXPECTED_SPARK_VERSION = scala.util.Properties.envOrElse("SPARK_VERSION", "")
var CURRENT_SPARK_VERSION = sc.version
if (sc.version == EXPECTED_SPARK_VERSION) {
  System.exit(0);
}
else {
  println(s"Incorrect spark version, expected $EXPECTED_SPARK_VERSION got $CURRENT_SPARK_VERSION.")
  System.exit(1);
}
