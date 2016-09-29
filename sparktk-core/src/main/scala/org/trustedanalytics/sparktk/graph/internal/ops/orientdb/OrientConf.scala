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
package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

/**
 * the database configurations parameters
 * @param dbUri the database Uri
 * @param dbUserName the database user name
 * @param dbPassword the database password
 * @param rootPassword the root password
 * @param dbProperties additional database properties
 */
case class OrientConf(dbUri: String, dbUserName: String, dbPassword: String, rootPassword: String, batchSize: Int = 1000, dbProperties: Option[Map[String, Any]] = None) extends Serializable {

  require(dbUri != null, "database URI is required")
  require(dbUserName != null, "the user name is required")
  require(dbPassword != null, "dbPassword is required")
  require(rootPassword != null, "the root password is required")
  require(batchSize > 0, "batch size should be a positive value")
}