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
 * OrientDB database configuration
 *
 * @param hostname OrientDB hostname
 * @param portNumber OrientDB port number
 * @param dbUserName the database user name
 * @param dbPassword the database password
 * @param rootPassword the root password
 * @param batchSize batch size
 * @param dbProperties  additional database properties
 */
case class OrientdbConf(hostname: String, portNumber: String, dbUserName: String, dbPassword: String, rootPassword: String, dbProperties: Option[Map[String, Any]] = None, batchSize: Int = 1000) extends Serializable {

  require(hostname != null, "host name is required")
  require(portNumber != null, "port number is required")
  require(dbUserName != null, "the user name is required")
  require(dbPassword != null, "dbPassword is required")
  require(rootPassword != null, "the root password is required")
  require(batchSize > 0, "batch size should be a positive value")
}

object OrientConfig {

  /**
   * creates OrientDB database credentials and other database parameters
   */
  def createOrientdbConf(hostName: String,
                         portNumber: String,
                         userName: String,
                         password: String,
                         rootPassword: String,
                         dbProperties: Option[Map[String, Any]] = None,
                         batchSize: Int = 1000): OrientdbConf = {
    OrientdbConf(hostName, portNumber, userName, password, rootPassword, dbProperties, batchSize)
  }
}
