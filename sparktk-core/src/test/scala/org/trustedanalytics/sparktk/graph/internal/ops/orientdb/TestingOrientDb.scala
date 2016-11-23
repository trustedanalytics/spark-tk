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

import java.io.File

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphFactory, OrientGraphNoTx }
import org.trustedanalytics.sparktk.testutils.DirectoryUtils

/**
 * setup for testing export to OrientDB plugin functions
 */
trait TestingOrientDb {

  var tmpDir: File = null
  var dbUri: String = null
  var dbName: String = "OrientDbTest"
  var dbUserName = "admin"
  var dbPassword = "admin"
  var orientMemoryGraph: OrientGraphNoTx = null
  var orientFileGraph: OrientGraphNoTx = null

  /**
   * create in memory Orient graph database
   */
  def setupOrientDbInMemory(): Unit = {
    val uuid = java.util.UUID.randomUUID.toString
    orientMemoryGraph = new OrientGraphNoTx("memory:OrientTestDb" + uuid)
  }

  /**
   * create plocal Orient graph database
   */
  def setupOrientDb(): Unit = {
    val uuid = java.util.UUID.randomUUID.toString
    tmpDir = DirectoryUtils.createTempDirectory("orientgraphtests")
    dbUri = "plocal:" + tmpDir.getAbsolutePath + "/" + dbName + uuid
    val factory = new OrientGraphFactory(dbUri, dbUserName, dbPassword)
    orientFileGraph = factory.getNoTx
  }

  /**
   * commit the transaction and close/drop the graph database
   */
  def cleanupOrientDb(): Unit = {
    try {
      if (orientFileGraph != null) {
        orientFileGraph.commit()
        orientFileGraph.drop()
      }
    }
    finally {
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }
  }

  /**
   * commit the transaction and close the graph database
   */
  def cleanupOrientDbInMemory(): Unit = {
    try {
      if (orientMemoryGraph != null) {
        orientMemoryGraph.commit()
      }
    }
    finally {
      orientMemoryGraph.shutdown()
    }
  }
}
