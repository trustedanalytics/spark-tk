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
  var dbName: String = "OrientDbTest1"
  var dbUserName = "admin"
  var dbPassword = "admin"
  var rootPassword = "root"
  var dbConfig: OrientConf = null
  var orientMemoryGraph: OrientGraphNoTx = null
  var orientFileGraph: OrientGraphNoTx = null
  val dbProperties: Map[String, Any] = Map(("storage.diskCache.bufferSize", 256))
  val verticesClassName = "vertex_"
  val batchSize = Some(1000)
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
    dbConfig = new OrientConf(dbUri, dbUserName, dbUserName, rootPassword, batchSize, Some(dbProperties))
    val factory = new OrientGraphFactory(dbUri, dbUserName, dbPassword)
    orientFileGraph = factory.getNoTx
    orientFileGraph.declareIntent(new OIntentMassiveInsert())
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
