package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphFactory, OrientGraphNoTx }
import scala.util.{ Failure, Success, Try }

/**
 * OrientDB graph factory
 */

object OrientdbGraphFactory {

  val rootUserName = "root"

  /**
   * create/connect to OrientDB graph
   *
   * @param dbConfigurations OrientDB configurations
   * @return a non transactional OrientDB graph database instance
   */
  def graphDbConnector(dbConfigurations: OrientConf): OrientGraphNoTx = {
    val orientDb: ODatabaseDocumentTx = new ODatabaseDocumentTx(dbConfigurations.dbUri)
    dbConfigurations.dbProperties.foreach(propertyMap => {
      propertyMap.foreach { case (key, value) => orientDb.setProperty(key, value) }
    })
    val orientGraphDb = if (dbConfigurations.dbUri.startsWith("remote:")) {
      if (!new OServerAdmin(dbConfigurations.dbUri).connect(rootUserName, dbConfigurations.rootPassword).existsDatabase()) {
        new OServerAdmin(dbConfigurations.dbUri).connect(rootUserName, dbConfigurations.rootPassword).createDatabase("graph", "plocal")
        openGraphDb(orientDb, dbConfigurations)
      }
      else {
        new OServerAdmin(dbConfigurations.dbUri).connect(rootUserName, dbConfigurations.rootPassword)
        openGraphDb(orientDb, dbConfigurations)
      }
    }
    else if (!orientDb.exists()) {
      createGraphDb(dbConfigurations)
    }
    else {
      openGraphDb(orientDb, dbConfigurations)
    }
    orientGraphDb.declareIntent(new OIntentMassiveInsert())
    orientGraphDb
  }

  /**
   * create Orient graph database
   *
   * @param dbConfigurations OrientDB configurations
   * @return a non transactional Orient graph database instance
   */
  def createGraphDb(dbConfigurations: OrientConf): OrientGraphNoTx = {

    val graph = Try {
      val factory = new OrientGraphFactory(dbConfigurations.dbUri, dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      factory.declareIntent(new OIntentMassiveInsert())
      factory.getDatabase.getMetadata.getSecurity.authenticate(dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      factory.getNoTx
    } match {
      case Success(orientGraph) => orientGraph
      case Failure(ex) =>
        throw new RuntimeException(s"Unable to create database: ${dbConfigurations.dbUri}, ${ex.getMessage}")
    }
    graph
  }

  /**
   * open Orient graph database
   *
   * @param dbConfigurations OrientDB configurations
   * @return a non transactional Orient graph database instance
   */
  private def openGraphDb(orientDb: ODatabaseDocumentTx, dbConfigurations: OrientConf): OrientGraphNoTx = {
    Try {
      val db: ODatabaseDocumentTx = orientDb.open(dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      db
    } match {
      case Success(db) => new OrientGraphNoTx(db)
      case Failure(ex) =>
        throw new scala.RuntimeException(s"Unable to open database: ${dbConfigurations.dbUri}, ${ex.getMessage}")
    }
  }

}
