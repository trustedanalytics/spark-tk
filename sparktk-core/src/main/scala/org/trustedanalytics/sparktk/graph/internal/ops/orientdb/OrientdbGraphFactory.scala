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

import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphFactory, OrientGraphNoTx }
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil
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
  def graphDbConnector(dbConfigurations: OrientConf,dbName:String): OrientGraphNoTx = {
    val dbUrl = getUrl(dbConfigurations,dbName)
    val orientDb: ODatabaseDocumentTx = new ODatabaseDocumentTx(dbUrl)
    dbConfigurations.dbProperties.foreach(propertyMap => {
      propertyMap.foreach { case (key, value) => orientDb.setProperty(key, value) }
    })
    val orientGraphDb = if (dbUrl.startsWith("remote:")) {
      if (!new OServerAdmin(dbUrl).connect(rootUserName, dbConfigurations.rootPassword).existsDatabase()) {
        new OServerAdmin(dbUrl).connect(rootUserName, dbConfigurations.rootPassword).createDatabase("graph", "plocal")
        openGraphDb(orientDb, dbConfigurations, dbUrl)
      }
      else {
        new OServerAdmin(dbUrl).connect(rootUserName, dbConfigurations.rootPassword)
        openGraphDb(orientDb, dbConfigurations,dbUrl)
      }
    }
    else if (!orientDb.exists()) {
      createGraphDb(dbConfigurations,dbUrl)
    }
    else {
      openGraphDb(orientDb, dbConfigurations,dbUrl)
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
  def createGraphDb(dbConfigurations: OrientConf, dbUrl:String): OrientGraphNoTx = {
    val graph = Try {
      val factory = new OrientGraphFactory(dbUrl, dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      factory.declareIntent(new OIntentMassiveInsert())
      factory.getDatabase.getMetadata.getSecurity.authenticate(dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      factory.getNoTx
    } match {
      case Success(orientGraph) => orientGraph
      case Failure(ex) =>
        throw new RuntimeException(s"Unable to create database: $dbUrl, ${ex.getMessage}")
    }
    graph
  }

  /**
   * open Orient graph database
   *
   * @param dbConfigurations OrientDB configurations
   * @return a non transactional Orient graph database instance
   */
  private def openGraphDb(orientDb: ODatabaseDocumentTx, dbConfigurations: OrientConf,dbUrl:String): OrientGraphNoTx = {
    Try {
      val db: ODatabaseDocumentTx = orientDb.open(dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      db
    } match {
      case Success(db) => new OrientGraphNoTx(db)
      case Failure(ex) =>
        throw new scala.RuntimeException(s"Unable to open database: $dbUrl, ${ex.getMessage}")
    }
  }

  /**
    * create the database URL
    *
    * @param orientConf OrientDB configurations
    * @param dbName OrientDB database name
    * @return full database URL
    */
  def getUrl(orientConf: OrientConf,dbName:String):String = {
    val dbUrl = s"remote:${orientConf.hostname}: ${orientConf.portNumber}/" + dbName
    dbUrl
  }

}
