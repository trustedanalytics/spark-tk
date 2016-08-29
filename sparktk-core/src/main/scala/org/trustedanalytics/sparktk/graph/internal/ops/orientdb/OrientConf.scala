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