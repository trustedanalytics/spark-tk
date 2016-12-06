# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from sparktk.propobj import PropertiesObject
from sparktk.tkcontext import TkContext


def create_orientdb_conf(hostname,
                         port_number,
                         db_user_name,
                         db_password,
                         root_password,
                         tc=TkContext.implicit):

    """
    Create OrientDB connection settings to be passed to export_to_orientdb and import_orientdb_graph APIs.

    Parameters
    ----------

    :param hostname: (str) OrientDB server hostname
    :param port_number: (str) OrientDB server port number
    :param db_user_name: (str) OrientDB database user name
    :param db_password: (str) the database password
    :param root_password: (str) OrientDB server root password

    :return (OrientConf) OrientDB connection settings

    Example
    -------

        >>> hostname = "localhost"

        >>> port_number = "2424"

        >>> root_password = "root"

        >>> orient_conf = tc.graph.create_orientdb_config(hostname,
        ...                                               port_number,
        ...                                               "admin",
        ...                                               "admin",
        ...                                               root_password)

        >>> orient_conf
        db_password   = admin
        db_user_name  = admin
        hostname      = localhost
        port_number   = 2424
        root_password = root


    """
    TkContext.validate(tc)
    scala_obj = tc.sc._jvm.org.trustedanalytics.sparktk.graph.internal.ops.orientdb.OrientdbConnection
    return OrientdbConf(tc,
                        scala_obj.createOrientdbConf(hostname,
                                                   port_number,
                                                   db_user_name,
                                                   db_password,
                                                   root_password))


class OrientdbConf(PropertiesObject):
    """
    OrientConf holds the connection settings for OrientDB export and import APIs in Spark-TK
    """
    def __init__(self, tc, scala_result):
        self._tc = tc
        self._scala = scala_result
        self._hostname = scala_result.hostname()
        self._port_number = scala_result.portNumber()
        self._db_user_name = scala_result.dbUserName()
        self._db_password = scala_result.dbPassword()
        self._root_password = scala_result.rootPassword()

    @property
    def hostname(self):
        """OrientDB database hostname"""
        return self._hostname

    @property
    def port_number(self):
        """OrientDB server port number for binary connection"""
        return self._port_number

    @property
    def db_user_name(self):
        """OrientDB database user name"""
        return self._db_user_name

    @property
    def db_password(self):
        """OrientDB database password"""
        return self._db_password

    @property
    def root_password(self):
        """OrientDB server root password"""
        return self._root_password
