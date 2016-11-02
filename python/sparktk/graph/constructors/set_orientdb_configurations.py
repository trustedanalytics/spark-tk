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

def set_orientdb_configurations(hostname, port_number, db_user_name, db_password, root_password, batch_size, db_properties,tc=TkContext.implicit):

    """
    Set OrientDB configurations to be passed to export_to_orientdb and import_orientdb_graph APIs.

    Parameters
    ----------

    :param:(str) hostname: OrientDB server hostname
    :param:(str) port_number: OrientDB server port number
    :param:(str) db_user_name: OrientDB database user name
    :param:(str) password: the database password
    :param:(str) root_password: OrientDB server root password
    :param:(int) batch_size: batch size for graph ETL to OrientDB database
    :param:(Optional(dict(str,any))) db_properties: additional properties for OrientDB database

    :return:  OrientConf object of OrientDB configurations

    Example
    -------


    """
    TkContext.validate(tc)
    return OrientConf(tc, tc.sc._jvm.org.trustedanalytics.sparktk.graph.internal.ops.orientdb.setOrientdbConfigurations(tc.jutils.get_scala_sc(),hostname, port_number, db_user_name, db_password, root_password, batch_size, db_properties))


class OrientConf(PropertiesObject):
    """
    OrientConf holds the configurations for OrientDB export and import APIs in Spark-TK
    """
    def __init__(self, tc,scala_result):
        self._tc = tc
        self._hostname = scala_result.hostname()
        self._port_number= scala_result.port_number()
        self._db_user_name = scala_result.db_user_name()
        self._db_password = scala_result.db_password()
        self._root_password = scala_result.root_password()
        self._batch_size = scala_result.batch_size()
        self._db_properties = self._tc.jutils.convert.scala_option_map_to_python(scala_result.db_properties())

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

    @property
    def batch_size(self):
        """the batch size to be committed to OrientDB database"""
        return self._batch_size

    @property
    def db_properties(self):
        """additional parameters to configure OrientDB database"""
        return self._db_properties
