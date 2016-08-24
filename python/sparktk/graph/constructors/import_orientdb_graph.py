from sparktk.tkcontext import TkContext


def import_orientdb_graph(db_url, user_name, password, root_password,tc=TkContext.implicit):
    """
    Import graph from OrientDB to spark-tk as spark-tk graph (Spark graph frame)

    Parameters
    ----------
    :param dn_url: OrientDB URI

    :param user_name: the database username

    :param password: the database password

    :param root_password: OrientDB server password
    """
    TkContext.validate(tc)
    scala_graph = tc.sc._jvm.org.trustedanalytics.sparktk.graph.internal.constructors.fromorientdb.ImportFromOrientdb.importOrientdbGraph(tc.jutils.get_scala_sc(), db_url,user_name,password,root_password)
    from sparktk.graph.graph import Graph
    return Graph(tc, scala_graph)
