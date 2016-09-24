from setup import tc, rm, get_sandbox_path
from sparktk import sparkconf
import os

def test_get_jars_filters_duplictes(tc):
    """
    Tests that get_jars_and_classpaths() does not return duplicate jars.  Calls the function with duplicate
    directories, so that in the function, it should get duplicate jars, but those should be filtered out
    before the jars are returned.
    """
    sparktk_dirs = sparkconf.get_sparktk_dirs()
    duplicate_dirs = sparktk_dirs + sparktk_dirs
    # call get_jars_and_classpaths with duplicate directories so that we can be sure to have
    # duplicate jars that should get filtered out.
    paths = sparkconf.get_jars_and_classpaths(duplicate_dirs)

    # expect to get back jars and class paths
    assert(len(paths) == 2)

    # get jar names from the paths
    jars = str.split(paths[0], ",")
    jar_names = [os.path.basename(j) for j in jars]

    # jar names should all be unique
    assert(len(set(jar_names)) == len(jar_names))

