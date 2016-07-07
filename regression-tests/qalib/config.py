# vim: set encoding=utf-8 
""" Global Config file for testcases, used heavily by automation"""
import os


qa_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
hdfs_namenode = os.getenv("CDH_MASTER", "localhost")
user = os.getenv("USER", "hadoop")
