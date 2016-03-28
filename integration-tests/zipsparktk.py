#!/usr/bin/env python

"""Creates a zip archive of sparktk"""

import os
import zipfile

package_parent = '../python'
package_name = 'sparktk'
archive_name = 'sparktk.zip'

def zip_package(package_parent, package_name, zip_handle):

    package_path = os.path.join(package_parent, package_name)
    for root, dirs, files in os.walk(package_path, topdown=True):
        dirs[:] = [d for d in dirs if d not in ["tests", "doc"]]  # exclude folders
        for file in files:
            if not file.endswith(".pyc"):
                src_path = os.path.join(root, file)
                zip_path = src_path[len(package_parent) + 1:]
                #print "zip_handle.write(%s, %s)" % (src_path, zip_path)
                zip_handle.write(src_path, zip_path)


if __name__ == '__main__':
    # todo - take args from sys
    with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED) as zip_handle:
        zip_package(package_parent, package_name, zip_handle)
