import os
import zipfile
import tempfile
import shutil
import logging
logger = logging.getLogger('sparktk')


def zip_package(package_parent, package_name, zip_handle, exclude_dirs=None):
    """creates a zip archive"""

    if exclude_dirs is None:
        exclude_dirs = []
    package_path = os.path.join(package_parent, package_name)
    for root, dirs, files in os.walk(package_path, topdown=True):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for f in files:
            if not f.endswith(".pyc"):
                src_path = os.path.join(root, f)
                zip_path = src_path[len(package_parent) + 1:]
                zip_handle.write(src_path, zip_path)


def zip_sparktk(archive_dir=None):
    """zips sparktk up, can specify directory where the sparktk.zip will go, else to the target dir"""

    here = os.path.abspath(__file__)
    package_parent = os.path.dirname(os.path.dirname(here))  # relative path to parent dir of sparktk (i.e. 'python/')
    package_name = 'sparktk'
    package_path = os.path.join(package_parent, package_name)
    if archive_dir is None:
        archive_dir = tempfile.mkdtemp()
    archive_name = 'sparktk.zip'
    archive_path = os.path.join(archive_dir, archive_name)
    exclude_dirs = ["tests", "doc"]
    logger.info("zipping %s to %s", package_path, archive_path)

    if os.path.exists(archive_dir):
        shutil.rmtree(archive_dir)
    os.makedirs(archive_dir)

    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zip_handle:
        zip_package(package_parent, package_name, zip_handle, exclude_dirs=exclude_dirs)

    return archive_path
