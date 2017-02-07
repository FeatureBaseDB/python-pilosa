import pkg_resources
import subprocess
import os

def get_version():
    try:
        return  pkg_resources.require('pilosa-driver')[0].version
    except pkg_resources.DistributionNotFound:
        try:
            path = os.path.dirname(os.path.abspath(__file__))
            return subprocess.check_output(['git', '-C', path, 'describe', '--tags']).strip()
        except OSError:
            return 'unversioned'
