import pkg_resources
import subprocess
import os


DEFAULT_VERSION = '0.0.0-unversioned'

def _git_version():
    try:
        path = os.path.dirname(os.path.abspath(__file__))
        command = ['git', '-C', path, 'describe', '--tags']
        try:
            return subprocess.check_output(command).strip().decode(
                   encoding='utf-8', errors='ignore')
        # subprocess.check_output does not exist in Python < 2.7:
        except AttributeError:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE)
            return proc.communicate()[0].split()[0]
    except OSError:
        return None

def _installed_version():
    try:
        return pkg_resources.require('pilosa-driver')[0].version
    except pkg_resources.DistributionNotFound:
        return None

def get_version():
    """
    Returns the version being used
    """
    return _installed_version() or _git_version() or DEFAULT_VERSION

def get_version_setup():
    """
    Returns the version for setup.py
    """
    return _git_version() or _installed_version() or DEFAULT_VERSION
