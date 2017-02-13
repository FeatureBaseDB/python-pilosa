import pkg_resources
import subprocess
import os


DEFAULT_VERSION = '0.0.0-unversioned'

def _git_version():
    try:
        path = os.path.dirname(os.path.abspath(__file__))
        return subprocess.check_output(
            ['git', '-C', path, 'describe', '--tags']
            ).strip().decode(encoding='utf-8', errors='ignore')
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
