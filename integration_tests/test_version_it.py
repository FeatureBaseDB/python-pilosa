import subprocess
import unittest

import pkg_resources

from pilosa.version import _git_version, get_version_setup


class VersionTestCase(unittest.TestCase):

    def test_git_version(self):
        # for coverage
        _git_version()

    def test_get_version_setup(self):
        def mock1(*args, **kwargs):
            raise OSError
        try:
            backup1 = subprocess.check_output
        except AttributeError:
            backup1 = None
        subprocess.check_output = mock1
        def mock2(*args, **kwargs):
            raise pkg_resources.DistributionNotFound
        backup2 = pkg_resources.require
        pkg_resources.require = mock2
        try:
            self.assertEquals("0.0.0-unversioned", get_version_setup())
        finally:
            if backup1:
                subprocess.check_output = backup1
            pkg_resources.require = backup2
