# Copyright 2017 Pilosa Corp.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
# contributors may be used to endorse or promote products derived
# from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
# CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
#

import os
import subprocess

import pkg_resources

DEFAULT_VERSION = '0.0.0-unversioned'


def _git_version():
    try:
        path = os.path.dirname(os.path.abspath(__file__))
        return subprocess.check_output(
            ['git', '-C', path, 'describe', '--tags']
            ).strip().decode(encoding='utf-8', errors='ignore')
    except (OSError, AttributeError, subprocess.CalledProcessError):
        return None


def _installed_version():
    try:
        return pkg_resources.require('pilosa')[0].version
    except pkg_resources.DistributionNotFound:
        return None


def get_version():
    """
    Returns the version being used
    """
    return _installed_version() or _git_version() or DEFAULT_VERSION


def _get_version_setup():
    """
    Returns the version for setup.py
    """
    return _git_version() or _installed_version() or DEFAULT_VERSION


VERSION = get_version()
