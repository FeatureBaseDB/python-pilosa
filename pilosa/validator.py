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

import re

from pilosa.exceptions import ValidationError

__all__ = ("valid_index_name", "validate_index_name", "valid_frame_name",
           "validate_frame_name", "valid_label", "validate_label")


__INDEX_NAME = re.compile(r"^[a-z][a-z0-9_-]*$")
__FRAME_NAME = re.compile(r"^[a-z][a-z0-9_-]*$")
__LABEL = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")
__KEY = re.compile(r"^[A-Za-z0-9_{}+/=.~%:-]*$")
__MAX_INDEX_NAME = 64
__MAX_FRAME_NAME = 64
__MAX_LABEL = 64
__MAX_KEY = 64


def valid_index_name(index_name):
    if len(index_name) > __MAX_INDEX_NAME:
        return False
    return bool(__INDEX_NAME.match(index_name))


def validate_index_name(index_name):
    if not valid_index_name(index_name):
        raise ValidationError("Invalid index name: %s" % index_name)


def valid_frame_name(frame_name):
    if len(frame_name) > __MAX_FRAME_NAME:
        return False
    return bool(__FRAME_NAME.match(frame_name))


def valid_key(key):
    if len(key) > __MAX_KEY:
        return False
    return bool(__KEY.match(key))


def validate_frame_name(frame_name):
    if not valid_frame_name(frame_name):
        raise ValidationError("Invalid frame name: %s" % frame_name)


def valid_label(label):
    if len(label) > __MAX_LABEL:
        return False
    return bool(__LABEL.match(label))


def validate_label(label):
    if not valid_label(label):
        raise ValidationError("Invalid label: %s" % label)


def validate_key(key):
    if not valid_key(key):
        raise ValidationError("Invalid key: %s" % key)
