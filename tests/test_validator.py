# -*- coding: utf-8

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

import unittest

from pilosa.exceptions import ValidationError
from pilosa.validator import validate_frame_name, validate_index_name, validate_label, validate_key


class ValidatorTestCase(unittest.TestCase):

    VALID_INDEX_NAMES = [
        "a", "ab", "ab1", "b-c", "d_e",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ]

    INVALID_INDEX_NAMES = [
        "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    ]

    VALID_FRAME_NAMES = [
        "a", "ab", "ab1", "b-c", "d_e",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ]

    INVALID_FRAME_NAMES = [
        "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce", "_", "-", ".data", "d.e", "1",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    ]

    VALID_LABELS = [
        "a", "ab", "ab1", "d_e", "A", "Bc", "B1", "aB", "b-c",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ]

    INVALID_LABELS = [
        "", "1", "_", "-", "'", "^", "/", "\\", "*", "a:b", "valid?no", "yüce",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    ]

    VALID_KEYS = [
        "", "1", "ab", "ab1", "b-c", "d_e", "pilosa.com",
        "bbf8d41c-7dba-40c4-94dc-94677b43bcf3",  # UUID
        "{bbf8d41c-7dba-40c4-94dc-94677b43bcf3}",  # Windows GUID
        "https%3A//www.pilosa.com/about/%23contact",  # escaped URL
        "aHR0cHM6Ly93d3cucGlsb3NhLmNvbS9hYm91dC8jY29udGFjdA==",  # base64
        "urn:isbn:1234567",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ]

    INVALID_KEYS = [
        '"', "'", "slice\\dice", "valid?no", "yüce", "*xyz", "with space", "<script>",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    ]

    def test_valid_index_name(self):
        for name in self.VALID_INDEX_NAMES:
            validate_index_name(name)

    def test_invalid_index_name_fails(self):
        for name in self.INVALID_INDEX_NAMES:
            try:
                validate_index_name(name)
            except ValidationError:
                continue
            self.fail("Index name validation should have failed for: " + name)

    def test_validate_valid_frame_name(self):
        for name in self.VALID_FRAME_NAMES:
            validate_frame_name(name)

    def test_invalid_frame_name_fails(self):
        for name in self.INVALID_FRAME_NAMES:
            try:
                validate_frame_name(name)
            except ValidationError:
                continue
            self.fail("Frame name validation should have failed for: " + name)

    def test_validate_valid_label(self):
        for label in self.VALID_LABELS:
            validate_label(label)

    def test_validate_invalid_frame_name_fails(self):
        for label in self.INVALID_LABELS:
            try:
                validate_label(label)
            except ValidationError:
                continue
            self.fail("Label validation should have failed for: " + label)

    def test_validate_valid_key(self):
        for key in self.VALID_KEYS:
            validate_key(key)

    def test_validate_invalid_key_fails(self):
        for key in self.INVALID_KEYS:
            try:
                validate_key(key)
            except ValidationError:
                continue
            self.fail("Key validation should have failed for: " + key)
