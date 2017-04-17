# -*- coding: utf-8

import unittest

from pilosa.exceptions import ValidationError
from pilosa.validator import validate_frame_name, validate_database_name, validate_label


class ValidatorTestCase(unittest.TestCase):
    VALID_DATABASE_NAMES = [
        "a", "ab", "ab1", "1", "_", "-", "b-c", "d_e",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ]

    INVALID_DATABASE_NAMES = [
        "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    ]

    VALID_FRAME_NAMES = [
        "a", "ab", "ab1", "b-c", "d_e", "d.e", "1",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ]

    INVALID_FRAME_NAMES = [
        "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce", "_", "-", ".data",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    ]

    VALID_LABELS = [
        "a", "ab", "ab1", "d_e", "A", "Bc", "B1", "aB",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ]

    INVALID_LABELS = [
        "", "1", "_", "-", "b-c", "'", "^", "/", "\\", "*", "a:b", "valid?no", "yüce",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    ]

    def test_validate_valid_database_name(self):
        for name in self.VALID_DATABASE_NAMES:
            validate_database_name(name)

    def test_validate_valid_database_name_fails(self):
        for name in self.INVALID_DATABASE_NAMES:
            try:
                validate_database_name(name)
            except ValidationError:
                continue
            self.fail("Validation should have failed for: " + name)

    def test_validate_valid_frame_name(self):
        for name in self.VALID_FRAME_NAMES:
            validate_frame_name(name)

    def test_validate_frame_name_fails(self):
        for name in self.INVALID_FRAME_NAMES:
            try:
                validate_frame_name(name)
            except ValidationError:
                continue
            self.fail("Validation should have failed for: " + name)

    def test_validate_valid_label(self):
        for label in self.VALID_LABELS:
            validate_label(label)

    def test_validate_valid_frame_name_fails(self):
        for label in self.INVALID_LABELS:
            try:
                validate_label(label)
            except ValidationError:
                continue
            self.fail("Validation should have failed for: " + label)
