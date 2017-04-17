import re

from pilosa.exceptions import ValidationError

__DATABASE_NAME = re.compile(r"^[a-z0-9_-]+$")
__FRAME_NAME = re.compile(r"^[a-z0-9][.a-z0-9_-]*$")
__LABEL = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
__MAX_DATABASE_NAME = 64
__MAX_FRAME_NAME = 64
__MAX_LABEL = 64


def valid_database_name(database_name):
    if len(database_name) > __MAX_DATABASE_NAME:
        return False
    return bool(__DATABASE_NAME.match(database_name))


def validate_database_name(database_name):
    if not valid_database_name(database_name):
        raise ValidationError("Invalid database name: %s" % database_name)


def valid_frame_name(frame_name):
    if len(frame_name) > __MAX_FRAME_NAME:
        return False
    return bool(__FRAME_NAME.match(frame_name))


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
