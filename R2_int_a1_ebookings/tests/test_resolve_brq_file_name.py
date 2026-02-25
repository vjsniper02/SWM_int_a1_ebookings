import pytest
from functions.brq_file_parser.resolve_brq_file_name import resolve_brq_file_name


def test_resolve_brq_file_name():
    (from_email, brq_request_id, brq_file_name) = resolve_brq_file_name(
        "email@email.com_ABCDEF-Request-123-456.brq"
    )
    assert from_email == "email@email.com"
    assert brq_request_id == "123-456"
    assert brq_file_name == "ABCDEF-Request-123-456.brq"


def test_resolve_brq_file_name_wrong_suffix():
    file_name = "email@email.com_ABCDEF-Request-123-456.brqXXXX"
    try:
        resolve_brq_file_name(file_name)
    except Exception as err:
        assert str(err) == f"Invalid BRQ File Name '{file_name}'"


def test_resolve_brq_file_name_no_underscore():
    file_name = "email@email.comABCDEF-Request-123-456.brq"
    try:
        resolve_brq_file_name(file_name)
    except Exception as err:
        assert str(err) == f"Invalid BRQ File Name '{file_name}'"


def test_resolve_brq_file_name_no_request():
    file_name = "email@email.comABCDEF-ReXXXquest-123-456.brq"
    try:
        resolve_brq_file_name(file_name)
    except Exception as err:
        assert str(err) == f"Invalid BRQ File Name '{file_name}'"
