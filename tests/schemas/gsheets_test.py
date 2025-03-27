"""
Tests for the GSheets schema.
"""

from pytest_mock import MockerFixture
from sqlalchemy.engine.url import make_url

from enginator.schemas.gsheets import GSheetsSchema


def test_gsheets_schema(mocker: MockerFixture) -> None:
    """
    Test the GSheets schema.
    """
    create_engine = mocker.patch("enginator.schemas.gsheets.create_engine")

    gsheets_schema = GSheetsSchema()

    engine = gsheets_schema.get_engine()
    create_engine.assert_called_with(make_url("gsheets+apsw://"))

    assert gsheets_schema.get_catalogs(engine) == set()
    assert gsheets_schema.get_namespaces(engine) == {"main"}

    assert gsheets_schema.match("gsheets") is True
    assert gsheets_schema.match("gsheets", "apsw") is True
    assert gsheets_schema.match("postgresql") is False
