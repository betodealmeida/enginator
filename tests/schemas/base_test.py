"""
Tests for the base schema.
"""

from pytest_mock import MockerFixture

from enginator.schemas.base import BaseSchema


def test_base_schema(mocker: MockerFixture) -> None:
    """
    Test the base schema.
    """
    base_schema = BaseSchema()
    make_engine = mocker.patch.object(base_schema, "make_engine")

    data = {
        "engine": "dummy",
        "driver": "dummy",
        "catalog": "my_db",
        "namespace": "my_schema",
    }
    base_schema.get_engine(**data)

    make_engine.assert_called_once_with(data)


def test_hierarchy_map(mocker: MockerFixture) -> None:
    """
    Test that engine can be instantiated with native terms.
    """
    base_schema = BaseSchema()
    base_schema.hierarchy_map = {
        "catalog": "database",
        "namespace": "schema",
    }
    make_engine = mocker.patch.object(base_schema, "make_engine")

    data = {
        "engine": "dummy",
        "driver": "dummy",
        "database": "my_db",  # native
        "namespace": "my_schema",  # standard
    }
    base_schema.get_engine(**data)

    make_engine.assert_called_once_with(
        {
            "engine": "dummy",
            "driver": "dummy",
            "catalog": "my_db",
            "namespace": "my_schema",
        },
    )


def test_get_namespaces(mocker: MockerFixture) -> None:
    """
    Test that the base schema can get namespaces.
    """
    inspect = mocker.patch("enginator.schemas.base.inspect")
    inspect().get_schema_names.return_value = ["public", "other"]
    engine = mocker.MagicMock()

    base_schema = BaseSchema()
    assert base_schema.get_namespaces(engine) == {"public", "other"}
