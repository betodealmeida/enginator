"""
Tests for the Postgres schema.
"""

import ssl
from typing import Any, Callable

import pytest
from marshmallow.exceptions import ValidationError
from pytest_mock import MockerFixture
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ProgrammingError

from enginator.schemas.postgres import (
    PostgresSchema,
    build_ssl_context,
    build_ssl_value,
)


class ListensForMock:  # pylint: disable=too-few-public-methods
    """
    Mocker for the `@listens_for` decorator.
    """

    functions: list[tuple[str, Callable[..., Any]]] = []

    def __init__(self, engine: Engine, event: str):  # pylint: disable=unused-argument
        self.event = event

    def __call__(self, func) -> None:
        self.functions.append((self.event, func))


def test_postgres_schema(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema.
    """
    create_engine = mocker.patch("enginator.schemas.postgres.create_engine")
    with create_engine().connect() as connection:
        connection.execute.side_effect = [
            [("my_db",)],
            [("public",), ("information_schema",)],
        ]

    postgres_schema = PostgresSchema()

    engine = postgres_schema.get_engine(host="localhost")
    create_engine.assert_called_with(
        make_url("postgresql+psycopg2://localhost:5432?sslmode=verify-full"),
    )

    assert postgres_schema.get_catalogs(engine) == {"my_db"}
    assert postgres_schema.get_namespaces(engine) == {"public", "information_schema"}

    assert postgres_schema.match("postgresql") is True
    assert postgres_schema.match("postgresql", "psycopg2") is True
    assert postgres_schema.match("gsheets") is False


def test_postgres_schema_asyncpg(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema with the asyncpg driver.
    """
    create_engine = mocker.patch("enginator.schemas.postgres.create_engine")
    # pylint: disable=redefined-outer-name
    build_ssl_context = mocker.patch("enginator.schemas.postgres.build_ssl_context")

    postgres_schema = PostgresSchema()

    postgres_schema.get_engine(driver="asyncpg", host="localhost")
    create_engine.assert_called_with(
        make_url("postgresql+asyncpg://localhost:5432"),
        connect_args={"ssl": build_ssl_context()},
    )


def test_postgres_schema_pg8000(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema with the pg8000 driver.
    """
    create_engine = mocker.patch("enginator.schemas.postgres.create_engine")
    # pylint: disable=redefined-outer-name
    build_ssl_context = mocker.patch("enginator.schemas.postgres.build_ssl_context")

    postgres_schema = PostgresSchema()

    postgres_schema.get_engine(driver="pg8000", host="localhost")
    create_engine.assert_called_with(
        make_url("postgresql+pg8000://localhost:5432"),
        connect_args={"ssl_context": build_ssl_context()},
    )


def test_postgres_schema_namespace(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema with a specific namespace.
    """
    mocker.patch("enginator.schemas.postgres.create_engine")
    mocker.patch("enginator.schemas.postgres.listens_for", new=ListensForMock)
    connection_record = conn = cursor = parameters = context = executemany = (
        mocker.MagicMock()
    )

    postgres_schema = PostgresSchema()

    postgres_schema.get_engine(
        driver="psycopg2",
        namespace="public",
        host="localhost",
    )

    assert len(ListensForMock.functions) == 2
    event1, set_namespace = ListensForMock.functions[0]
    event2, disallow_namespace_change = ListensForMock.functions[1]

    assert event1 == "connect"
    dbapi_con = mocker.MagicMock()
    set_namespace(dbapi_con, connection_record)
    dbapi_con.cursor().execute.assert_called_with('set search_path = "public"')

    assert event2 == "before_cursor_execute"
    assert (
        disallow_namespace_change(
            conn,
            cursor,
            "SELECT * FROM sales;",
            parameters,
            context,
            executemany,
        )
        is None
    )

    # clear stack
    ListensForMock.functions = []


def test_postgres_schema_namespace_altered(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema when `search_path` is altered.
    """
    mocker.patch("enginator.schemas.postgres.create_engine")
    mocker.patch("enginator.schemas.postgres.listens_for", new=ListensForMock)
    connection_record = conn = cursor = parameters = context = executemany = (
        mocker.MagicMock()
    )

    postgres_schema = PostgresSchema()

    postgres_schema.get_engine(
        driver="psycopg2",
        namespace="public",
        host="localhost",
    )

    assert len(ListensForMock.functions) == 2
    event1, set_namespace = ListensForMock.functions[0]
    event2, disallow_namespace_change = ListensForMock.functions[1]

    assert event1 == "connect"
    dbapi_con = mocker.MagicMock()
    set_namespace(dbapi_con, connection_record)
    dbapi_con.cursor().execute.assert_called_with('set search_path = "public"')

    assert event2 == "before_cursor_execute"
    with pytest.raises(ProgrammingError) as excinfo:
        disallow_namespace_change(
            conn,
            cursor,
            "SET search_path TO private; SELECT * FROM sales;",
            parameters,
            context,
            executemany,
        )
    assert excinfo.value.orig == (
        "Queries modifying `search_path` are not allowed since a default namespace "
        "has been set. Please use fully qualified names or change the default "
        "namespace for the connection."
    )

    # clear stack
    ListensForMock.functions = []


def test_postgres_schema_namespace_same(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema when `search_path` is set to the default namespace.
    """
    mocker.patch("enginator.schemas.postgres.create_engine")
    mocker.patch("enginator.schemas.postgres.listens_for", new=ListensForMock)
    connection_record = conn = cursor = parameters = context = executemany = (
        mocker.MagicMock()
    )

    postgres_schema = PostgresSchema()

    postgres_schema.get_engine(
        driver="psycopg2",
        namespace="public",
        host="localhost",
    )

    assert len(ListensForMock.functions) == 2
    event1, set_namespace = ListensForMock.functions[0]
    event2, disallow_namespace_change = ListensForMock.functions[1]

    assert event1 == "connect"
    dbapi_con = mocker.MagicMock()
    set_namespace(dbapi_con, connection_record)
    dbapi_con.cursor().execute.assert_called_with('set search_path = "public"')

    assert event2 == "before_cursor_execute"
    assert (
        disallow_namespace_change(
            conn,
            cursor,
            "SET search_path TO public; SELECT * FROM sales;",
            parameters,
            context,
            executemany,
        )
        is None
    )

    # clear stack
    ListensForMock.functions = []


def test_postgres_schema_invalid_driver(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema with an invalid driver.
    """
    mocker.patch("enginator.schemas.postgres.create_engine")
    mocker.patch("enginator.schemas.postgres.build_ssl_context")

    postgres_schema = PostgresSchema()

    with pytest.raises(ValidationError) as excinfo:
        postgres_schema.get_engine(driver="invalid", host="localhost")
    assert excinfo.value.messages == {
        "driver": ["Must be one of: psycopg2, psycopg, pg8000, asyncpg, psycopg2cffi."],
    }


def test_postgres_schema_ssl_not_required(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema when SSL it not required.
    """
    create_engine = mocker.patch("enginator.schemas.postgres.create_engine")

    postgres_schema = PostgresSchema()

    postgres_schema.get_engine(host="localhost", require_ssl=False)
    create_engine.assert_called_with(
        make_url("postgresql+psycopg2://localhost:5432?sslmode=prefer"),
    )


def test_postgres_schema_ssl_self_signed(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema when self-signed certificates are allowed.
    """
    create_engine = mocker.patch("enginator.schemas.postgres.create_engine")

    postgres_schema = PostgresSchema()

    postgres_schema.get_engine(
        host="localhost",
        allow_self_signed_certificates=True,
    )
    create_engine.assert_called_with(
        make_url("postgresql+psycopg2://localhost:5432?sslmode=require"),
    )


def test_postgres_schema_ssl_disable_hostname_checking(mocker: MockerFixture) -> None:
    """
    Test the Postgres schema when hostname checking is disabled.
    """
    create_engine = mocker.patch("enginator.schemas.postgres.create_engine")

    postgres_schema = PostgresSchema()

    postgres_schema.get_engine(
        host="localhost",
        disable_hostname_checking=True,
    )
    create_engine.assert_called_with(
        make_url("postgresql+psycopg2://localhost:5432?sslmode=verify-ca"),
    )


def test_build_ssl_value() -> None:
    """
    Test building the SSL value.
    """
    assert build_ssl_value({"require_ssl": False}) == "prefer"
    assert (
        build_ssl_value({"require_ssl": True, "allow_self_signed_certificates": True})
        == "require"
    )
    assert (
        build_ssl_value({"require_ssl": True, "disable_hostname_checking": True})
        == "verify-ca"
    )
    assert build_ssl_value({"require_ssl": True}) == "verify-full"


def test_build_ssl_context() -> None:
    """
    Test building the SSL context.
    """
    ssl_context = build_ssl_context({"require_ssl": False})
    assert ssl_context is None

    ssl_context = build_ssl_context(
        {
            "require_ssl": True,
            "allow_self_signed_certificates": True,
            "disable_hostname_checking": True,
        },
    )
    assert ssl_context is not None
    assert ssl_context.check_hostname is False
    assert ssl_context.verify_mode == ssl.CERT_NONE

    ssl_context = build_ssl_context({"require_ssl": True})
    assert ssl_context is not None
    assert ssl_context.check_hostname is True
    assert ssl_context.verify_mode == ssl.CERT_REQUIRED
