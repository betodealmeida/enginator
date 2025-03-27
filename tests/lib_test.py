"""
Tests for the helper functions.
"""

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine.url import URL

from enginator.lib import build_spec, get_engine, get_settings

postgres_data = {
    "engine": "postgresql",
    "driver": "psycopg2",
    "database": "mydb",
    "schema": "public",
    "port": 5432,
    "host": "localhost",
    "username": "myuser",
    "password": "mypassword",
}


def test_build_spec() -> None:
    """
    Test the `build_spec` function.
    """
    assert build_spec().to_dict() == {
        "paths": {},
        "info": {"title": "SQLAlchemy engine builder", "version": "0.0.1"},
        "openapi": "3.0.2",
        "components": {
            "schemas": {
                "GoogleServiceAccountInfo": {
                    "type": "object",
                    "properties": {
                        "type": {"default": "service_account"},
                        "project_id": {"type": "string"},
                        "private_key_id": {"type": "string"},
                        "private_key": {"type": "string"},
                        "client_email": {"type": "string", "format": "email"},
                        "client_id": {"type": "string"},
                        "auth_uri": {"type": "string", "format": "url"},
                        "token_uri": {"type": "string", "format": "url"},
                        "auth_provider_x509_cert_url": {
                            "type": "string",
                            "format": "url",
                        },
                        "client_x509_cert_url": {"type": "string", "format": "url"},
                    },
                    "required": [
                        "auth_provider_x509_cert_url",
                        "auth_uri",
                        "client_email",
                        "client_id",
                        "client_x509_cert_url",
                        "private_key",
                        "private_key_id",
                        "project_id",
                        "token_uri",
                    ],
                },
                "GSheetsSchema": {
                    "type": "object",
                    "properties": {
                        "engine": {
                            "default": "gsheets",
                            "description": "Engine name",
                            "type": "string",
                            "x-ui-schema": {"ui:readonly": True},
                        },
                        "driver": {
                            "default": "apsw",
                            "description": "Database driver",
                            "type": "string",
                            "enum": ["apsw"],
                        },
                        "catalog": {
                            "type": "string",
                            "default": None,
                            "description": (
                                "The catalog name, also often called a database "
                                "(Postgres) or project (BigQuery)."
                            ),
                            "nullable": True,
                        },
                        "namespace": {
                            "type": "string",
                            "default": None,
                            "description": (
                                "The namespace name, also often called a schema "
                                "(Postgres and most databases) or database (MySQL)."
                            ),
                            "nullable": True,
                        },
                        "access_token": {
                            "type": "string",
                            "description": "OAuth2 access token",
                        },
                        "service_account_file": {
                            "type": "string",
                            "description": "Path to service account JSON file",
                        },
                        "service_account_info": {
                            "description": "Contents of service account JSON file",
                            "allOf": [
                                {
                                    "$ref": "#/components/schemas/GoogleServiceAccountInfo",
                                },
                            ],
                        },
                        "subject": {"type": "string"},
                        "app_default_credentials": {"type": "boolean"},
                    },
                },
                "PostgresSchema": {
                    "type": "object",
                    "properties": {
                        "engine": {
                            "default": "postgresql",
                            "description": "Engine name",
                            "type": "string",
                            "x-ui-schema": {"ui:readonly": True},
                        },
                        "driver": {
                            "default": "psycopg2",
                            "description": "Database driver",
                            "type": "string",
                            "enum": [
                                "psycopg2",
                                "psycopg",
                                "pg8000",
                                "asyncpg",
                                "psycopg2cffi",
                            ],
                        },
                        "catalog": {
                            "type": "string",
                            "default": None,
                            "description": (
                                "The catalog name, also often called a database "
                                "(Postgres) or project (BigQuery)."
                            ),
                            "nullable": True,
                        },
                        "namespace": {
                            "type": "string",
                            "default": None,
                            "description": (
                                "The namespace name, also often called a schema "
                                "(Postgres and most databases) or database (MySQL)."
                            ),
                            "nullable": True,
                        },
                        "username": {"type": "string", "description": "Username"},
                        "password": {"type": "string", "description": "Password"},
                        "host": {
                            "type": "string",
                            "description": "Hostname or IP address",
                        },
                        "port": {
                            "type": "integer",
                            "default": 5432,
                            "minimum": 0,
                            "maximum": 65536,
                            "description": "Port number",
                        },
                        "database": {"type": "string", "description": "Database name"},
                        "require_ssl": {
                            "type": "boolean",
                            "default": True,
                            "description": "Require SSL for the connection",
                            "title": "Require SSL",
                        },
                        "disable_hostname_checking": {
                            "type": "boolean",
                            "default": False,
                            "description": "Disable hostname checking",
                        },
                        "allow_self_signed_certificates": {
                            "type": "boolean",
                            "default": False,
                            "description": "Allow self-signed certificates",
                        },
                    },
                    "required": ["host"],
                },
            },
        },
    }


def test_build_spec_exception_on_load(mocker: MockerFixture) -> None:
    """
    Test the `build_spec` function when an exception occurs on entry point load.
    """
    APISpec = mocker.patch("enginator.lib.APISpec")  # pylint: disable=invalid-name
    spec = APISpec()
    good_entry_point = mocker.MagicMock()
    good_entry_point.load.return_value = mocker.MagicMock(__name__="good")
    schema = good_entry_point.load()()
    bad_entry_point = mocker.MagicMock()
    bad_entry_point.name = "bad"
    bad_entry_point.load.side_effect = Exception("Bad entry point")
    mocker.patch(
        "enginator.lib.entry_points",
        return_value=[bad_entry_point, good_entry_point],
    )
    logger = mocker.patch("enginator.lib.logger")

    build_spec()

    spec.components.schema.assert_called_with("good", schema=schema)
    logger.warning.assert_called_with("Error loading schema %s", "bad")


def test_get_engine(mocker: MockerFixture) -> None:
    """
    Test the `get_engine` function.
    """
    create_engine = mocker.patch("enginator.schemas.postgres.create_engine")
    listens_for = mocker.patch("enginator.schemas.postgres.listens_for")

    get_engine(postgres_data)
    create_engine.assert_called_with(
        URL(
            drivername="postgresql+psycopg2",
            username="myuser",
            password="mypassword",
            host="localhost",
            port=5432,
            database="mydb",
            query={"sslmode": "verify-full"},
        ),
    )
    listens_for.assert_called()


def test_get_engine_invalid_engine() -> None:
    """
    Test the `get_engine` function with an invalid engine.
    """
    with pytest.raises(ValueError) as excinfo:
        get_engine({})
    assert str(excinfo.value) == "No schema found for this engine."


def test_engine_exception_on_load(mocker: MockerFixture) -> None:
    """
    Test the `get_engine` function when an exception occurs on entry point load.
    """
    good_entry_point = mocker.MagicMock()
    good_entry_point.load.return_value = mocker.MagicMock(__name__="good")
    good_entry_point.match.return_value = True
    schema = good_entry_point.load()()
    bad_entry_point = mocker.MagicMock()
    bad_entry_point.name = "bad"
    bad_entry_point.load.side_effect = Exception("Bad entry point")
    mocker.patch(
        "enginator.lib.entry_points",
        return_value=[bad_entry_point, good_entry_point],
    )
    logger = mocker.patch("enginator.lib.logger")

    get_engine(postgres_data)

    schema.get_engine.assert_called_with(**postgres_data)
    logger.warning.assert_called_with("Error loading schema %s", "bad")


def test_get_settings() -> None:
    """
    Test the `get_settings` function.
    """
    sql = """
        SET search_path TO schema_name;
        SELECT 1;
    """
    assert get_settings(sql, "postgresql") == {"search_path": "schema_name"}
