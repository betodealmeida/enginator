"""
PostgreSQL engine schema.
"""

import ssl
from enum import StrEnum
from typing import Any

from marshmallow import fields
from marshmallow.validate import Range
from sqlalchemy.engine import Connection, Engine, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.event import listens_for
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.pool import _ConnectionRecord
from sqlalchemy.sql import text

from enginator.lib import get_settings
from enginator.schemas.base import BaseSchema


def build_ssl_value(data: dict[str, Any]) -> str:
    """
    Build the SSL configuration for the connection.
    """
    require_ssl = data.get("require_ssl", True)
    allow_self_signed_certificates = data.get("allow_self_signed_certificates", False)
    disable_hostname_checking = data.get("disable_hostname_checking", False)

    if not require_ssl:
        return "prefer"

    if allow_self_signed_certificates:
        return "require"

    if disable_hostname_checking:
        return "verify-ca"

    return "verify-full"


def build_ssl_context(data: dict[str, Any]) -> ssl.SSLContext | None:
    """
    Build the SSL context for the connection.
    """
    if not data.get("require_ssl"):
        return None

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = not data.get("disable_hostname_checking")

    if data.get("allow_self_signed_certificates"):
        ssl_context.verify_mode = ssl.CERT_NONE

    return ssl_context


class PostgresDriver(StrEnum):
    """
    Supported drivers for Postgres in SQLAlchemy.

    Different drivers have different validation.
    """

    # pylint: disable=invalid-name
    psycopg2 = "psycopg2"
    psycopg = "psycopg"
    pg8000 = "pg8000"
    asyncpg = "asyncpg"
    psycopg2cffi = "psycopg2cffi"


class PostgresSchema(BaseSchema):
    """
    PostgreSQL schema.
    """

    name = "PostgreSQL"

    hierarchy_map = {
        "catalog": "database",
        "namespace": "schema",
    }

    engine = fields.Constant(
        "postgresql",
        metadata={
            "description": "Engine name",
            "type": "string",
            "x-ui-schema": {"ui:readonly": True},
        },
    )
    driver = fields.Enum(
        PostgresDriver,
        required=False,
        load_default=PostgresDriver.psycopg2,
        metadata={"description": "Database driver"},
    )

    username = fields.String(required=False, metadata={"description": "Username"})
    password = fields.String(
        required=False,
        metadata={"description": "Password", "x-ui-schema": {"ui:widget": "password"}},
    )
    host = fields.String(
        required=True,
        metadata={"description": "Hostname or IP address"},
    )
    port = fields.Integer(
        required=False,
        load_default=5432,
        validate=Range(min=0, max=2**16, max_inclusive=False),
        metadata={"description": "Port number"},
    )
    database = fields.String(required=False, metadata={"description": "Database name"})

    require_ssl = fields.Boolean(
        required=False,
        load_default=True,
        metadata={
            "description": "Require SSL for the connection",
            "title": "Require SSL",
        },
    )
    disable_hostname_checking = fields.Boolean(
        required=False,
        load_default=False,
        metadata={"description": "Disable hostname checking"},
    )
    allow_self_signed_certificates = fields.Boolean(
        required=False,
        load_default=False,
        metadata={"description": "Allow self-signed certificates"},
    )

    @staticmethod
    def get_catalogs(engine: Engine) -> set[str]:
        """
        Return a list of databases.
        """
        with engine.connect() as connection:
            return {
                catalog
                for (catalog,) in connection.execute(
                    text(
                        "SELECT datname FROM pg_database WHERE datistemplate = false;",
                    ),
                )
            }

    @staticmethod
    def get_namespaces(engine: Engine) -> set[str]:
        """
        Return a list of schemas.
        """
        with engine.connect() as connection:
            return {
                schema
                for (schema,) in connection.execute(
                    text("SELECT schema_name FROM information_schema.schemata;"),
                )
            }

    @classmethod
    def match(cls, engine: str, driver: str | None = None) -> bool:
        return engine == "postgresql" and (
            driver is None or driver in PostgresDriver.__members__.values()
        )

    def make_engine(  # pylint: disable=unused-argument
        self,
        data: dict[str, Any],
        **kwargs: Any,
    ) -> Engine:
        """
        Build the SQLAlchemy engine.
        """
        parameters = {}
        query = {}

        if data["driver"] == PostgresDriver.asyncpg:
            parameters["connect_args"] = {"ssl": build_ssl_context(data)}
        elif data["driver"] in {
            PostgresDriver.psycopg,
            PostgresDriver.psycopg2,
            PostgresDriver.psycopg2cffi,
        }:
            query["sslmode"] = build_ssl_value(data)
        elif data["driver"] == PostgresDriver.pg8000:
            parameters["connect_args"] = {"ssl_context": build_ssl_context(data)}
        else:
            # should never happen due to Marshmallow validation
            raise ValueError(f"Invalid driver: {data['driver']}")  # pragma: no cover

        url = URL(
            drivername=f"{data['engine']}+{data['driver']}",
            username=data.get("username"),
            password=data.get("password"),
            host=data["host"],
            port=data["port"],
            database=data.get("catalog") or data.get("database"),
            query=query,
        )

        engine = create_engine(url, **parameters)

        if namespace := data.get("namespace"):

            @listens_for(engine, "connect")
            def set_namespace(  # pylint: disable=unused-argument
                dbapi_con: Any,
                connection_record: _ConnectionRecord,
            ) -> None:
                """
                Set the default namespace.
                """
                cursor = dbapi_con.cursor()
                cursor.execute(f'set search_path = "{namespace}"')

            @listens_for(engine, "before_cursor_execute")
            def disallow_namespace_change(  # pylint: disable=too-many-arguments,too-many-positional-arguments,unused-argument
                conn: Connection,
                cursor: Any,
                statement: str,
                parameters: Any,
                context: Any,
                executemany: bool,
            ) -> None:
                r"""
                Check for statements altering the default namespace.
                """
                settings = get_settings(statement, data["engine"])
                search_path = settings.get("search_path")

                if not search_path:
                    return

                if isinstance(search_path, str) and search_path.strip('"') != namespace:
                    raise ProgrammingError(
                        statement,
                        parameters,
                        "Queries modifying `search_path` are not allowed since a "
                        "default namespace has been set. Please use fully qualified "
                        "names or change the default namespace for the connection.",
                    )

        return engine
