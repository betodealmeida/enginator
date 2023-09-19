import re
import ssl
from enum import StrEnum
from typing import Any

from marshmallow import fields, post_load, validates_schema, ValidationError
from marshmallow.validate import Range
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.event import listens_for
from sqlalchemy.sql import text

from enginator.schemas.base import BaseSchema


class PostgresDriver(StrEnum):
    """
    Supported drivers for Postgres in SQLAlchemy.

    Different drivers have different validation.
    """

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

    engine = fields.Constant("postgresql")
    driver = fields.Enum(
        PostgresDriver,
        required=False,
        load_default=PostgresDriver.psycopg2,
    )

    # can we group these?
    username = fields.String(required=False, metadata={"description": "Username."})
    password = fields.String(required=False, metadata={"description": "Password."})
    host = fields.String(
        required=True,
        metadata={"description": "Hostname or IP address."},
    )
    port = fields.Integer(
        required=False,
        load_default=5432,
        validate=Range(min=0, max=2**16, max_inclusive=False),
        metadata={"description": "Port number."},
    )
    database = fields.String(required=False, metadata={"description": "Database name."})

    # can we group these?
    ssl = fields.Boolean(
        required=False,
        load_default=True,
        metadata={"description": "Use SSL."},
    )
    disable_hostname_checking = fields.Boolean(
        required=False,
        load_default=False,
        metadata={"description": "Disable hostname checking."},
    )
    allow_self_signed_certificates = fields.Boolean(
        required=False,
        load_default=False,
        metadata={"description": "Allow self-signed certificates."},
    )

    @staticmethod
    def get_catalogs(engine: Engine) -> list[str]:
        """
        Return a list of databases.
        """
        with engine.connect() as connection:
            return sorted(
                catalog
                for (catalog,) in connection.execute(
                    text("SELECT datname FROM pg_database WHERE datistemplate = false;")
                )
            )

    @staticmethod
    def get_namespaces(engine: Engine) -> list[str]:
        """
        Return a list of schemas.
        """
        with engine.connect() as connection:
            return sorted(
                schema
                for (schema,) in connection.execute(
                    text("SELECT schema_name FROM information_schema.schemata;")
                )
            )

    @classmethod
    def match(cls, engine: str, driver: str | None = None) -> bool:
        return engine == "postgresql" and (
            driver is None or driver in PostgresDriver.__members__.values()
        )

    @validates_schema
    def validate_ssl(self, data: dict[str, Any], **kwargs: Any) -> None:
        """
        Make sure we can enable SSL.
        """
        # XXX
        if data.get("ssl") and data["driver"] not in {
            PostgresDriver.psycopg2,
            PostgresDriver.pg8000,
        }:
            raise ValidationError("SSL is only supported with psycopg2 and pg8000.")

    @post_load
    def make_engine(self, data: dict[str, Any], **kwargs: Any) -> Engine:
        """
        Build the SQLAlchemy engine.
        """
        parameters = {}

        query = {}
        if data.get("ssl"):
            # XXX
            if data["driver"] == PostgresDriver.psycopg2:
                query["sslmode"] = "require"
            elif data["driver"] == PostgresDriver.pg8000:
                ssl_context = ssl.create_default_context()
                if data.get("disable_hostname_checking"):
                    ssl_context.check_hostname = False
                if data.get("allow_self_signed_certificates"):
                    ssl_context.verify_mode = ssl.CERT_NONE
                parameters["connect_args"] = {"ssl_context": ssl_context}

        url = URL(
            drivername="{engine}+{driver}".format(**data),
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
            def set_namespace(dbapi_con, connection_record) -> None:
                """
                Set the default namespace.
                """
                cursor = dbapi_con.cursor()
                cursor.execute(f'set search_path = "{namespace}"')

            @listens_for(engine, "before_cursor_execute")
            def disallow_namespace_change(
                conn,
                cursor,
                statement,
                parameters,
                context,
                executemany,
            ) -> None:
                r"""
                Check for statements altering the default namespace.

                We use a rather aggressive regular expression to check for statements
                that modify the search path. Ideally we'd strip comments and check for
                `set\s+search_path\s*=`, or even better, parse the query and analyze it.
                """
                if re.search("search_path", statement, re.IGNORECASE):
                    raise Exception(
                        "Queries modifying search_path are not allowed for security "
                        "reasons."
                    )

        return engine
