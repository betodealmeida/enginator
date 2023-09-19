import inspect
import re
import ssl
from enum import StrEnum
from typing import Any

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from marshmallow import Schema, fields, post_load, validates_schema, ValidationError
from marshmallow.validate import Range
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.event import listens_for

from sqlalchemy_url_builder import __version__


class BaseSchema(Schema):
    name: str

    # Basic attributes that every DB schema should have.
    engine = fields.String(
        required=True,
        metadata={"description": "SQLAlchemy engine."},
    )
    driver = fields.String(
        required=True,
        metadata={"description": "SQLAlchemy driver."},
    )

    # Different databases use different names for the organizational hierarchies. For
    # example, a Postgres "database" is a Presto "catalog" and a BigQuery "project";
    # a Postgres "schema" is a MySQL "database". The base class provides a standard
    # nomenclature for this as:
    #
    #     database => catalog => namespace => table => column
    #
    # When instantiating engines users can use either the standard name or the
    # database specific one.
    catalog = fields.String(
        required=False,
        allow_none=True,
        load_default=None,
        metadata={
            "description": (
                "The catalog name, also often called a database (Postgres, eg) or "
                "project (BigQuery, eg)"
            ),
        },
    )
    namespace = fields.String(
        required=False,
        allow_none=True,
        load_default=None,
        metadata={
            "description": (
                "The namespace name, also often called a schema (Postgres, eg) or "
                "database (MySQL, eg)"
            ),
        },
    )

    @classmethod
    def match(cls, engine: str, driver: str | None = None) -> bool:
        """
        Does the schema handle a given `engine[:driver]`?
        """
        return False

    def get_engine(
        self,
        catalog: str | None = None,
        namespace: str | None = None,
        **kwargs: Any,
    ) -> Engine:
        """
        Return the engine optionally configured for a given catalog and namespace.
        """
        if catalog:
            kwargs["catalog"] = catalog
        if namespace:
            kwargs["namespace"] = namespace

        return self.load(data=kwargs)


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


class GoogleServiceAccountInfoSchema(Schema):
    """
    Information about a Google service account.
    """

    type = fields.Constant("service_account")
    project_id = fields.String(required=True)
    private_key_id = fields.String(required=True)
    private_key = fields.String(required=True)
    client_email = fields.Email(required=True)
    client_id = fields.String(required=True)
    auth_uri = fields.Url(required=True)
    token_uri = fields.Url(required=True)
    auth_provider_x509_cert_url = fields.Url(required=True)
    client_x509_cert_url = fields.Url(required=True)


class GSheetsSchema(BaseSchema):
    """
    Google Sheets schema.

    The pseudo-database takes no arguments other than the authentication information.
    """

    name = "Google Sheets"

    engine = fields.Constant("gsheets")
    driver = fields.Constant("apsw")

    # auth
    access_token = fields.String(
        required=False,
        metadata={"description": "OAuth2 access token."},
    )
    service_account_file = fields.String(
        required=False,
        metadata={"description": "Path to service account JSON file."},
    )
    service_account_info = fields.Nested(
        GoogleServiceAccountInfoSchema,
        required=False,
        metadata={"description": "Contents of service account JSON file."},
    )

    subject = fields.String(required=False)
    app_default_credentials = fields.Boolean(required=False)

    @classmethod
    def match(cls, engine: str, driver: str | None = None) -> bool:
        return engine == "gsheets" and (driver is None or driver == "apsw")

    @post_load
    def make_engine(self, data: dict[str, Any], **kwargs: Any) -> Engine:
        """
        Build the SQLAlchemy engine.
        """
        parameters = {
            k: v
            for k, v in data.items()
            if k not in {"engine", "driver", "catalog", "namespace"}
        }
        url = URL(
            drivername="{engine}+{driver}".format(**data),
            username=None,
            password=None,
            host=None,
            port=None,
            database=None,
            query=None,
        )

        return create_engine(url, **parameters)


def build_spec() -> APISpec:
    """
    Build the OpenAPI spec.
    """
    spec = APISpec(
        title="SQLAlchemy URL Builder",
        version=__version__,
        openapi_version="3.0.2",
        plugins=[MarshmallowPlugin()],
    )

    for obj in globals().copy().values():
        if inspect.isclass(obj) and issubclass(obj, Schema) and obj != BaseSchema:
            spec.components.schema(obj.__name__, schema=obj)

    return spec


def get_engine(data: dict[str, Any]) -> Engine:
    """
    Return an engine given a raw payload.
    """


if __name__ == "__main__":
    import json

    print(json.dumps(build_spec().to_dict()))
