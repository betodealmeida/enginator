"""
Google Sheets engine schema.
"""

from enum import StrEnum
from typing import Any

from marshmallow import Schema, fields, post_load
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.engine.url import URL

from enginator.schemas.base import BaseSchema


class GSheetsDriver(StrEnum):
    """
    Google Sheets drivers.
    """

    APSW = "apsw"


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

    engine = fields.Constant(
        "gsheets",
        metadata={
            "description": "Engine name",
            "type": "string",
            "x-ui-schema": {"ui:readonly": True},
        },
    )
    driver = fields.Enum(
        GSheetsDriver,
        required=False,
        load_default=GSheetsDriver.APSW,
        metadata={"description": "Database driver"},
    )

    # auth
    access_token = fields.String(
        required=False,
        metadata={"description": "OAuth2 access token"},
    )
    service_account_file = fields.String(
        required=False,
        metadata={"description": "Path to service account JSON file"},
    )
    service_account_info = fields.Nested(
        GoogleServiceAccountInfoSchema,
        required=False,
        metadata={"description": "Contents of service account JSON file"},
    )

    subject = fields.String(required=False)
    app_default_credentials = fields.Boolean(required=False)

    @staticmethod
    def get_catalogs(engine: Engine) -> list[str]:
        """
        Return a list of catalogs available in the engine.
        """
        return []

    @staticmethod
    def get_namespaces(engine: Engine) -> list[str]:
        """
        Return a list of namespaces available in the engine.
        """
        return ["main"]

    @classmethod
    def match(cls, engine: str, driver: str | None = None) -> bool:
        return engine == "gsheets" and (driver is None or driver == "apsw")

    @post_load
    def make_engine(  # pylint: disable=unused-argument
        self,
        data: dict[str, Any],
        **kwargs: Any,
    ) -> Engine:
        """
        Build the SQLAlchemy engine.
        """
        parameters = {
            k: v
            for k, v in data.items()
            if k not in {"engine", "driver", "catalog", "namespace"}
        }
        url = URL(
            drivername=f"{data['engine']}+{data['driver']}",
            username=None,
            password=None,
            host=None,
            port=None,
            database=None,
            query=None,
        )

        return create_engine(url, **parameters)
