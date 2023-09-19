from typing import Any

from marshmallow import Schema, fields, post_load
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.engine.url import URL

from enginator.schemas.base import BaseSchema


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
