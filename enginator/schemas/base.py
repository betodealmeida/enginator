from typing import Any

from marshmallow import Schema, fields
from sqlalchemy.engine import Engine


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
                "The catalog name, also often called a database (Postgres) or project "
                "(BigQuery)."
            ),
        },
    )
    namespace = fields.String(
        required=False,
        allow_none=True,
        load_default=None,
        metadata={
            "description": (
                "The namespace name, also often called a schema (Postgres and most "
                "databases) or database (MySQL)."
            ),
        },
    )

    @classmethod
    def match(cls, engine: str, driver: str | None = None) -> bool:
        """
        Does the schema handle a given `engine[:driver]`?
        """
        return False

    @staticmethod
    def get_catalogs(engine: Engine) -> list[str]:
        """
        Return a list of catalogs available in the engine.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def get_namespaces(engine: Engine) -> list[str]:
        """
        Return a list of namespaces available in the engine.
        """
        raise NotImplementedError("Subclasses must implement this method.")

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
