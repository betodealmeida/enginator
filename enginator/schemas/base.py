"""
Base class for engine schemas.
"""

from typing import Any

from marshmallow import Schema, fields, post_load, pre_load
from sqlalchemy import inspect
from sqlalchemy.engine import Engine


class BaseSchema(Schema):
    """
    Base class for engine schemas.
    """

    name: str

    hierarchy_map: dict[str, str] = {}

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
    # database specific one (via the `hierarchy_map` class attribute).
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

    @pre_load
    def handle_specific_names(  # pylint: disable=unused-argument
        self,
        data: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Handle specific names for catalogs and namespaces.
        """
        for standard_name, native_name in self.hierarchy_map.items():
            if native_name in data:
                data[standard_name] = data.pop(native_name)

        return data

    @classmethod
    def match(cls, engine: str, driver: str | None = None) -> bool:
        """
        Does the schema handle a given `engine[:driver]`?
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def get_catalogs(engine: Engine) -> set[str]:
        """
        Return a list of catalogs available in the engine.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def get_namespaces(engine: Engine) -> set[str]:
        """
        Return a list of namespaces available in the engine.
        """
        inspector = inspect(engine)
        return set(inspector.get_schema_names())

    def get_engine(self, **kwargs: Any) -> Engine:
        """
        Return the engine optionally configured for a given catalog and namespace.
        """
        return self.load(data=kwargs)

    @post_load
    def _make_engine(  # pylint: disable=unused-argument
        self,
        data: dict[str, Any],
        **kwargs: Any,
    ) -> Engine:
        """
        Make an engine from the schema data.
        """
        return self.make_engine(data)

    def make_engine(self, data: dict[str, Any]) -> Engine:
        """
        Actual implementation of `_make_engine`.
        """
        raise NotImplementedError("Subclasses must implement this method.")
