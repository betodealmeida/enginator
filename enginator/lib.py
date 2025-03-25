import inspect
from importlib.metadata import entry_points
from typing import Any

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from sqlalchemy.engine import Engine
from sqlglot import exp, parse

from enginator import __version__
from enginator.constants import DIALECTS


def build_spec() -> APISpec:
    """
    Build the OpenAPI spec.

    :return: OpenAPI spec
    """
    spec = APISpec(
        title="SQLAlchemy URL Builder",
        version=__version__,
        openapi_version="3.0.2",
        plugins=[MarshmallowPlugin()],
    )

    for ep in entry_points(group="enginator.schemas"):
        try:
            klass = ep.load()
            schema = klass()
            spec.components.schema(klass.__name__, schema=schema)
        except Exception:
            pass

    return spec


def get_engine(data: dict[str, Any]) -> Engine:
    """
    Return an engine given a raw payload.

    :param data: raw payload
    :return: SQLAlchemy engine
    """
    engine = data.get("engine")
    driver = data.get("driver")

    for ep in entry_points(group="enginator.schemas"):
        try:
            klass = ep.load()
            if klass.match(engine, driver):
                return klass.get_engine(**data)
        except Exception:
            pass

    raise ValueError("No schema found for this engine.")


def get_settings(sql: str, engine: str) -> dict[str, str | bool]:
    """
    Return settings from a SQL script.

        >>> get_settings("SET foo = 'bar'")
        {'foo': "'bar'"}

    :param sql: SQL script
    :return: settings dictionary
    """
    dialect = DIALECTS.get(engine)

    return {
        eq.this.sql(comments=False): eq.expression.sql(comments=False)
        for statement in parse(sql, dialect=dialect)
        for set_item in statement.find_all(exp.SetItem)
        for eq in set_item.find_all(exp.EQ)
    }
