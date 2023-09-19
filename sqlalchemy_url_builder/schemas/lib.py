import inspect
from typing import Any

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from sqlalchemy.engine import Engine

from sqlalchemy_url_builder import __version__
from sqlalchemy_url_builder.schemas.base import BaseSchema



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
        if inspect.isclass(obj) and issubclass(obj, BaseSchema) and obj != BaseSchema:
            spec.components.schema(obj.__name__, schema=obj)

    return spec


def get_engine(data: dict[str, Any]) -> Engine:
    """
    Return an engine given a raw payload.
    """
