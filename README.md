# enginator

A library that helps creating SQLAlchemy engines:

```python
# `PostgresSchema` is a Marshmallow schema, so it will perform validation of the
# parameters, as well as exposing them in an OpenAPI 3.0 spec for UI construction.
from enginator.schemas import PostgresSchema


schema = PostgresSchema()

# Payload with all the information needed to connect -- some of these attributes will be
# used to build the SQLAlchemy URL, others to build extra engine arguments:
data = {
    "engine": "postgresql",
    "driver": "psycopg2",
    "host": "localhost",
    "port": 5432,
    "database": "superset",
    "ssl": False,
}

# Load data; these two are equivalent:
engine = schema.get_engine(**data)
engine = schema.load(data=data)

# The library also offers a standard way of overriding the "catalog" ("database", in
# Postgres) and the "namespace" ("schema", in Postgres), as well as methods to list
# them:
catalogs = schema.get_catalogs(engine)  # ["superset", "examples"]
engine = schema.get_engine(catalog="examples", **data)
namespaces = schema.get_namespaces(engine)  # ["public", "information_schema"]

# Connect to database "examples", and set the search path to "information_schema":
engine = schema.get_engine(
    catalog="examples",
    namespace="information_schema",
    **data,
)
```
