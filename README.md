# enginator

## A library that helps creating SQLAlchemy engines.

Creating a single [SQLAlchemy engine](https://docs.sqlalchemy.org/en/20/core/engines.html) is easy; allowing your users to create an engine to a database from a list of dozens of database types, not as much. Each database type has its own way of connecting, requiring SSL, setting up credentials, etc., requiring custom logic that is not abstracted by SQLAlchemy. Additionally, there's no standardized way for navigating database hierarchies: switching to a different namespace* or catalog* at runtime, to execute a query on a different Postgres schema or BigQuery project.

This library offers that abstraction, allowing developers to easily build UIs for connecting to different databases, and providing an abstraction to different database levels.

### A note on terminology

Database terminology is confusing. A Postgres installation has multiple databases; each database has multiple schemas; each schema, multiple tables. Tables have schemas, but these are different from the schemas databases have. What BigQuery calls a "project" is a "catalog" for Trino; what MySQL calls a "database" is a "schema" for Postgres. And so on.

We use the following terminology in this project:

| enginator | Postgres |  MySQL   | BigQuery |  Trino  |
| --------- | -------- | -------- | -------- | ------- |
| database  | cluster  | ?        | ?        | ?       |
| catalog   | database | N/A      | project  | catalog |
| namespace | schema   | database | schema   | schema  |
| table     | table    | table    | table    | table   |
| column    | column   | column   | column   | column  |

## Usage

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
