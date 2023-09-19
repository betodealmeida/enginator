# sqlalchemy-url-builder

A library that helps building URLs for SQLAlchemy engines:

```python
from sqlalchemy_url_builder.schemas import PostgresSchema


data = {
    "engine": "postgresql",
    "driver": "psycopg2",
    "host": "localhost",
    "port": 5432,
    "database": "master",
    "ssl": False,
}

# connect to the `examples` database (instead of `master`), and set default search path to
# `information_schema`
engine = PostgresSchema().get_engine(
    catalog="examples",
    namespace="information_schema",
    **data,
)

with engine.connect() as conn:
    print(conn.execute(text("SELECT * FROM columns")).fetchall())
```
