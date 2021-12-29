import trino
from getpass import getpass

# Opening connection with Datalake
def datalake_connection(auth, trino_username, trino_catalog, trino_schema, password=None):
    password = getpass()
    print(password)

    if password != '': 
        conn = trino.dbapi.connect(
            host='localhost',
            port=8080,
            user=trino_username,
            catalog=trino_catalog,
            schema=trino_schema,
            http_scheme='https',
            auth=trino.auth.BasicAuthentication(trino_username, password),
        )
    else:
        conn = trino.dbapi.connect(
            host='localhost',
            port=8080,
            user=trino_username,
            catalog=trino_catalog,
            schema=trino_schema,
        )

    cur = conn.cursor()
    return cur

# Get all the catalogs available.
def datalake_get_catalogs(auth, trino_username, trino_catalog, trino_schema):
    cur = datalake_connection(auth, trino_username, trino_catalog, trino_schema)
    cur.execute(f'SHOW catalogs')
    rows = cur.fetchall()
    return rows

# Get Schemas from a specific catalog.
def datalake_get_schemas(auth, trino_username, trino_catalog, trino_schema):
    cur = datalake_connection(auth, trino_username, trino_catalog, trino_schema)
    cur.execute(f'SHOW SCHEMAS FROM {trino_catalog}')
    rows = cur.fetchall()
    return rows

# Get all tables from a schema.
def datalake_get_tables(auth, trino_username, trino_catalog, trino_schema):
    cur = datalake_connection(auth, trino_username, trino_catalog, trino_schema)
    cur.execute(f'SHOW TABLES FROM {trino_catalog}.{trino_schema}')
    rows = cur.fetchall()
    return rows


if __name__ == '__main__':
    rows = datalake_get_catalogs(auth=False, trino_username='admin', trino_catalog='system', trino_schema='runtime')