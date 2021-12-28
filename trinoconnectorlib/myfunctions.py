import trino

def datalake_connection(auth, trino_username, trino_catalog, sdp_schema, trino_password):
    conn = trino.dbapi.connect(
        host='localhost',
        port=8080,
        user=trino_username,
        catalog=trino_catalog,
        schema=sdp_schema,
    )
    cur = conn.cursor()
    return cur

def datalake_connection_user(auth=False, trino_username=None, trino_catalog=None, sdp_schema=None, trino_password=None):
    cur = datalake_connection(auth, trino_username, trino_catalog, sdp_schema, trino_password)
    cur.execute('SELECT * FROM system.runtime.nodes')
    rows = cur.fetchall()
    return rows


if __name__ == '__main__':
    rows = datalake_connection_user(True, "admin", "system", "runtime")
    print(rows)
