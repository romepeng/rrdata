#Example of a callable using PostgreSQL COPY clause:

# Alternative to_sql() *method* for DBs that support COPY FROM
import csv
from io import StringIO
import psycopg2

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted (list, dict, tuple(),set(), file,)
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        print(columns)
        
        #if table.schema:
        #    table_name = '{}.{}'.format(table.schema, table.name)
        #else:
        #    table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table, columns)
        cur.copy_expert(sql=sql, file=s_buf)
        
if  __name__ == "__main__":
    from rrdata.common.engine_pgsql import engine
    conn = psycopg2.connect(user='postgres',
                                    database='rrdata',
                                    password='Mysqlpd1219.',
                                    host='39.108.68.163', 
                                    port=5432)
    psql_insert_copy(table="upsert_test", conn=conn, keys=['id','updated_at'], data_iter=['30', "2017-03-23 21:50:05"])
    
    