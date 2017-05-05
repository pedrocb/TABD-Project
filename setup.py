import psycopg2 as psy

conn = psy.connect("dbname=guest user=guest")

def create_time_table(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE TEMPO ()")
