import psycopg2 as psy

def create_time_table(conn):
    cur = conn.cursor()
    cur.execute("create table Timestamps ( TIMESTAMP timestamp, ID serial primary key);")
    conn.commit()

def fill_time_table(conn):
   cur = conn.cursor()
   cur.execute('''insert into timestamps
                select timestamp from generate_series(
                  '2015-01-01 12:00am'::date,
                  '2015-09-23 11:00pm'::date,
                  '1 hour'::interval) timestamp;''')
   conn.commit()

if __name__ == "__main__":
    conn = psy.connect("dbname=guest user=guest")
    create_time_table(conn)
    fill_time_table(conn)
