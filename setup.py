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

def create_locations_table(conn):
    cur = conn.cursor()
    cur.execute("create table Locations (FREGUESIA varchar(35), CONCELHO varchar(22), STAND_ID int, ID serial primary key);")
    conn.commit()

def fill_locations_table(conn):
    cur = conn.cursor()
    cur.execute('''insert into locations (freguesia, concelho)
                   select freguesia, concelho from map
                   where distrito = 'PORTO';''')
    conn.commit()

def create_taxis_table(conn):
    cur = conn.cursor()
    cur.execute("create table Taxis (ID serial primary key, LICENSE integer);")
    conn.commit()

def create_stands_table(conn):
    cur = conn.cursor()
    cur.execute("create table Stands (ID serial primary key, NAME varchar(255));")

def create_tables(conn):
    create_time_table(conn)
    create_locations_table(conn)
    create_stands_table(conn)
    create_taxis_table(conn)

if __name__ == "__main__":
    conn = psy.connect("dbname=guest user=guest")

    create_tables(conn)
    fill_time_table(conn)

    fill_locations_table(conn)
