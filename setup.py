import psycopg2 as psy
import sys

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
    cur.execute('''insert into locations (freguesia, concelho, stand_id)
                   select freguesia, concelho, taxi_stands.id from taxi_stands, map
                   where st_contains(map.geom, taxi_stands.location);''')
    conn.commit()

def create_taxis_table(conn):
    cur = conn.cursor()
    cur.execute("create table Taxis (ID serial primary key, LICENSE integer);")
    conn.commit()

def fill_taxis_table(conn):
    cur = conn.cursor()
    cur.execute("insert into taxis (license) select distinct taxi_id from taxi_services;")
    conn.commit()

def create_stands_table(conn):
    cur = conn.cursor()
    cur.execute("select id, name into Stands from taxi_stands;")
    conn.commit()


def create_tables(conn):
    create_time_table(conn)
    create_locations_table(conn)
    create_stands_table(conn)
    create_taxis_table(conn)

if __name__ == "__main__":
    print(sys.argv)
    if(len(sys.argv) != 3):
        print("Wrong number of arguments")
        exit(0)

    user = sys.argv[1]
    dbname = sys.argv[2]
    conn = psy.connect("dbname=%s user=%s" % (dbname, user))


    create_tables(conn)
    fill_time_table(conn)
    fill_locations_table(conn)
    fill_taxis_table(conn)
