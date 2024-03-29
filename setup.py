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
                    '2015-09-24 00:00am'::date,
                    '1 hour'::interval) timestamp;''')
   conn.commit()

def create_locations_table(conn):
    cur = conn.cursor()
    cur.execute("create table Locations (FREGUESIA varchar(35), CONCELHO varchar(22), STAND_ID int, ID serial primary key);")
    conn.commit()

def fill_locations_table(conn):
    cur = conn.cursor()
    cur.execute('''insert into locations (freguesia, concelho)
                   values ('OUTSIDE', 'OUTSIDE')''')
    cur.execute('''insert into locations (freguesia, concelho)
                   select freguesia, concelho from porto;''')
    cur.execute('''insert into locations (freguesia, concelho, stand_id)
                   select freguesia, concelho, taxi_stands.id from taxi_stands, porto
                   where st_contains(porto.geom, taxi_stands.location);''')
    conn.commit()

def create_facts_table(conn):
    cur = conn.cursor()
    cur.execute("create table Facts (TIME int references timestamps(id), LOCATION int references locations(id), TAXI_ID int references taxis(id), N_SERVICES_INITIAL int, SUM_DURATION_INITIAL int, SUM_DISTANCE_INITIAL int, N_SERVICES_FINAL int, SUM_DURATION_FINAL int, SUM_DISTANCE_FINAL int);")
    cur.execute("create unique index idx_facts_time_location_taxi on facts(time, location, taxi_id);")

def find_stand(cur, service_id, initial):
    if initial:
        point = "initial"
    else:
        point = "final"

    cur.execute("select taxi_stands.id from taxi_stands, taxi_services where st_distancesphere(taxi_services.%s_point, taxi_stands.location) < 100 and taxi_services.id = %s;" % (point, service_id))
    return cur.fetchone()


def fill_facts_table(conn):
    cur = conn.cursor()
    services_cur = conn.cursor()
    # Select all the taxi services
    services_cur.execute("select taxis.id, initial_ts, final_ts, st_distancesphere(final_point, initial_point), taxi_services.id from taxi_services, taxis where taxi_id = taxis.license;");
    while 1:
        # Fetch 5000 each time
        services = services_cur.fetchmany(1000)
        if len(services) == 0:
            break

        for service in services:
            taxi_id = service[0]
            initial_timestamp = service[1]
            final_timestamp = service[2]
            distance = service[3]
            service_id = service[4]
            duration = final_timestamp - initial_timestamp

            # Calculate timestamp_id
            cur.execute("select timestamps.id from timestamps, taxi_services where date_trunc('hour', to_timestamp(initial_ts)) = timestamp and taxi_services.id = %s;" % (service_id))
            timestamp_id = cur.fetchone()[0]

            # Check if near stand
            stand = find_stand(cur, service_id, True)
            if stand:
                cur.execute("select id from locations where stand_id = %s;" % (stand[0]))
                location_id = cur.fetchone()[0]
            else:
                # If isn't near stand get location
                cur.execute("select locations.id from locations, taxi_services, porto where taxi_services.id = %s and st_contains(geom, initial_point) and locations.freguesia = porto.freguesia and locations.concelho = porto.concelho and locations.stand_id is null;" % service_id)
                location = cur.fetchone()
                if location:
                    location_id = location[0]
                else:
                    # If location not in Porto, location = OUTSIDE
                    location_id = 1

            # Insert in facts table if combination not present. Otherwise update the values
            cur.execute("insert into facts (time, location, taxi_id, n_services_initial, sum_duration_initial, sum_distance_initial, n_services_final, sum_duration_final, sum_distance_final) values (%d, %d, %d, 1, %d, %d, 0, 0, 0) on conflict(time, location, taxi_id) do update set n_services_initial = facts.n_services_initial + 1, sum_duration_initial = facts.sum_duration_initial + %d, sum_distance_initial = facts.sum_distance_initial + %d;" % (timestamp_id, location_id, taxi_id, duration, distance, duration, distance))

            # Repeat location calculation but for final point
            stand = find_stand(cur, service_id, False)
            if stand:
                cur.execute("select id from locations where stand_id = %s;" % (stand[0]))
                location_id = cur.fetchone()[0]
            else:
                cur.execute("select locations.id from locations, taxi_services, porto where taxi_services.id = %s and st_contains(geom, final_point) and locations.freguesia = porto.freguesia and locations.concelho = porto.concelho and locations.stand_id is null;" % service_id)
                location = cur.fetchone()
                if location:
                    location_id = location[0]
                else:
                    location_id = 1

            cur.execute("insert into facts (time, location, taxi_id, n_services_initial, sum_duration_initial, sum_distance_initial, n_services_final, sum_duration_final, sum_distance_final) values (%d, %d, %d, 0, 0, 0, 1, %d, %d) on conflict(time, location, taxi_id) do update set n_services_final = facts.n_services_final + 1, sum_duration_final = facts.sum_duration_final + %d, sum_distance_final = facts.sum_distance_final + %d;" % (timestamp_id, location_id, taxi_id, duration, distance, duration, distance))
        # Commit 5000 each time
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

def create_porto_map(conn):
    cur = conn.cursor()
    cur.execute("create table Porto as select * from map where distrito = 'PORTO';")
    conn.commit()


def create_porto_polygon(conn):
    cur = conn.cursor()
    cur.execute("create table polygon_porto (ID serial primary key);")
    cur.execute("SELECT AddGeometryColumn('','polygon_porto','geom','4326','MULTIPOLYGON',2);")
    cur.execute('''insert into polygon_porto (geom)
                (select st_multi(st_union(f.geom)) FROM porto as f)''')


def create_tables(conn):
    create_time_table(conn)
    create_stands_table(conn)
    create_locations_table(conn)
    create_taxis_table(conn)
    create_facts_table(conn)

def drop_tables(conn):
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS facts;")
        cur.execute("DROP TABLE IF EXISTS stands;")
        cur.execute("DROP TABLE IF EXISTS timestamps;")
        cur.execute("DROP TABLE IF EXISTS locations;")
        cur.execute("DROP TABLE IF EXISTS taxis;")
        cur.execute("DROP TABLE IF EXISTS porto;")
        cur.execute("DROP TABLE IF EXISTS polygon_porto;")
        conn.commit()
        print("Deleted tables")
    except:
        print("Can't drop\n")

def clean_tables(conn):
    cur = conn.cursor()
    print("Cleaning tables")
    # Delete services that start and finish outside Porto district
    cur.execute('''DELETE from taxi_services
                using polygon_porto
                where not st_contains(geom, initial_point)
                and not st_contains(geom, final_point)''')
    # Delete services that have a speed lower than 3 Km/h or less than one minute
    cur.execute('''DELETE from taxi_services
                where (final_ts - initial_ts) < 60
                OR (st_distancesphere(initial_point, final_point)/1000)
                / ((final_ts - initial_ts)::float/3600) < 3;''')
    conn.commit()
    print("Tables cleaned")

#if __name__ == "__main__":
    #if(len(sys.argv) != 3):
        #print("Wrong number of arguments")
        #exit(0)
#
    #user = sys.argv[1]
    #dbname = sys.argv[2]
    #conn = psy.connect("dbname=%s user=%s" % (dbname, user))
#
#
    #drop_tables(conn)
    #create_porto_map(conn)
    #create_porto_polygon(conn)
    #clean_tables(conn)
    #create_tables(conn)
#
    #fill_time_table(conn)
    #fill_locations_table(conn)
    #fill_taxis_table(conn)
    #fill_facts_table(conn)
