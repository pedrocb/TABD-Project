shp2pgsql -s 27493:4326 -W "latin1" Cont_Freg_V5.shp map > map.sql
psql -U $1 $2 < map.sql

if [ ! -f taxi_services.sql ]; then
    curl -O http://www.dcc.fc.up.pt/~michel/Mobile_Intelligence/taxi_services.sql
fi
if [ ! -f taxi_stands.sql ]; then
    curl -O http://www.dcc.fc.up.pt/~michel/Mobile_Intelligence/taxi_stands.sql
fi

psql -U $1 $2 < taxi_services.sql
psql -U $1 $2 < taxi_stands.sql
