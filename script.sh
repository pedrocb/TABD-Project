shp2pgsql -s 27493:4326 -W "latin1" Cont_Freg_V5.shp map > map.sql
psql -U guest guest < map.sql

