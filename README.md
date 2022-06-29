# KStreams labs

## Start
```
docker-compose up
./topics.sh
./connectors.sh
./ksqldb.sh
```

## ksqlDB cli
```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
RUN SCRIPT '/etc/ksqldb/queries.sql';
RUN SCRIPT '/etc/ksqldb/inserts.sql';
```

