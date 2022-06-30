# KStreams labs

## Start
```
docker-compose up
./topics.sh
./connectors.sh
./ksqldb.sh
```

## Labs
### Bank
```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
RUN SCRIPT '/labs/bank/streams.sql';
SHOW TYPES;
SHOW TOPICS;
-- SHOW STREAMS;
-- SHOW STREAMS EXTENDED;
-- SHOW QUERIES;
-- EXPLAIN CSAS_BANK_ATM_WITHDRAWN_EVENTS_13;

RUN SCRIPT '/labs/bank/data.sql';
SHOW CONNECTORS;

-- SET 'auto.offset.reset'='earliest';
-- SET 'auto.offset.reset'='latest';
PRINT 'bank.atm.Withdrawn-event' LIMIT 10;
PRINT 'bank.atm.Deposit-event' LIMIT 10;

SELECT * FROM BANK_ATM_WITHDRAWN_EVENTS EMIT CHANGES LIMIT 10;
SELECT * FROM BANK_ATM_DEPOSIT_EVENTS LIMIT 10;
```

