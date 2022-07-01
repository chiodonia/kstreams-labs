# KStreams and ksqlDB labs

```
docker-compose up
```

```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
SHOW PROPERTIES;
```

## Bank
```
                            ┌────────────────────────┐
                            │                        │
                ┌──────────►│ bank.Deposit-event     ├────────────┐
                │           │                        │            ▼
┌───────────────┴─┐         └────────────────────────┘          ┌─────────────────────────┐       ┌─────────────────────┐
│                 │                                             │                         │       │                     │
│  bank.Atm-event │                                             │ bank.AccountTransaction ├──────►│ bank.Account-state  │
│                 │                                             │                         │       │                     │
└───────────────┬─┘         ┌────────────────────────┐          └─────────────────────────┘       └─────────────────────┘
                │           │                        │            ▲                                <<TABLE>>
                ├──────────►│ bank.Withdrawn-event   ├────────────┘
                │           │                        │
                │           └────────────────────────┘
                │
                │
                │
                │           ┌────────────────────────┐
                │           │                        │
                └──────────►│ bank.Atm-event-by-atm  │
                            │                        │
                            └────────────────────────┘
                             REKEYED BY ATM


 ┌────────────────┐
 │                │
 │ bank.Atm-state │
 │                │
 └────────────────┘
```

### Testing 
```
docker exec ksqldb-cli ksql-test-runner \
    -i /test/bank/inputs/bank.Atm-even.json \
    -s /sql/bank/statements.sql \
    -o /test/bank/outputs/bank.DepositAndWithdrawn-event.json \
     | grep ">>>"

docker exec ksqldb-cli ksql-test-runner \
    -i /test/bank/inputs/bank.DepositAndWithdrawn-event.json \
    -s /sql/bank/statements.sql \
    -o /test/bank/outputs/bank.AccountTransaction.json \
     | grep ">>>"

docker exec ksqldb-cli ksql-test-runner \
    -i /test/bank/inputs/bank.Atm-even.json \
    -s /sql/bank/statements.sql \
    -o /test/bank/outputs/bank.AccountTransaction.json \
     | grep ">>>"

docker exec ksqldb-cli ksql-test-runner \
    -i /test/bank/inputs/bank.AccountTransaction.json \
    -s /sql/bank/statements.sql \
    -o /test/bank/outputs/bank.Account-state.json \
     | grep ">>>"

docker exec ksqldb-cli ksql-test-runner \
    -i /test/bank/inputs/bank.Atm-even.json \
    -s /sql/bank/statements.sql \
    -o /test/bank/outputs/bank.Account-state.json \
     | grep ">>>"

docker exec ksqldb-cli ksql-test-runner \
    -i /test/bank/inputs/bank.Atm-even.json \
    -s /sql/bank/statements.sql \
    -o /test/bank/outputs/bank.Atm-event-by-atm.sql \
     | grep ">>>"
```

### Deploy
```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
RUN SCRIPT '/sql/bank/statements.sql';

SHOW TYPES;
SHOW STREAMS;
SHOW TOPICS EXTENDED;
SHOW TABLES;

RUN SCRIPT '/sql/bank/generate-records.sql';

PRINT 'bank.Atm-state' FROM BEGINNING LIMIT 3;
-- SET 'auto.offset.reset'='earliest';
SELECT * FROM BANK_ATMS EMIT CHANGES LIMIT 3;

SHOW CONNECTORS;

PRINT 'bank.Atm-event' FROM BEGINNING LIMIT 10;
PRINT 'bank.Deposit-event' FROM BEGINNING LIMIT 10;
PRINT 'bank.Withdrawn-event' FROM BEGINNING LIMIT 10;
PRINT 'bank.AccountTransaction' FROM BEGINNING LIMIT 10;
PRINT 'bank.Account-state' FROM BEGINNING LIMIT 10;

PRINT 'bank.Atm-event' LIMIT 10;
PRINT 'bank.Deposit-event' LIMIT 10;
PRINT 'bank.Withdrawn-event' LIMIT 10;
PRINT 'bank.AccountTransaction' LIMIT 10;
PRINT 'bank.Account-state' LIMIT 10;

SELECT * FROM BANK_ATM_EVENTS LIMIT 10;
SELECT * FROM BANK_ATM_DEPOSIT_EVENTS EMIT CHANGES LIMIT 10;
SELECT * FROM BANK_ATM_WITHDRAWN_EVENTS EMIT CHANGES LIMIT 10;
SELECT * FROM BANK_ACCOUNT_TRANSACTIONS LIMIT 10;
SELECT * FROM BANK_ACCOUNTS LIMIT 10;

SELECT * from BANK_ACCOUNTS WHERE `account` = '92';
SELECT * from BANK_ACCOUNTS WHERE `account` = '92' EMIT CHANGES LIMIT 2;

SELECT BANK_ACCOUNT_TRANSACTIONS.`account`, 
    `amount`,
    CASE 
        WHEN `amount` < 0 THEN 'withdrawn'
        WHEN `amount` > 0 THEN 'deposit'
        ELSE 'unknown'
    END AS `transaction`,
    `atm`,
    CONCAT(`zip`, ' ', `city`) AS `city`  
    FROM BANK_ACCOUNT_TRANSACTIONS 
    LEFT JOIN BANK_ATMS ON BANK_ACCOUNT_TRANSACTIONS.`atm` = BANK_ATMS.`id`
    EMIT CHANGES 
    LIMIT 10;
  
PRINT 'bank.Atm-event-by-atm' LIMIT 10;
SELECT * FROM BANK_ATM_EVENTS_REKEYED_BY_ATM EMIT CHANGES LIMIT 10;
```

# TODO 

SELECT "account.id" AS ID, "atm" as ATM , "amount" AS AMOUNT FROM ACCOUNT_STREAM EMIT CHANGES;

SELECT account."id" AS ID, CAST(GEO_DISTANCE(atm.LOCATION->"latitude", atm.LOCATION->"longitude", 45.997129, 8.899271, 'KM') AS INT) AS X
FROM ACCOUNT_STREAM account INNER JOIN ATM_TABLE atm ON account."atm" = atm.ID
EMIT CHANGES;

SELECT "id" AS ID, COUNT(*) AS MOVEMENTS, SUM("amount") TOTAL
FROM ACCOUNT_STREAM  
WINDOW TUMBLING (SIZE 5 MINUTE)
GROUP BY "id"
HAVING COUNT(*) >= 2 AND SUM("amount") >= 100
EMIT CHANGES;