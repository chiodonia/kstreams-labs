DROP STREAM IF EXISTS WITHDRAWN DELETE TOPIC;
DROP STREAM IF EXISTS DEPOSIT DELETE TOPIC;
DROP STREAM IF EXISTS TRANSACTION DELETE TOPIC;
DROP TYPE IF EXISTS ATM_OPERATION;

CREATE TYPE ATM_OPERATION AS STRUCT<AMOUNT BIGINT, ATM VARCHAR>;

CREATE OR REPLACE STREAM TRANSACTION (
    KEY VARCHAR KEY,
    TIMESTAMP BIGINT,
    WITHDRAWN ATM_OPERATION,
    DEPOSIT ATM_OPERATION
) WITH (
    KAFKA_TOPIC='labs.Transaction-event',
    PARTITIONS=6,
    VALUE_FORMAT='JSON'
);

CREATE OR REPLACE STREAM WITHDRAWN
WITH (
    KAFKA_TOPIC='labs.Withdrawn-event',
    PARTITIONS=6,
    VALUE_FORMAT='JSON'
) AS
SELECT KEY, TIMESTAMP, STRUCT(AMOUNT:=ABS(WITHDRAWN->AMOUNT), ATM:=CONCAT('ATM-', UCASE(WITHDRAWN->ATM))) AS WITHDRAWN FROM TRANSACTION WHERE WITHDRAWN IS NOT NULL;

CREATE OR REPLACE STREAM DEPOSIT
WITH (
    KAFKA_TOPIC='labs.Deposit-event',
    PARTITIONS=6,
    VALUE_FORMAT='JSON'
) AS
SELECT KEY, TIMESTAMP, STRUCT(AMOUNT:=ABS(DEPOSIT->AMOUNT), ATM:=CONCAT('ATM-', UCASE(DEPOSIT->ATM))) AS DEPOSIT FROM TRANSACTION WHERE DEPOSIT IS NOT NULL;

DESCRIBE STREAMS EXTENDED;
