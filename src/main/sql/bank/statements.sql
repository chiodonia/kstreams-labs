/*
    Create some data types
*/
DROP TYPE IF EXISTS BANK_ATM_TRANSACTION;
CREATE TYPE BANK_ATM_TRANSACTION AS
    STRUCT<
        `amount` BIGINT,
        `atm` VARCHAR
    >;

DROP TYPE IF EXISTS COORDINATE;
CREATE TYPE COORDINATE AS
    STRUCT<
        `latitude` DECIMAL(8,6),
        `longitude` DECIMAL(8,6)
   >;

/*
    Create a stream for the events to get in
*/
CREATE OR REPLACE STREAM BANK_ATM_EVENTS (
        `key` VARCHAR KEY,
        `account` VARCHAR,
        `timestamp` BIGINT,
        `withdrawn` BANK_ATM_TRANSACTION,
        `deposit` BANK_ATM_TRANSACTION
    ) WITH (
        KAFKA_TOPIC='bank.Atm-event',
        PARTITIONS=6,
        VALUE_FORMAT='JSON'
    );

/*
    Filter, transform and split a stream into different streams
*/
CREATE OR REPLACE STREAM BANK_ATM_WITHDRAWN_EVENTS
    WITH (
        KAFKA_TOPIC='bank.Withdrawn-event',
        PARTITIONS=6,
        VALUE_FORMAT='JSON'
    ) AS
    SELECT `key`, `account`, `timestamp`, STRUCT(`amount`:=ABS(`withdrawn`->`amount`), `atm`:=CONCAT('ATM-', UCASE(`withdrawn`->`atm`))) AS `withdrawn`
        FROM BANK_ATM_EVENTS
        WHERE `withdrawn` IS NOT NULL AND `withdrawn`->`amount` < 0
        PARTITION BY `key`;

CREATE OR REPLACE STREAM BANK_ATM_DEPOSIT_EVENTS
    WITH (
        KAFKA_TOPIC='bank.Deposit-event',
        PARTITIONS=6,
        VALUE_FORMAT='JSON'
    ) AS
    SELECT `key`, `account`, `timestamp`, STRUCT(`amount`:=ABS(`deposit`->`amount`), `atm`:=CONCAT('ATM-', UCASE(`deposit`->`atm`))) AS `deposit`
        FROM BANK_ATM_EVENTS
        WHERE `deposit` IS NOT NULL AND `deposit`->`amount` > 0
        PARTITION BY `key`;

/*
    Merge two or more streams
*/
CREATE OR REPLACE STREAM BANK_ACCOUNT_TRANSACTIONS
    WITH (
        KAFKA_TOPIC='bank.AccountTransaction',
        PARTITIONS=6,
        VALUE_FORMAT='JSON'
    ) AS
    SELECT `key`, `account`, `timestamp`, ABS(`withdrawn`->`amount`) * -1 AS `amount`, `withdrawn`->`atm` AS `atm`, 'withdrawn' AS `transaction`
        FROM BANK_ATM_WITHDRAWN_EVENTS
        PARTITION BY `key`;

INSERT INTO BANK_ACCOUNT_TRANSACTIONS
  SELECT `key`, `account`, `timestamp`, ABS(`deposit`->`amount`) AS `amount`, `deposit`->`atm` AS `atm`, 'deposit' AS `transaction`
    FROM BANK_ATM_DEPOSIT_EVENTS
    PARTITION BY `key`;

/*
    Aggregate a stream into a table (events to state)
*/
CREATE OR REPLACE TABLE BANK_ACCOUNTS
    WITH (
        KAFKA_TOPIC='bank.Account-state',
        PARTITIONS=6,
        VALUE_FORMAT='JSON'
    ) AS
    SELECT `account`, sum(`amount`) AS `saldo`
        FROM BANK_ACCOUNT_TRANSACTIONS
        GROUP BY `account`;

/*
    Create a table to host master-data
*/
CREATE TABLE BANK_ATMS (
        `id` VARCHAR PRIMARY KEY,
        `zip` VARCHAR,
        `city` VARCHAR,
        `coordinate` COORDINATE
    ) WITH (
        KAFKA_TOPIC='bank.Atm-state',
        PARTITIONS=6,
        VALUE_FORMAT='JSON'
    );

/*
    Change the key of a stream (rekey)
*/
CREATE OR REPLACE STREAM BANK_ATM_EVENTS_REKEYED_BY_ATM
    WITH (
        KAFKA_TOPIC='bank.Atm-event-by-atm',
        PARTITIONS=6,
        VALUE_FORMAT='JSON'
    ) AS
    SELECT *
        FROM BANK_ATM_EVENTS
        PARTITION BY
            CASE
                WHEN `withdrawn` IS NOT NULL THEN `withdrawn`->`atm`
                WHEN `deposit` IS NOT NULL THEN `deposit`->`atm`
            END;