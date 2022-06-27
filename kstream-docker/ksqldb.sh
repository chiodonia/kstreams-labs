#!/bin/bash

execute() {
curl --silent --header "Accept: application/vnd.ksql.v1+json" --header "Content-Type: application/vnd.ksql.v1+json" --request POST \
--data '{
    "ksql": "'"$1"'",
    "streamsProperties": '"$2"'
}' \
localhost:8088/ksql | jq .
}

execute 'DROP CONNECTOR IF EXISTS FOO_SOURCE;' {}
execute 'DROP STREAM IF EXISTS FOO DELETE TOPIC;' {}

ksql=$({ cat | tr -d '\n' | sed 's/"/\\"/g'; } << EOF
CREATE STREAM FOO (
  KEY VARCHAR KEY,
  TIMESTAMP BIGINT,
  WITHDRAWN STRUCT <AMOUNT BIGINT, ATM VARCHAR>) WITH (
    KAFKA_TOPIC='labs.Foo',
    PARTITIONS=6,
    VALUE_FORMAT='JSON'
  );
EOF
)
execute "${ksql}" '{"ksql.streams.auto.offset.reset":"earliest"}'

schema=$({ cat | tr -d '\n'; } << EOF
{
"name": "accountevent",
"type": "record",
"fields": [{
  "name": "id",
  "type": {
    "type": "string",
    "arg.properties": {
      "regex": "[1-9]{2}"
    }
  }
}, {
   "name":"timestamp",
   "type":{
     "type":"long",
     "format_as_time":"unix_long",
     "arg.properties": {
        "iteration": { "start": 1, "step": 100}
      }
   }
 }, {
  "name": "withdrawn",
  "type": {
    "name": "withdrawn",
    "type": "record",
    "fields": [{
      "name": "amount",
      "type": {
        "type": "int",
        "arg.properties": {
          "range": {
            "min": 1,
            "max": 1000
          }
        }
      }
    }, {
      "name": "atm",
      "type": {
        "type": "string",
        "arg.properties": {
          "options": ["1", "2", "3"]
        }
      }
    }]
  }
}]
}
EOF
)
ksql=$({ cat | tr -d '\n' | sed 's/"/\\"/g'; } << EOF
CREATE SOURCE CONNECTOR FOO_SOURCE WITH (
  'connector.class'='io.confluent.kafka.connect.datagen.DatagenConnector',
  'key.converter'='org.apache.kafka.connect.storage.StringConverter',
  'value.converter'='org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable'='false',
  'max.interval'='1000',
  'max.tasks'='1',
  'kafka.topic'='labs.Foo',
  'schema.keyfield'='id',
  'schema.string'='$schema'
);
EOF
)
execute "${ksql}" '{}'

execute 'SHOW TOPICS;' {}
execute 'SHOW STREAMS;' {}
execute 'SHOW CONNECTORS;' {}
