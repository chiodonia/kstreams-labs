#!/bin/bash

drop_connector() {
  curl -f -X DELETE http://localhost:8083/connectors/"$1"
  echo INFO: Connector \'"$1"\' has been dropped.
}

create_datagen_connector() {
  curl -s -X POST http://localhost:8083/connectors \
     -H 'Content-Type: application/json' \
     -d '{
      "name": "'"$1"'",
      "config": {
        "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "max.interval": "'"$3"'",
        "tasks.max": "1",
        "kafka.topic": "'"$2"'",
        "schema.keyfield": "id",
        "schema.string": "'"$4"'"
      }
    }'
  echo INFO: Datagen connector \'"$1"\' writing in \'"$2"\' has been created.
}

schema_atm_state=$({ cat | tr -d '\n' | sed 's/"/\\"/g'; } << EOF
{
  "type": "record",
  "name": "atm",
  "fields": [{
    "name": "id",
    "type": {
      "type": "string",
      "arg.properties": {
        "options": ["1", "2", "3"]
      }
    }
  }, {
    "name": "location",
    "type": {
      "type": "record",
      "name": "location",
      "fields": [{
        "name": "latitude",
        "type": "double"
      }, {
        "name": "longitude",
        "type": "double"
      }],
      "arg.properties": {
        "options": [{
          "latitude": 45.99580,
          "longitude": 8.90048
        }, {
          "latitude": 46.00081,
          "longitude": 8.92109
        }, {
          "latitude": 46.94890,
          "longitude": 7.44224
        }]
      }
    }
  }]
}
EOF
)

schema_account_withdrawn_event=$({ cat | tr -d '\n' | sed 's/"/\\"/g'; } << EOF
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

schema_account_deposit_event=$({ cat | tr -d '\n' | sed 's/"/\\"/g'; } << EOF
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
  "name": "deposit",
  "type": {
    "name": "deposit",
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

drop_connector labs-atm-state
drop_connector labs-account-deposit-events
drop_connector labs-account-withdrawn-events

create_datagen_connector labs-atm-state labs.Atm-state 1000 "${schema_atm_state}"
create_datagen_connector labs-account-deposit-events labs.Account-event 1000 "${schema_account_deposit_event}"
create_datagen_connector labs-account-withdrawn-events labs.Account-event 1000 "${schema_account_withdrawn_event}"