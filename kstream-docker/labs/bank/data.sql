DROP CONNECTOR IF EXISTS `bank.atm.Withdrawn-event-source`;
DROP CONNECTOR IF EXISTS `bank.atm.Deposit-event-source`;

-- Generate data
CREATE SOURCE CONNECTOR `bank.atm.Withdrawn-event-source` WITH (
  'connector.class'='io.confluent.kafka.connect.datagen.DatagenConnector',
  'key.converter'='org.apache.kafka.connect.storage.StringConverter',
  'value.converter'='org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable'='false',
  'max.interval'='1000',
  'max.tasks'='1',
  'kafka.topic'='bank.atm.Atm-event',
  'schema.keyfield'='id',
  'schema.string'='
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
  '
);

CREATE SOURCE CONNECTOR `bank.atm.Deposit-event-source` WITH (
  'connector.class'='io.confluent.kafka.connect.datagen.DatagenConnector',
  'key.converter'='org.apache.kafka.connect.storage.StringConverter',
  'value.converter'='org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable'='false',
  'max.interval'='1000',
  'max.tasks'='1',
  'kafka.topic'='bank.atm.Atm-event',
  'schema.keyfield'='id',
  'schema.string'='
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
  '
);