/*
    Insert master-data into a table
*/
INSERT INTO BANK_ATMS (`id`, `zip`, `city`, `coordinate`) VALUES ('ATM-1', '6982', 'Agno', STRUCT(`latitude`:=45.997526, `longitude`:=8.902032));
INSERT INTO BANK_ATMS (`id`, `zip`, `city`, `coordinate`) VALUES ('ATM-2', '6500', 'Bellinzona', STRUCT(`latitude`:=46.196088, `longitude`:=9.025576));
INSERT INTO BANK_ATMS (`id`, `zip`, `city`, `coordinate`) VALUES ('ATM-3', '3001', 'Bern', STRUCT(`latitude`:=46.944854, `longitude`:=7.436056));

DROP CONNECTOR IF EXISTS `bank.Atm-event-withdrawn`;
DROP CONNECTOR IF EXISTS `bank.Atm-event-deposit`;

/*
    Generate events
*/
CREATE SOURCE CONNECTOR `bank.Atm-event-withdrawn` WITH (
  'connector.class'='io.confluent.kafka.connect.datagen.DatagenConnector',
  'key.converter'='org.apache.kafka.connect.storage.StringConverter',
  'value.converter'='org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable'='false',
  'max.interval'='1000',
  'max.tasks'='1',
  'kafka.topic'='bank.Atm-event',
  'schema.keyfield'='account',
  'schema.string'='
        {
        "name": "accountevent",
        "type": "record",
        "fields": [{
          "name": "account",
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
                    "min": -1000,
                    "max": 1
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

/*
    Generate events
*/
CREATE SOURCE CONNECTOR `bank.Atm-event-deposit` WITH (
  'connector.class'='io.confluent.kafka.connect.datagen.DatagenConnector',
  'key.converter'='org.apache.kafka.connect.storage.StringConverter',
  'value.converter'='org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable'='false',
  'max.interval'='1000',
  'max.tasks'='1',
  'kafka.topic'='bank.Atm-event',
  'schema.keyfield'='account',
  'schema.string'='
        {
        "name": "accountevent",
        "type": "record",
        "fields": [{
          "name": "account",
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