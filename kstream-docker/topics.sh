#!/bin/bash

docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --create \
  --topic labs.Account-event \
  --partitions 6

docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --create \
  --topic labs.Account-state \
  --partitions 6 \
  --config cleanup.policy=compact

docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --create \
  --topic labs.Atm-state \
  --partitions 6 \
  --config cleanup.policy=compact
