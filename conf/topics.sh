#!/bin/bash

docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --create \
  --topic bank.Atm-event \
  --partitions 6

docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --create \
  --topic bank.Atm-event-by-atm \
  --partitions 6

docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --create \
  --topic bank.Atm-state \
  --partitions 6 \
  --config cleanup.policy=compact

docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --create \
  --topic bank.Account-state \
  --partitions 6 \
  --config cleanup.policy=compact

