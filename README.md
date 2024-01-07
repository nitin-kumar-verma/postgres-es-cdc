This project can be used to sync data from postgres onto es

Pre Requisites:
  wal_level should be set to logical in postgres
  Running instance of apache kafka

Currently basic auth is supported for elastic-search

Steps:
  1. Ensure Postgres, ElasticSearch and Kafka are deployed correctly.
  2. Start the application in producer mode, it will listen to wallog and push the events to kafka
  3. Start the application in consumer mode, it will listen to kafka events and push the changes to elasticsearch
  4. To start in producer / consumer mode, refer to envs (set MODE = PRODUCER / CONSUMER as required)
