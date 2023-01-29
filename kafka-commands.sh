#!/usr/bin/env bash

#kafka-topics --bootstrap-server localhost:9092 --list ;
#  __consumer_offsets
#  __transaction_state
#  input
#  message-balance-application-message-balance-agg-changelog
#  message-balance-exactly-once
#  message-transactions

kafka-console-consumer --bootstrap-server localhost:9092 --topic message-transactions --from-beginning

#kafka-console-consumer --bootstrap-server localhost:9092 --topic message-balance-application-message-balance-agg-changelog --from-beginning

#kafka-console-consumer --bootstrap-server localhost:9092 --topic message-balance-exactly-once --from-beginning





