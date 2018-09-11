# multi-storage-kafka-avro-serdes

[![Build Status](https://travis-ci.org/saidbouras/multi-storage-kafka-avro-serdes.svg?branch=master)](https://travis-ci.org/saidbouras/multi-storage-kafka-avro-serdes)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/saidbouras/multi-storage-kafka-avro-serdes/blob/master/LICENSE)

Library that wrapped the schema registry client to conform to a client interface, 
allowing external schemas locations like stores (in-memory, rocksdb), 
databases to be used to replace the confluent schema registry.