# lazy-kafka

[![PyPI - Version](https://img.shields.io/pypi/v/lazy-kafka.svg)](https://pypi.org/project/lazy-kafka)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/lazy-kafka.svg)](https://pypi.org/project/lazy-kafka)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/lazy-kafka/lazy-kafka/trunk/.github/badges/coverage-badge.json&style=flat)](https://github.com/lazy-kafka/lazy-kafka/actions)

-----

A tui application to enhance Kafka eco-system devX.

I started this project quite a while ago and haven't really worked on it all that much. There are many, more feature rich and mature projects, so probably you should not use exactly this one. The purpose of this project is to have some fun, not to churn out a project, so I am not too keen on vibeing it to v1.0.0. I am making the repo public, so maybe someone can get inspired/look around 🤷

[![asciicast](https://asciinema.org/a/1258296.svg)](https://asciinema.org/a/1258296)

# Features
- tui
- cli

## Kafka
- list topics
- display basic topic information
- list messages (experimental)

## Kafka connect
- list connectors
- display basic connector information
- display connector status
- monitor connector status (follow)

## Schema Registry
- list subjects
- create, delete subjects
- display subject data



## Table of Contents

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install lazy-kafka
```

## License

`lazy-kafka` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

# dev
run:
- 🐳 docker environment `$ scripts/start-up`
- 📃 `$ hatch run dev:console` -> `🐣 lazy-kafka/scripts ➜ textual console -x EVENT -x SYSTEM --port 7342`
- 💾 `$ hatch run dev:app` -> `🐣 lazy-kafka ❯ python src/lazy_kafka/__main__.py`

If you are using pip:
- `pip install -e .`
- `lazy-kafka`

check topics:
docker exec 1793ae26d162 /bin/kafka-topics --bootstrap-server=localhost:9092

## Docker compose
The docker compose will:
- Start a Kafka cluster: 1 broker and 1 zookeeper
  - create a test-topic
- Start ConfluentSchemaRegistry
  - register a 'test-schema' for the 'test-topic'
- Start LocalStack S3 bucket 'test-bucket'
- Start Kafka Connect
  - create a connector to read from test-topic, dump to S3 test-bucket



# useful

curl -s -XGET http://localhost:8083/connector-plugins | jq '.[].class'

curl -XGET http://localhost:8083/connectors

# Roadmap

## v0.1.0
- settings management
- k-connect continuous update (like watch)
  - [x] Color the State column based on the value
  - [x] Fix currently highlighted connector selection on update
  - [x] Load from config file
    - [-] Add file watcher - get inspiration from watchdog or [toolong](https://github.com/Textualize/toolong/blob/main/src/toolong/watcher.py)
  - [x] Expose more configuration (kafka auth related)

## v0.2.0
- [ ] see messages in a stream
  - [ ] extend kafka page support: AIOKafka/confluent-kafka library facade
    - [ ] implementation only with confluent-kafka, but with a facade
- [ ] produce messages into a stream
  - [ ] concrete producer
  - [ ] dummy producer (agent pool pattern?)



## 

## v1.0
Generate client code from openapi
- deprecate confluent-kafka in favor of [kafka-rest api](https://github.com/confluentinc/kafka-rest/blob/master/api/v3/openapi.yaml)
    - [docker](https://hub.docker.com/r/confluentinc/cp-kafka-rest)
- kakfa-connect
-schema-registy

## 04-28
- [x] kafka consume feature
- [ ] kafka consume UI with message table
- [ ] CLI
  - [x] initial set up
  - [ ] factor into own module
  - [x] consume command to follow topic
  - [ ] produce command (de-scope e.g.: schemathesis like test data generation)
- [ ] tests
  - [-] :( 

## 09-15
- schema listing and browsing
  x multiple versions of stuff (-> always show the latest)
  x ** it is cumbersome to go from subject to schema (human exploration) **
    ** it is easy to look up `id` -> schema (how the messages are serialized) **
    0. request to list subjects
    1. request to `subjects/<subject>/versions` to list versions
    2. request to `subjects/<subjects>/versions/-1` to get the latest schema info
    3. request to `schemas/ids/{id}/schema`
  -> can delete schema with a dialog popping up
  -> The design philosophy is to handle events as close to the source as possible. 


- create modal screens to create schema, connector, topic (?)
- create settings management from config file
- create a screen for settings
- add delete action
- 



# Development
- confluent kafka docs: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.TopicPartition
- detailed example of confluent kafka lib api: https://github.com/confluentinc/confluent-kafka-python/issues/1443


🫛 lazy-kafka ➜ textual console -x EVENT -x SYSTEM --port 7342
🫛 lazy-kafka ➜ python src/lazy_kafka/__main__.py --help
🫛 lazy-kafka ➜ python src/lazy_kafka/scripts/topic_schema_producer.py
🫛 lazy-kafka ➜textual run --dev --port 7342 src/lazy_kafka/__main__.py

The app does not handle the case where topics are empty. Produce dummy data with the cli:
🐣 lazy-kafka ➜ python -m lazy_kafka kafka produce -f src/lazy_kafka/default_config.toml test-topic

(optionally also run this:)
🐣 lazy-kafka ❯ python -m lazy_kafka kafka consume -f src/lazy_kafka/default_config.toml test-topic
