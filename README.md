== Red Panda Http Rust ==
This crate uses the HTTP Proxy API of Red Panda (=native Kafka clone) to connect.
(I'm unsure if the original Kafka has a similar or compatible HTTP-based API)
It has a pluggable http client library, in order to make it embedded and WASM friendly.



This implementation has a 'reqwest' based implementation, and there is an ESP32-IDF version in development for
embedded use cases, and a Spin based implementation to support WASM.

CI:
[![CircleCI](https://circleci.com/gh/flyaruu/redpanda-http-rust.svg?style=svg)](https://circleci.com/gh/flyaruu/redpanda-http-rust)
