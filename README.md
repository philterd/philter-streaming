# Philter Streaming

This repository contains sample projects to illustrate how to use [Philter](https://www.philter.app) in a text streaming environment to find and remove sensitive information from streaming text. We hope you find these sample projects helpful to get you started using Philter.

These projects use Philter's REST API to find and remove sensitive information from text. In some cases this may not be desired due to performance or other restrictions. If this is the case please [contact us](mailto:support@mtnfog.com) for native Philter libraries that can be called directly from your Java/Scala code without the extra burden of making REST calls.

## Apache Flink

The `philter-flink-kafka` project is a Flink application to consume text from an Apache Kafka topic, filter sensitive information from the text, and then publish the filtered text to another Kafka topic.

## Apache Pulsar

The `philter-pulsar` project is an Apache Pulsar Function that uses Philter to find and remove sensitive information from text going through an Apache Pulsar topic.

## License

This project is licensed under the Apache License, version 2.0.

Copyright 2020 Mountain Fog, Inc.
Philter is a registered trademark of Mountain Fog, Inc.
