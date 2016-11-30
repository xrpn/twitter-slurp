## Synopsis

This code slurps the Twitter spritzer.  The spritzer is a random sample of the firehose, and contains approximately 1% of it.

## Architecture

There are four components: 
* The twitter client receives content from the spritzer.  It behaves as the producer for a local buffer storing raw messages.  Implementation is largely via 3rd party libraries mostly in Java.
* The streamer is the consumer that feeds off the local buffer.  It processes the raw messages to extract some relevant information.  Implementation is by means of Akka streams, packages json and aks.
* The stats accumulators, which keep track of the relevant information.  They are fed from the streamer.  Implementation is by means of Akka actors, package aka.
* The REST Api, which can be interrogated with, say, curl to retrieve relevant information..  Implementation by means of Akka http, package http.

## Motivation

Homework for a job interview.  The driver is to slurp the spritzer as a POC, using an architecture that can scale to the firehose.

## Installation

Provide suitable credentials for Twitter in twitter4j.properties.  Examine other config items for suitability.  Run from the build tool as $sbt run.  After launching check test.log (a peer of this README.md).  The server for REST should appear at port 5000.

## API Reference

Query the REST api with http GET methods.  This is a good starting point if using default config values:

$curl --request GET 'localhost:5000/help'

## Tests

Unit tests will run using $sbt test

