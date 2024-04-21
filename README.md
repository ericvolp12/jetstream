# Jetstream

Jetstream is a streaming service that consumes an ATProto `com.atproto.sync.subscribeRepos` stream and converts it into lightweight, friendly JSON.

Jetstream converts the CBOR-encoded MST blocks produced by the ATProto firehose and translates them into JSON objects that are easier to interface with using standard tooling available in programming languages.

Jetstream also provides the ability to persist streams to Kafka for alternative stream consumption.

## Running Jetstream

To run Jetstream, make sure you have docker and docker compose installed and run `make up` in the repo root.

This will start a Jetstream instance at `http://localhost:6008`

Once started, you can connect to the event stream at: `ws://localhost:6008/subscribe`

Prometheus metrics are exposed at `http://localhost:6008/metrics`

## Consuming Jetstream

To consume Jetstream you can use any websocket client

Connect to `ws://localhost:6008/subscribe` to start the stream

The following Query Parameters are supported:

- `format` - The encoding format of messages (default `json`)
  - `json`
  - `cbor`
- `compress` - Whether or not to use `zstd` compression on all messages in the stream for your client (default `false`)
  - `true` - enables compression
- `wantedCollections` - An array of [Collection NSIDs](https://atproto.com/specs/nsid) to filter which records you receive on your stream (default empty = all types)
- `wantedDids` - An array of Repo DIDs to filter which records you receive on your stream (Default empty = all repos)

A maximal example using all parameters looks like:

```
ws://localhost:6008/subscribe?format=cbor&compress=true&wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.graph.follow&wantedDids=did:plc:q6gjnaw2blty4crticxkmujt
```
