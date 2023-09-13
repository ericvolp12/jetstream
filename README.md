# Jetstream

Jetstream is a Bluesky-specific streaming service that consumes an ATProto `com.atproto.sync.subscribeRepos` stream and converts it into a series of `app.bsky` namespaced objects.

Jetstream uses BadgerDB to keep track of subjects of likes, reposts, follows, and blocks so that when they are deleted, it can enrich a stream event with the subject being "unliked", "unreposted", "unfollowed" or "unblocked".
- This is done as a "best-effort", Jetstream only knows about the subjects of events it has witnessed, so there will be like deletions and such that don't contain a subject.

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
- `wantedTypes` - An array of types to filter which records you hear about on your stream (default empty = all types)
  - `post`
  - `like`
  - `repost`
  - `follow`
  - `block`
  - `list`
  - `listItem`
  - `feedGenerator`
  - `handle`
  - `profile`

A maximal example using all parameters looks like:
```
ws://localhost:8080/subscribe?format=cbor&compress=true&wantedTypes=post&wantedTypes=like&wantedTypes=follow
```
