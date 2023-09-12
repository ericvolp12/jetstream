# Jetstream

Jetstream is a Bluesky-specific streaming service that consumes an ATProto `com.atproto.sync.subscribeRepos` stream and converts it into a series of `app.bsky` namespaced objects.

## Running Jetstream

To run Jetstream, make sure you have docker and docker compose installed and run `make up` in the repo root.

This will start a Jetstream instance at `http://localhost:6008`

Once started, you can connect to the event stream at: `ws://localhost:6008/subscribe`

Prometheus metrics are exposed at `http://localhost:6008/metrics`
