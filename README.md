# tracen

`tracen` is a Rust library for defining trackers, validating events, and computing metrics from event logs.

The tracker definition is the source of truth. It describes the event payload, the derived values, the metrics, and the queryable outputs. The rest of the system works from that definition.

## Defining a tracker

A tracker starts as a small DSL file.

The DSL is compiled during build time and `tracen` generates the Rust and TypeScript artifacts the consuming application integrates with. That keeps the tracking layer in one compiled core instead of reimplementing it across application code.

## Running a tracker

At runtime, `tracen` works with the compiled tracker definition and the event log.

The runtime does three things:

- validates and normalizes raw events
- runs queries over normalized events
- returns deterministic outputs such as counts, metrics, grouped results, and alerts

## Example

A workout tracker looks like this:

```text
tracker "workout" v1 {
  fields {
    exercise: text
    reps: int optional
    weight: float optional
  }

  metrics {
    total_sessions = count() over all_time
    max_weight = max(weight) over all_time
  }
}
```

An event for that tracker looks like this:

```json
{
  "event_id": "w1",
  "ts": 1704067200000,
  "payload": {
    "exercise": "bench_press",
    "reps": 5,
    "weight": 100.0
  }
}
```

After validation, the application stores the normalized event and includes it in later compute queries. A compute result over a log containing that event looks like this:

```json
{
  "total_events": 1,
  "window_events": 1,
  "metrics": {
    "total_sessions": 1,
    "max_weight": 100.0
  },
  "alerts": []
}
```

## Using it from an app

`tracen` does not own persistence. The consuming application stores events, loads them, and decides when validation or compute should run.

An integration looks like this:

- the app receives a raw event
- `tracen` validates and normalizes it
- the app stores the normalized event
- the app asks `tracen` to compute results from the stored log

This keeps event storage and application flow outside the library while keeping tracking behavior inside it.

## Getting started

Use `cargo add tracen`.

Most consumers should depend on the top-level `tracen` crate.

## Development

Main checks:

- `just check`
- `just publish-check`

Nix development shell:

- `nix develop`
- `nix develop -c just check`

## Contributing

Any contributions are welcome and appreciated.

If something here is useful and there is an open issue that matches the work, feel free to pick it up.

Feel free to open an issue for bugs, rough edges, missing pieces, or ideas that would make the library easier to use.

Keep changes generic to the tracking layer. If a behavior is specific to one application or domain, it does not belong in `tracen`.

Before opening a change, run:

- `just check`
- `just publish-check`

## License

MIT. See [LICENSE](LICENSE).
