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

The engine is storage-independent. The optional `tracen_ffi_core::event_store` SQLite host owns durable command receipts, accepted revisions and atomic publication. Ordinary producers use `execute_command` with a tracker adapter; there is no public raw commit. Adapters retain domain rules. Migration and projection callbacks are trusted native integration boundaries, not producer escape hatches.

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

### Event acceptance rules

Arithmetic and sum/average accumulator overflow return evaluation errors instead of JSON null. A failed derivation leaves engine state unchanged. Large finite metric values remain finite when rounding; missing optional values still use the existing null semantics.

Derived expressions are checked for available names and scalar types at compilation. They can read declared fields (bare or `payload.`), earlier derives by bare name, `event.id`, `event.tracker_id`, `event.ts`, and dynamic `meta.*` paths. Payload-qualified derived values are unavailable until preparation finishes. Unknown names/functions, incompatible arithmetic/comparisons and incompatible conditional branches are rejected. Metadata types remain runtime-checked because the DSL does not declare their schema.

Metric targets and grouping fields are also checked against their prepared-event scope, including derived payload fields. Numeric aggregations require a supported numeric/boolean target; `count()` takes no target. `distinct_count(value)` counts unique non-null scalar values within each group (zero for an empty input), including text and booleans. Numeric equality treats `1` and `1.0`, and positive and negative zero, as equal; text and booleans remain separate types. Like other scalar calculations, numbers use f64 precision. For active days, use `distinct_count(day_bucket)` with calendar buckets supplied by the pack time semantics. Memory grows with distinct values per group. Unsupported functions and modulo expressions fail compilation rather than deferred metric-plan execution. Metadata remains dynamically typed.

Derived fields must have different names from declared input fields. A derived expression that refers to another derived value must appear after that dependency; forward references and dependency cycles are rejected during compilation.

A tracker can declare named `validations` using the existing condition syntax:

```text
tracker "sleep" v1 {
  fields {
    start: timestamp
    end: timestamp
    quality: int optional
  }
  validations {
    chronological = end > start
    quality_range = quality == null || (quality >= 1 && quality <= 5)
  }
}
```

`validate_event` and strict pack event validation apply these rules after field
normalization. Public `apply` rechecks the same rules before changing engine state,
including for manually constructed events or payloads mutated after validation.
A failed rule rejects the event and identifies the rule by name.
Rules see declared payload fields (also accessible as `payload.field`), not
aggregate metrics, derived fields, or arbitrary functions. Invalid field names,
incompatible expression types, duplicate sections and unknown sections fail at
compile time. Optional fields must explicitly allow absence where intended.
Legacy pack-query preparation and partial event-plan drafts remain separate from
strict acceptance: reading historical data or drafting a plan is not permission
to persist it. Hosts must validate completed events before committing them.

Compatibility: unknown sections that older versions silently ignored are now
errors. Use the canonical `derive` spelling, not `derives`. Optional generated
TypeScript fields include `null`, matching the existing Serde wire format;
consumers must handle absence explicitly. Query optional nulls normalize to
omission, and declared named object/enum/alias types are validated before dispatch.

Generic pack query inputs preserve optional `event_id`, `tracker_id`, and `meta`
through preparation and time normalization. Metadata remains available to derived
expressions. Historical inputs containing only `ts` and `payload` still work;
internal fallback IDs are not exposed as persisted event identities. Rust callers
constructing `PackInputEvent` literals must supply the added fields (`None`, `None`,
and `Value::Null` for legacy inputs). Pack preparation remains a read operation,
not strict acceptance for storage.

Compute plans borrow their immutable tracker definition and cannot outlive it. Reusing a plan with a changed definition fails even if its tracker ID is unchanged. Equivalent deserialized definitions remain accepted. Reusing the original definition uses a pointer comparison, without copying or hashing the definition on each query. Rust consumers storing a `ComputePlan` must retain its source definition and include the plan lifetime in type annotations.

Alerts are evaluated per prepared event, so they may read input fields, derived fields, event metadata and dynamic `meta.*` values. Aggregate metric names are not available in this scope and are rejected at compile time; aggregate alert evaluation is not supported. Duplicate alert names and invalid `signal(name, optional_payload)` calls also fail compilation.

Object literals inside scalar expressions (including signal payload expressions) are unsupported and now fail parsing; older versions incorrectly emitted their source text as a string. Pass a supported scalar expression as a signal payload. JSON configuration blocks outside scalar expressions are unaffected.

Incremental state is owned by `tracen_engine::EngineState<'a>` (moved from `tracen_ir`). Construct it with `EngineState::new(&definition)` or `for_definition`, and insert only through `apply`. Its definition is borrowed and immutable; changed definitions cannot be substituted just because they reuse an ID. Public raw insertion and unchecked `Deserialize` are removed. Serialization retains the `tracker_id`/`events` snapshot shape. Use `restore_state(&definition, json)` to restore: every event is revalidated and stored derives are recomputed before state is returned. Failed restoration leaves existing state untouched. This is an intentional Rust API migration; the definition must outlive the state.

`prepare_events_for_compute` now returns `PreparedEvents<'a>`, an immutable batch borrowing its source definition. Prepared compute APIs require this type instead of raw event slices. Read its events via indexing or `.iter()`; mutable access and deserialization are unavailable. Changed definitions (including reused tracker IDs and empty batches) are rejected. Repeated queries use definition identity instead of rescanning all event IDs. Preparation remains a read/derive operation, not persistence acceptance: producers must validate before storing, and engine state applies that validation at insertion.

Generic pack queries retain supplied event IDs and metadata through view evaluation. Legacy inputs may omit identity and metadata; fallback IDs remain deterministic within a batch. A supplied tracker ID must match the compiled definition, and supplied IDs must be nonblank, payloads must be objects, and metadata must be an object or omitted/null. Preparation and query dispatch share these envelope checks, including custom read-model dispatch. Import adapters must explicitly map foreign tracker identities before querying; a query no longer silently reassigns them. This is a read-envelope guarantee, not full domain write acceptance.

`weighted_avg(value, weight)` computes sum(value × weight) / sum(weight) for eligible numeric values with positive weights. Missing value/weight and zero weight do not contribute; no contributing rows yields null. Negative weights and nonfinite products, sums or results produce explicit errors. Grouping and query filters apply before aggregation. This supports aggregate pace using distance weights, rather than averaging per-run pace equally. The Rust aggregation structs now include an optional `weight` expression (set `None` for other aggregations); absent fields still deserialize and are omitted during serialization. Metric literal `null` now remains null, including conditional branches and comparisons; it is no longer translated to zero. Existing trackers that relied on that incorrect coercion must use an explicit zero instead.
