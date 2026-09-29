# Generated pack conformance

From the Tracen repository root, run `just conformance /absolute/path/to/tsc` (or `just conformance` when `tsc` is on PATH).

This isolated test crate builds the hydration and sleep fixture DSLs through `tracen_pack::build`, compiles the actual generated Rust adapters, and executes validation and generic daily aggregation. The recipe also strictly type-checks all generated core and compatibility TypeScript contracts. The fixtures require no workout-specific helper implementation or external identity types.

This complements the shared pack runtime validation/reload tests. It does not claim Android JNI coverage or persistence failure recovery.
