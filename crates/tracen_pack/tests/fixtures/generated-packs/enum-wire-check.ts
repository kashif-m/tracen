import type { Unit } from './hydrationPackCoreDomainContract';
import type { UnitEchoPackQuery, UnitEchoResponse } from './hydrationPackCoreApiContract';
const literals: Unit[] = ['fl-oz', 'metric_ml', 'MixedCase'];
const roundTrip = (unit: Unit): UnitEchoResponse => ({ unit });
for (const unit of literals) {
  const query: UnitEchoPackQuery = { read_model: 'unit_echo', unit };
  roundTrip(query.unit);
}
// @ts-expect-error normalized aliases are not DSL literals
const alias: Unit = 'fl_oz';
// @ts-expect-error casing is authoritative
const wrongCase: Unit = 'mixed_case';
// @ts-expect-error unknown values are rejected
const unknown: Unit = 'unknown';
