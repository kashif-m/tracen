// AUTO-GENERATED from workout_codegen pack API. Do not edit.

import type { WorkoutCodegenViewMetricKey } from './workout_codegenDslContract';
import type { PackMetricPoint } from './workout_codegenPackCoreApiContract';

export type SummaryMetricKey = WorkoutCodegenViewMetricKey<'summary'>;
export type SummaryGroupByKey =
  | 'exercise';

export interface SummaryQuery {
  metric: SummaryMetricKey;
  group_by: SummaryGroupByKey;
}

export interface SummaryPackQuery {
  view: 'summary';
  metric: SummaryMetricKey;
  group_by: SummaryGroupByKey;
}


export interface SummaryResponse {
  metric: SummaryMetricKey;
  group_by: SummaryGroupByKey;
  points: PackMetricPoint[];
}

export type WorkoutCodegenPackQuery =
  | SummaryPackQuery
;

export type WorkoutCodegenPackResult =
  | SummaryResponse
;
