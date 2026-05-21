// AUTO-GENERATED from workout_codegen pack core API. Do not edit.

import type { WorkoutCodegenViewMetricKey } from './workout_codegenDslContract';
import type {
  PackTypeRef,
} from './workout_codegenPackCoreDomainContract';

export interface PackMetricPoint {
  label: string;
  value: number;
  count: number;
  bucket: number;
}

export interface PackDistributionItem {
  label: string;
  value: number;
  percentage: number;
}


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
