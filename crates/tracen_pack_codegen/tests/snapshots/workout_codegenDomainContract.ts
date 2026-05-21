// AUTO-GENERATED from workout_codegen pack domain contracts. Do not edit.

import type { WorkoutCodegenPackCapabilities } from './workout_codegenPackCoreDomainContract';

export type DomainJsonValue =
  | null
  | boolean
  | number
  | string
  | BrandedString
  | DomainJsonObject
  | DomainJsonValue[];

export type DomainJsonObject = {
  [key: string]: DomainJsonValue;
};

export type WorkoutCodegenEvent = DomainJsonObject & {
  event_id: EventId;
  tracker_id: TrackerId;
  ts: number;
  payload: DomainJsonObject;
  meta: DomainJsonObject;
};

export type WorkoutCodegenState = {
  events: WorkoutCodegenEvent[];
};

export type WorkoutCodegenAnalyticsCapabilities = WorkoutCodegenPackCapabilities;
