// AUTO-GENERATED from workout_codegen.tracker. Do not edit.

export const WORKOUT_CODEGEN_ID = "workout_codegen_v1_8a232c54" as const;

export const WORKOUT_CODEGEN_METRIC_KEYS = [
  'sessions',
  'total_calories',
  'total_duration',
] as const;

export const WORKOUT_CODEGEN_VIEW_METRIC_KEYS = {
  summary: [
    'total_calories',
    'total_duration',
  ],
} as const;

export const WORKOUT_CODEGEN_VIEW_DEFAULT_METRIC = {
  summary: 'total_calories',
} as const;

export const WORKOUT_CODEGEN_VIEW_METRIC_CONFIG = {
  summary: {
    total_calories: {
      metric: 'total_calories',
      label: 'Total Calories',
      unit: '',
      modes: [],
      requires: [],
    },
    total_duration: {
      metric: 'total_duration',
      label: 'Total Duration',
      unit: '',
      modes: [],
      requires: [],
    },
  },
} as const;

export type WorkoutCodegenMetricKey = (typeof WORKOUT_CODEGEN_METRIC_KEYS)[number];
export type WorkoutCodegenViewName = keyof typeof WORKOUT_CODEGEN_VIEW_METRIC_KEYS;
export type WorkoutCodegenViewMetricKey<V extends WorkoutCodegenViewName> = (typeof WORKOUT_CODEGEN_VIEW_METRIC_KEYS)[V][number];
