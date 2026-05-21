// AUTO-GENERATED from workout_codegen pack core domain contracts. Do not edit.


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

export type PackTypeRef<Name extends string> = unknown;

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



export type WorkoutCodegenPackCapabilities = {
  views?: Record<
    string,
    {
      metrics?: string[];
      default_metric?: string;
      metric_config?: Record<
        string,
        {
          metric?: string;
          label?: string;
          unit?: string;
          modes?: string[];
          requires?: string[];
        }
      >;
      query_type?: string;
      pack_query_type?: string;
      response_type?: string;
      result_kind?: string;
      group_by?: string[];
      filters?: Array<{
        key: string;
        field: string;
        op: string;
        type: string;
        optional?: boolean;
      }>;
    }
  >;
  catalog?: Record<
    string,
    {
      base_source?: string;
      schema_type?: string;
      fields: Array<{
        name: string;
        type: string;
        optional?: boolean;
      }>;
    }
  >;
  read_models?: Record<
    string,
    {
      query_type?: string;
      pack_query_type?: string;
      response_type?: string;
      params?: Array<{
        name: string;
        type: string;
        optional?: boolean;
      }>;
    }
  >;
  event_plans?: {
    enabled?: boolean;
  };
};
