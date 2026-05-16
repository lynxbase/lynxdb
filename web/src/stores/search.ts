import { create } from "zustand";
import type {
  QueryResult,
  QueryStats,
  IndexInfo,
  ViewSummary,
  ExplainResult,
  HistogramBucket,
  HistogramBucketGrouped,
  FieldInfo,
} from "../api/client";
import type { TailEvent } from "../api/sse";

interface SearchState {
  query: string;
  from: string;
  to: string | undefined;
  result: QueryResult | null;
  stats: QueryStats | null;
  loading: boolean;
  error: string | null;

  // Part 3: sidebar & timeline
  sidebarVisible: boolean;
  timelineBuckets: HistogramBucket[];
  groupedBuckets: HistogramBucketGrouped[];
  histogramBrushed: boolean;
  hasQueried: boolean;

  // Flow sidebar
  sidebarIndexes: IndexInfo[];
  sidebarViews: ViewSummary[];
  explainResult: ExplainResult | null;
  fieldTypeMap: Map<string, string>;
  catalogFields: FieldInfo[];

  // Part 4: Live Tail
  tailActive: boolean;
  tailEvents: TailEvent[];
  tailNewCount: number;
  tailCatchupDone: boolean;
  tailReconnecting: boolean;

  // Explain inspector
  explainOpen: boolean;

  // Streaming & Progress
  queryActive: boolean;
  streaming: boolean;
  streamingCount: number;
  progressData: {
    percent: number;
    scanned: number;
    total: number;
    elapsedMs: number;
  } | null;
  canceled: boolean;
  elapsedMs: number;
  isPreview: boolean;

  // Pagination, view mode, toolbar
  page: number;
  pageSize: number;
  viewMode: "table" | "list";
  copyTooltip: { visible: boolean; x: number; y: number };
}

export const useSearchStore = create<SearchState>(() => ({
  query: "",
  from: "-1h",
  to: undefined,
  result: null,
  stats: null,
  loading: false,
  error: null,

  sidebarVisible: true,
  timelineBuckets: [],
  groupedBuckets: [],
  histogramBrushed: false,
  hasQueried: false,

  sidebarIndexes: [],
  sidebarViews: [],
  explainResult: null,
  fieldTypeMap: new Map(),
  catalogFields: [],

  tailActive: false,
  tailEvents: [],
  tailNewCount: 0,
  tailCatchupDone: false,
  tailReconnecting: false,

  explainOpen: false,

  queryActive: false,
  streaming: false,
  streamingCount: 0,
  progressData: null,
  canceled: false,
  elapsedMs: 0,
  isPreview: false,

  page: 1,
  pageSize: 100,
  viewMode: "table",
  copyTooltip: { visible: false, x: 0, y: 0 },
}));
