import type { components } from "./generated";

export type Agent = components["schemas"]["Agent"];
export type Query = components["schemas"]["Query"];
export type CreateAgentRequest = components["schemas"]["CreateAgentRequest"];
export type TakeRequest = components["schemas"]["TakeRequest"];
export type InRequest = components["schemas"]["InRequest"];
export type CompleteAgentRequest = components["schemas"]["CompleteAgentRequest"];
export type Event = components["schemas"]["Event"];

type HealthResponse = Record<string, string>;
type AgentListResponse = { agents: Agent[]; total?: number };
type EventListResponse = { events: Event[]; total?: number };
type ReleaseRequest = { reason?: string };

export interface AgentSpaceClientOptions {
  baseUrl: string;
  apiKey?: string;
  headers?: HeadersInit;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}

export interface CompletionHeaders {
  agentId: string;
  leaseToken: string;
  preferMinimal?: boolean;
}

export interface ReleaseHeaders {
  agentId: string;
  leaseToken: string;
  reason?: string;
}

export interface EventQuery {
  tupleId?: string;
  agentId?: string;
  type?: string;
  traceId?: string;
  limit?: number;
  offset?: number;
}

export class AgentSpaceClient {
  private readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly extraHeaders?: HeadersInit;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;

  constructor(options: AgentSpaceClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.apiKey = options.apiKey;
    this.extraHeaders = options.headers;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.timeoutMs = options.timeoutMs ?? 30_000;
  }

  async health(): Promise<HealthResponse> {
    return this.request<HealthResponse>("/health", { method: "GET" });
  }

  async createAgent(body: CreateAgentRequest): Promise<Agent> {
    return this.request<Agent>("/agents", {
      method: "POST",
      body: JSON.stringify(body),
    });
  }

  async getAgent(id: string): Promise<Agent> {
    return this.request<Agent>(`/agents/${id}`, { method: "GET" });
  }

  async queryAgents(query: Query): Promise<AgentListResponse> {
    return this.request<AgentListResponse>("/agents/query", {
      method: "POST",
      body: JSON.stringify(query),
    });
  }

  async takeAgent(body: TakeRequest, agentId?: string): Promise<Agent | null> {
    return this.request<Agent | null>("/agents/take", {
      method: "POST",
      headers: agentId ? { "X-Agent-ID": agentId } : undefined,
      body: JSON.stringify(body),
    });
  }

  async inAgent(body: InRequest, agentId?: string): Promise<Agent | null> {
    return this.request<Agent | null>("/agents/in", {
      method: "POST",
      headers: agentId ? { "X-Agent-ID": agentId } : undefined,
      body: JSON.stringify(body),
    });
  }

  async completeAgent(id: string, body: CompleteAgentRequest, headers: CompletionHeaders): Promise<Agent | null> {
    const requestHeaders: HeadersInit = {
      "X-Agent-ID": headers.agentId,
      "X-Lease-Token": headers.leaseToken,
    };
    if (headers.preferMinimal) {
      (requestHeaders as Record<string, string>).Prefer = "return=minimal";
    }
    return this.request<Agent | null>(`/agents/${id}/complete`, {
      method: "POST",
      headers: requestHeaders,
      body: JSON.stringify(body),
    });
  }

  async releaseAgent(id: string, headers: ReleaseHeaders): Promise<Agent> {
    return this.request<Agent>(`/agents/${id}/release`, {
      method: "POST",
      headers: {
        "X-Agent-ID": headers.agentId,
        "X-Lease-Token": headers.leaseToken,
      },
      body: JSON.stringify({ reason: headers.reason } satisfies ReleaseRequest),
    });
  }

  async getEvents(query: EventQuery = {}): Promise<EventListResponse> {
    return this.request<EventListResponse>("/events", {
      method: "GET",
      query: {
        tuple_id: query.tupleId,
        agent_id: query.agentId,
        type: query.type,
        trace_id: query.traceId,
        limit: query.limit,
        offset: query.offset,
      },
    });
  }

  private async request<T>(
    path: string,
    init: RequestInit & { query?: Record<string, string | number | undefined> },
  ): Promise<T> {
    const url = new URL(`${this.baseUrl}${path}`);
    for (const [key, value] of Object.entries(init.query ?? {})) {
      if (value !== undefined && value !== null) {
        url.searchParams.set(key, String(value));
      }
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await this.fetchImpl(url, {
        ...init,
        headers: this.headers(init.headers),
        signal: controller.signal,
      });

      if (response.status === 204) {
        return null as T;
      }

      if (!response.ok) {
        const message = await response.text();
        throw new Error(`Agent Spaces request failed (${response.status}): ${message}`);
      }

      return (await response.json()) as T;
    } finally {
      clearTimeout(timeout);
    }
  }

  private headers(headers?: HeadersInit): Headers {
    const merged = new Headers({
      Accept: "application/json",
      "Content-Type": "application/json",
    });

    if (this.apiKey) {
      merged.set("X-API-Key", this.apiKey);
    }

    for (const source of [this.extraHeaders, headers]) {
      if (!source) {
        continue;
      }
      new Headers(source).forEach((value, key) => merged.set(key, value));
    }
    return merged;
  }
}
