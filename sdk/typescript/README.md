# Agent Spaces TypeScript SDK

Status: preview

This SDK is generated from the repo OpenAPI spec and wrapped with a small fetch-based client for the most common coordination operations.

## Install

```bash
cd sdk/typescript
npm install
npm run generate:types
npm run build
```

## What It Includes

- generated API types from [`api/openapi.yaml`](../../api/openapi.yaml)
- a thin `AgentSpaceClient` wrapper for create, query, take, `in`, complete, release, events, and health
- browser and Node-compatible fetch usage

## Example

```ts
import { AgentSpaceClient } from "./dist/index.js";

const client = new AgentSpaceClient({
  baseUrl: "http://localhost:8080/api/v1",
});

const created = await client.createAgent({
  kind: "example.task",
  payload: { message: "hello" },
});

console.log(created.id);
```
