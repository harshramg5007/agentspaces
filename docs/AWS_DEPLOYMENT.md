# AWS Deployment

This guide shows the public deployment path for Agent Spaces on AWS: run Postgres separately, then deploy the server on EKS with the bundled Helm chart.

## Prerequisites

- An existing EKS cluster with `kubectl` access
- A reachable Postgres instance for the coordination store
- Helm v3+

## Quick Start

```bash
helm upgrade --install agentspaces ./deploy/helm/agentspaces \
  --namespace agentspaces \
  --create-namespace \
  --set image.repository=ghcr.io/urobora-ai/agentspaces \
  --set image.tag=v1.0.0 \
  --set service.type=LoadBalancer \
  --set env.postgresHost=your-postgres-host \
  --set env.postgresPort=5432 \
  --set env.postgresUser=agents \
  --set env.postgresPassword=agents \
  --set env.postgresDatabase=agents
```

After the service is ready, fetch the AWS load balancer hostname:

```bash
kubectl get svc -n agentspaces
```

## Cleanup

```bash
helm uninstall agentspaces -n agentspaces
```

## Notes

- The chart lives in [`deploy/helm/agentspaces`](../deploy/helm/agentspaces).
- `service.type=LoadBalancer` lets AWS provision an external endpoint for the HTTP API.
- Keep Postgres managed separately; the public chart only deploys the Agent Spaces server.
