# Secret Management (`rag-tabular-analytics-worker`)

This service separates **non-sensitive runtime configuration** (stored in `.env.prod` and synced to the Helm `ConfigMap`) from **sensitive credentials** (injected into a Kubernetes `Secret` via CI/CD).

---

## 1. Architecture Overview

| Category | Examples | Storage / Source of Truth | Kubernetes Mechanism |
| :--- | :--- | :--- | :--- |
| **Non-sensitive config** | `TABULAR_WORKER_POOL_SIZE`, `TABULAR_WORKER_S3_ENDPOINT_URL`, `TABULAR_WORKER_S3_REGION`, `TABULAR_WORKER_S3_BUCKET_ALLOWLIST` | `.env.prod` -> `deployments/helm/values.yaml` (via `scripts/sync_env_to_deployments.py --sync`) | `ConfigMap` (`<release>-config`) via `envFrom.configMapRef` |
| **Sensitive credentials** | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (Cloudflare R2 S3 API tokens), `OTEL_EXPORTER_OTLP_HEADERS` | GitHub Actions Environment Secrets (`staging`, `prod`) | `Secret` (`rag-tabular-analytics-worker-secrets`) via `envFrom.secretRef` |

> [!IMPORTANT]
> Never commit credentials to `.env.example`, `deployments/helm/values*.yaml`, or Git.
> `scripts/sync_env_to_deployments.py` automatically excludes any key containing `PASSWORD`, `SECRET`, `TOKEN`, or `KEY` (while preserving `env.secretName`).

---

## 2. Cloudflare R2 S3 Configuration

[`internal/storage/s3_streamer.py`](../internal/storage/s3_streamer.py) uses `aiobotocore` / `botocore`, which reads standard AWS S3 credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) from the pod environment.

### Non-sensitive R2 settings (`.env.prod` / `ConfigMap`)

Set these in `.env.prod` and sync to `deployments/helm/values.yaml`:

```dotenv
TABULAR_WORKER_S3_ENDPOINT_URL=https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com
TABULAR_WORKER_S3_REGION=auto
TABULAR_WORKER_S3_BUCKET_ALLOWLIST=["your-r2-bucket"]
```

### Sensitive R2 credentials (Kubernetes `Secret`)

Create an R2 API token in the Cloudflare dashboard with **Object Read only** (or Read & Write if needed) scoped to the target bucket(s), and store its S3-compatible credentials in GitHub Environment Secrets:

- `R2_ACCESS_KEY_ID` -> injected as `AWS_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY` -> injected as `AWS_SECRET_ACCESS_KEY`

---

## 3. Helm & Kubernetes Wiring

1. **`deployments/helm/values.yaml`** sets:
   ```yaml
   env:
     secretName: "rag-tabular-analytics-worker-secrets"
   ```
2. **`deployments/helm/templates/deployment.yaml`** mounts both the `ConfigMap` and `Secret` via `envFrom`:
   ```yaml
   envFrom:
     - configMapRef:
         name: {{ include "rag-tabular-analytics-worker.fullname" . }}-config
     {{- if .Values.env.secretName }}
     - secretRef:
         name: {{ .Values.env.secretName }}
     {{- end }}
   ```
3. **`deployments/helm/templates/configmap.yaml`** explicitly skips `secretName` and `nil` values so the `Secret` name is never emitted into the `ConfigMap`.
4. **`deployments/helm/templates/secret.yaml`** is disabled by default (`secrets.create: false`) so Helm never stores plaintext secrets in release values when CI/CD injects `rag-tabular-analytics-worker-secrets` directly.

---

## 4. CI/CD Secret Injection (GitHub Actions)

[`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) handles secret injection and Helm deployment on `workflow_dispatch`:

1. **Configure GitHub Environments** (`staging` and `prod`) under **Settings -> Environments** in GitHub.
2. **Add the following Environment Secrets** to each environment:
   | GitHub Secret | Required | Description |
   | :--- | :--- | :--- |
   | `KUBE_CONFIG_DATA` | Yes | Base64-encoded `kubeconfig` with deploy permissions for the target cluster/namespace. |
   | `R2_ACCESS_KEY_ID` | Yes | Cloudflare R2 S3 Access Key ID (mapped to `AWS_ACCESS_KEY_ID`). |
   | `R2_SECRET_ACCESS_KEY` | Yes | Cloudflare R2 S3 Secret Access Key (mapped to `AWS_SECRET_ACCESS_KEY`). |
   | `OTEL_EXPORTER_OTLP_HEADERS` | No | Optional auth headers for OpenTelemetry OTLP exporter. |
3. **How injection works safely:**
   - The workflow writes secrets to a temporary `chmod 600` file (`mktemp` with `trap 'rm -f ...' EXIT`) rather than passing secret values on the `kubectl` command line.
   - It runs `kubectl create secret generic rag-tabular-analytics-worker-secrets --from-env-file=... --dry-run=client -o yaml | kubectl apply -f -` to idempotently create or update the `Secret` in `rag-tabular-staging` or `rag-tabular-prod`.
   - It then runs `helm upgrade --install` with `values.yaml` and the environment overlay (`values-staging.yaml` or `values-prod.yaml`).

---

## 5. Local Development & Manual Operations

### Local Development
Put your development R2 (or local MinIO) credentials in `.env` (which is gitignored):

```dotenv
AWS_ACCESS_KEY_ID=your-dev-r2-access-key-id
AWS_SECRET_ACCESS_KEY=your-dev-r2-secret-access-key
TABULAR_WORKER_S3_ENDPOINT_URL=https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com
TABULAR_WORKER_S3_REGION=auto
```

### Secret Rotation
When rotating Cloudflare R2 credentials:
1. Update `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY` in the GitHub Environment.
2. Re-run the **Deploy** workflow (or apply the updated `Secret` manually), then trigger a rolling restart so pods pick up the new environment variables:
   ```bash
   kubectl rollout restart deployment/rag-tabular-analytics-worker -n rag-tabular-prod
   ```
