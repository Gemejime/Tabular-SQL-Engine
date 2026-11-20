"""Unit tests for scripts/sync_env_to_deployments.py and Helm ConfigMap/checksum wiring."""

from __future__ import annotations

from pathlib import Path

import yaml

from scripts.sync_env_to_deployments import main as sync_main

REPO_ROOT = Path(__file__).resolve().parents[2]
HELM_DIR = REPO_ROOT / "deployments" / "helm"


def test_sync_and_check_only_preserve_blocks_and_secret_name(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.prod"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "TABULAR_WORKER_LOG_LEVEL=INFO # inline comment",
                "TABULAR_WORKER_POOL_SIZE=8",
                'TABULAR_WORKER_QUOTED_HASH="literal#hash" # trailing comment',
                "TABULAR_WORKER_SINGLE_QUOTED_HASH='single#hash'",
                r'TABULAR_WORKER_S3_BUCKET_ALLOWLIST="[\"bucket-a\",\"bucket-b\"]"',
                "TABULAR_WORKER_OTEL_ENABLED=true",
                "TABULAR_WORKER_OTEL_EXPORTER_ENDPOINT=http://otel:4317",
                "AWS_SECRET_ACCESS_KEY=super-secret",
                "DB_PASSWORD=hunter2",
                "API_TOKEN=tok_123",
                "SIGNING_KEY=key_abc",
                "OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer secret",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    values_file = tmp_path / "values.yaml"
    values_file.write_text(
        "\n".join(
            [
                "replicaCount: 4",
                "image:",
                "  repository: example/worker",
                '  tag: "1.0.0"',
                "env:",
                '  secretName: "my-k8s-secret"',
                '  TABULAR_WORKER_LOG_LEVEL: "DEBUG"',
                '  OTEL_EXPORTER_OTLP_HEADERS: "stale-secret"',
                "keda:",
                "  enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    # Drift detected before sync
    assert (
        sync_main(["--check-only", "--env-file", str(env_file), "--values-file", str(values_file)])
        == 1
    )

    # Sync updates values.yaml
    assert (
        sync_main(["--sync", "--env-file", str(env_file), "--values-file", str(values_file)]) == 0
    )

    # Zero drift after sync
    assert (
        sync_main(["--check-only", "--env-file", str(env_file), "--values-file", str(values_file)])
        == 0
    )

    updated = yaml.safe_load(values_file.read_text(encoding="utf-8"))
    assert updated["replicaCount"] == 4
    assert updated["image"] == {"repository": "example/worker", "tag": "1.0.0"}
    assert updated["keda"] == {"enabled": True}
    assert updated["env"] == {
        "secretName": "my-k8s-secret",
        "TABULAR_WORKER_LOG_LEVEL": "INFO",
        "TABULAR_WORKER_POOL_SIZE": "8",
        "TABULAR_WORKER_QUOTED_HASH": "literal#hash",
        "TABULAR_WORKER_SINGLE_QUOTED_HASH": "single#hash",
        "TABULAR_WORKER_S3_BUCKET_ALLOWLIST": '["bucket-a","bucket-b"]',
        "TABULAR_WORKER_OTEL_ENABLED": "true",
        "TABULAR_WORKER_OTEL_EXPORTER_ENDPOINT": "http://otel:4317",
    }


def test_helm_configmap_and_deployment_checksum_templates() -> None:
    configmap_tpl = (HELM_DIR / "templates" / "configmap.yaml").read_text(encoding="utf-8")
    assert "kind: ConfigMap" in configmap_tpl
    assert 'default "8080" (get .Values.env "PORT")' in configmap_tpl
    assert 'default "50051" (get .Values.env "GRPC_PORT")' in configmap_tpl
    assert 'ne $key "secretName"' in configmap_tpl
    assert "ne $val nil" in configmap_tpl
    assert "range $key, $val := .Values.env" in configmap_tpl

    deployment_tpl = (HELM_DIR / "templates" / "deployment.yaml").read_text(encoding="utf-8")
    expected_checksum = (
        'checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}'
    )
    assert expected_checksum in deployment_tpl
    assert "configMapRef:" in deployment_tpl
    assert "secretRef:" in deployment_tpl

    secret_tpl = (HELM_DIR / "templates" / "secret.yaml").read_text(encoding="utf-8")
    assert "kind: Secret" in secret_tpl
    assert ".Values.secrets.create" in secret_tpl

    values = yaml.safe_load((HELM_DIR / "values.yaml").read_text(encoding="utf-8"))
    assert values["env"]["secretName"] == "rag-tabular-analytics-worker-secrets"
