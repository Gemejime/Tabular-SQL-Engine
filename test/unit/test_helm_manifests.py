"""Unit tests validating Helm chart structure, memory limits, and KEDA ScaledObject."""

from __future__ import annotations

from pathlib import Path

import yaml

HELM_DIR = Path(__file__).resolve().parents[2] / "deployments" / "helm"


def test_helm_chart_and_values_conform_to_architecture_budgets() -> None:
    chart = yaml.safe_load((HELM_DIR / "Chart.yaml").read_text(encoding="utf-8"))
    assert chart["apiVersion"] == "v2"
    assert chart["name"] == "rag-tabular-analytics-worker"

    values = yaml.safe_load((HELM_DIR / "values.yaml").read_text(encoding="utf-8"))
    assert values["resources"]["requests"]["memory"] == "4Gi"
    assert values["resources"]["limits"]["memory"] == "6Gi"
    assert values["resources"]["requests"]["cpu"] == "2000m"
    assert values["resources"]["limits"]["cpu"] == "4000m"
    assert values["terminationGracePeriodSeconds"] >= 20
    assert values["cache"]["sizeLimit"] == "10Gi"
    assert values["jemalloc"]["enabled"] is True
    assert values["jemalloc"]["path"] == "/usr/local/lib/libjemalloc.so"
    assert values["securityContext"]["runAsNonRoot"] is True
    assert "ALL" in values["securityContext"]["capabilities"]["drop"]

    keda = values["keda"]
    assert keda["enabled"] is True
    assert keda["minReplicaCount"] == 4
    assert keda["maxReplicaCount"] == 24
    assert int(keda["prometheus"]["threshold"]) < int(
        values["env"]["TABULAR_WORKER_MAX_CONCURRENT_QUERIES"]
    )


def test_helm_templates_contain_required_k8s_and_keda_directives() -> None:
    deployment_tpl = (HELM_DIR / "templates" / "deployment.yaml").read_text(encoding="utf-8")
    assert 'medium: ""' in deployment_tpl
    assert "medium: Memory\n            sizeLimit: 10Gi" not in deployment_tpl
    assert "terminationGracePeriodSeconds" in deployment_tpl
    assert "path: /healthz" in deployment_tpl
    assert "path: /ready" in deployment_tpl
    assert "LD_PRELOAD" in deployment_tpl
    assert ".Values.jemalloc.enabled" in deployment_tpl

    service_tpl = (HELM_DIR / "templates" / "service.yaml").read_text(encoding="utf-8")
    assert "name: grpc" in service_tpl
    assert "name: http" in service_tpl
    assert "clusterIP: None" in service_tpl

    scaledobject_tpl = (HELM_DIR / "templates" / "scaledobject.yaml").read_text(encoding="utf-8")
    assert "apiVersion: keda.sh/v1alpha1" in scaledobject_tpl
    assert "kind: ScaledObject" in scaledobject_tpl
    assert "minReplicaCount:" in scaledobject_tpl
    assert "maxReplicaCount:" in scaledobject_tpl
    assert "sum(tabular_worker_active_queries{" in scaledobject_tpl


def test_dockerfile_and_dockerignore_harden_nonroot_jemalloc_runtime() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dockerfile = (repo_root / "deployments" / "docker" / "Dockerfile").read_text(encoding="utf-8")
    assert "FROM python:3.12-slim AS builder" in dockerfile
    assert "FROM python:3.12-slim AS runtime" in dockerfile
    assert "libjemalloc2" in dockerfile
    assert "/usr/local/lib/libjemalloc.so" in dockerfile
    assert "LD_PRELOAD=/usr/local/lib/libjemalloc.so" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert "EXPOSE 8815 8080" in dockerfile
    assert "HEALTHCHECK" in dockerfile
    assert 'CMD ["python", "-m", "cmd.worker.main"]' in dockerfile

    dockerignore = (repo_root / ".dockerignore").read_text(encoding="utf-8")
    assert ".venv" in dockerignore
    assert ".git" in dockerignore


def test_helm_staging_and_prod_overlays_and_argocd_canary_manifests() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    staging = yaml.safe_load((HELM_DIR / "values-staging.yaml").read_text(encoding="utf-8"))
    assert staging["keda"]["minReplicaCount"] == 1
    assert staging["keda"]["maxReplicaCount"] == 6
    assert staging["env"]["TABULAR_WORKER_OTEL_ENABLED"] == "true"
    assert staging["jemalloc"]["enabled"] is True

    prod = yaml.safe_load((HELM_DIR / "values-prod.yaml").read_text(encoding="utf-8"))
    assert prod["keda"]["minReplicaCount"] == 4
    assert prod["keda"]["maxReplicaCount"] == 24
    assert prod["resources"]["requests"]["memory"] == "4Gi"
    assert prod["resources"]["limits"]["memory"] == "6Gi"
    assert prod["cache"]["sizeLimit"] == "10Gi"
    assert prod["jemalloc"]["enabled"] is True
    assert prod["jemalloc"]["path"] == "/usr/local/lib/libjemalloc.so"

    argocd_dir = repo_root / "deployments" / "argocd"
    app_staging = yaml.safe_load((argocd_dir / "application-staging.yaml").read_text("utf-8"))
    assert app_staging["kind"] == "Application"
    assert "values-staging.yaml" in app_staging["spec"]["source"]["helm"]["valueFiles"]

    app_prod = yaml.safe_load((argocd_dir / "application-prod.yaml").read_text("utf-8"))
    assert app_prod["kind"] == "Application"
    assert "values-prod.yaml" in app_prod["spec"]["source"]["helm"]["valueFiles"]

    docs = list(yaml.safe_load_all((argocd_dir / "rollout-canary.yaml").read_text("utf-8")))
    kinds = {doc["kind"]: doc for doc in docs}
    assert "AnalysisTemplate" in kinds
    assert "Rollout" in kinds

    rollout = kinds["Rollout"]
    assert rollout["spec"]["workloadRef"]["kind"] == "Deployment"
    weights = [
        step["setWeight"]
        for step in rollout["spec"]["strategy"]["canary"]["steps"]
        if "setWeight" in step
    ]
    assert weights == [10, 25, 50, 100]

    analysis_text = (argocd_dir / "rollout-canary.yaml").read_text("utf-8")
    for metric_name in (
        "tabular_worker_query_duration_seconds",
        "tabular_worker_subprocess_recycles_total",
        "tabular_worker_cache_hit_ratio",
        "tabular_worker_active_queries",
    ):
        assert metric_name in analysis_text
