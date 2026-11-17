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
    assert values["jemalloc"]["enabled"] is False
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
    assert "avg(tabular_worker_active_queries{" in scaledobject_tpl
