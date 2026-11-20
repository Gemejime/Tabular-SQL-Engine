"""Synchronize `.env.prod` into `deployments/helm/values.yaml` and detect drift."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

SENSITIVE_KEY_MARKERS: tuple[str, ...] = (
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "KEY",
    "OTEL_EXPORTER_OTLP_HEADERS",
)
TOP_LEVEL_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+:")


def configure_utf8_streams() -> None:
    """Ensure stdout/stderr emit UTF-8 cleanly on Windows consoles."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def is_sensitive_key(key: str) -> bool:
    """Return True if *key* looks like a credential that must stay out of values.yaml."""
    if key == "secretName":
        return False
    upper = key.upper()
    return any(marker in upper for marker in SENSITIVE_KEY_MARKERS)


def _unquote_env_value(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if value[0] in ('"', "'"):
        quote = value[0]
        escaped = False
        for idx in range(1, len(value)):
            ch = value[idx]
            if quote == '"' and escaped:
                escaped = False
                continue
            if quote == '"' and ch == "\\":
                escaped = True
                continue
            if ch == quote:
                trailing = value[idx + 1 :].strip()
                if not trailing or trailing.startswith("#"):
                    inner = value[1:idx]
                    if quote == '"':
                        inner = inner.replace('\\"', '"')
                    return inner
                break
        return value
    if value.startswith("#"):
        return ""
    return re.split(r"\s+#", value, maxsplit=1)[0].rstrip()


def parse_env_file(env_path: Path) -> tuple[dict[str, str], list[str]]:
    """Parse a dotenv file, returning (non_sensitive_env, excluded_sensitive_keys)."""
    if not env_path.is_file():
        raise FileNotFoundError(f"Environment file not found: {env_path}")

    env_vars: dict[str, str] = {}
    excluded: list[str] = []

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue

        key, _, raw_value = line.partition("=")
        key = key.strip()
        if not key:
            continue

        value = _unquote_env_value(raw_value)
        if is_sensitive_key(key):
            excluded.append(key)
            continue

        env_vars[key] = value

    return env_vars, excluded


def _normalize_yaml_env_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def load_helm_env(values_path: Path) -> tuple[dict[str, str], Any, bool]:
    """Load `env` from Helm values.yaml.

    Returns (env_without_secret_name, secret_name_value, has_secret_name).
    """
    if not values_path.is_file():
        raise FileNotFoundError(f"Helm values file not found: {values_path}")

    data = yaml.safe_load(values_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping at root of {values_path}")

    raw_env = data.get("env") or {}
    if not isinstance(raw_env, dict):
        raise ValueError(f"Expected mapping for 'env' in {values_path}")

    has_secret_name = "secretName" in raw_env
    secret_name_val = raw_env.get("secretName")

    env_map: dict[str, str] = {}
    for key, val in raw_env.items():
        if key == "secretName":
            continue
        env_map[str(key)] = _normalize_yaml_env_value(val)

    return env_map, secret_name_val, has_secret_name


def compute_drift(
    expected: dict[str, str],
    actual: dict[str, str],
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Return (missing, extra, changed, forbidden_sensitive) key lists."""
    forbidden_sensitive = sorted(k for k in actual if is_sensitive_key(k))
    missing = sorted(k for k in expected if k not in actual)
    extra = sorted(k for k in actual if k not in expected and not is_sensitive_key(k))
    changed = sorted(k for k in expected if k in actual and actual[k] != expected[k])
    return missing, extra, changed, forbidden_sensitive


def _render_env_block(
    env_vars: dict[str, str],
    secret_name_val: Any,
    has_secret_name: bool,
) -> list[str]:
    lines = ["env:"]
    if has_secret_name:
        if secret_name_val is None:
            lines.append("  secretName: null")
        else:
            lines.append(f"  secretName: {json.dumps(str(secret_name_val))}")
    for key, value in env_vars.items():
        lines.append(f"  {key}: {json.dumps(value)}")
    return lines


def sync_values_file(
    env_vars: dict[str, str],
    values_path: Path,
    secret_name_val: Any,
    has_secret_name: bool,
) -> None:
    """Replace only the top-level `env:` block in values.yaml, preserving all other blocks."""
    original_text = values_path.read_text(encoding="utf-8")
    newline = "\r\n" if "\r\n" in original_text else "\n"
    trailing_newline = original_text.endswith(("\n", "\r\n"))
    lines = original_text.splitlines()

    new_env_lines = _render_env_block(env_vars, secret_name_val, has_secret_name)

    start_idx: int | None = None
    end_idx: int | None = None
    for idx, line in enumerate(lines):
        if start_idx is None:
            if re.match(r"^env:\s*(#.*)?$", line):
                start_idx = idx
        elif TOP_LEVEL_KEY_RE.match(line):
            end_idx = idx
            break

    if start_idx is None:
        if lines and lines[-1].strip():
            lines.append("")
        updated_lines = lines + new_env_lines
    else:
        if end_idx is None:
            end_idx = len(lines)
        # Preserve a single blank line before the next top-level block if one existed.
        had_separator = (
            end_idx < len(lines) and end_idx > start_idx + 1 and not lines[end_idx - 1].strip()
        )
        if had_separator:
            new_env_lines.append("")
        updated_lines = lines[:start_idx] + new_env_lines + lines[end_idx:]

    output = newline.join(updated_lines)
    if trailing_newline:
        output += newline
    values_path.write_text(output, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Sync .env.prod into deployments/helm/values.yaml or check for drift."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--sync",
        action="store_true",
        help="Update deployments/helm/values.yaml from .env.prod.",
    )
    mode.add_argument(
        "--check-only",
        action="store_true",
        help="Verify zero drift between .env.prod and deployments/helm/values.yaml.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=repo_root / ".env.prod",
        help="Path to source .env.prod file.",
    )
    parser.add_argument(
        "--values-file",
        type=Path,
        default=repo_root / "deployments" / "helm" / "values.yaml",
        help="Path to target Helm values.yaml file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_utf8_streams()
    args = build_parser().parse_args(argv)

    try:
        expected_env, excluded_keys = parse_env_file(args.env_file)
        actual_env, secret_name_val, has_secret_name = load_helm_env(args.values_file)
    except (FileNotFoundError, ValueError, yaml.YAMLError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if excluded_keys:
        print(f"Excluded sensitive keys from sync: {', '.join(excluded_keys)}")

    missing, extra, changed, forbidden = compute_drift(expected_env, actual_env)

    if args.check_only:
        if not (missing or extra or changed or forbidden):
            print("OK: Zero drift between .env.prod and deployments/helm/values.yaml.")
            return 0

        print("DRIFT DETECTED between .env.prod and deployments/helm/values.yaml:", file=sys.stderr)
        if missing:
            print(f"  Missing in values.yaml: {', '.join(missing)}", file=sys.stderr)
        if extra:
            print(f"  Extra in values.yaml:   {', '.join(extra)}", file=sys.stderr)
        if changed:
            print(f"  Changed values:         {', '.join(changed)}", file=sys.stderr)
        if forbidden:
            print(
                f"  Forbidden sensitive keys in values.yaml: {', '.join(forbidden)}",
                file=sys.stderr,
            )
        print(
            "Run `python scripts/sync_env_to_deployments.py --sync` to update values.yaml.",
            file=sys.stderr,
        )
        return 1

    if not (missing or extra or changed or forbidden):
        print("Already in sync: deployments/helm/values.yaml matches .env.prod.")
        return 0

    sync_values_file(expected_env, args.values_file, secret_name_val, has_secret_name)
    print(
        f"Synced {len(expected_env)} environment variables from "
        f"{args.env_file} to {args.values_file}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
