#!/usr/bin/env python3
"""
Determine which plugin crates changed version numbers relative to a base revision.

This script is meant to be run inside GitHub Actions. It prints a JSON array of
package names that require rebuilding because their Cargo.toml version changed.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PackageSpec:
    name: str
    cargo_toml: str


def parse_package_fields(toml_text: str) -> dict[str, str]:
    """Extract key/value pairs from the [package] section."""
    in_package = False
    data: dict[str, str] = {}
    for raw_line in toml_text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            in_package = stripped == "[package]"
            continue
        if in_package and "=" in stripped:
            key, _, value = stripped.partition("=")
            key = key.strip()
            if not key:
                continue
            version = value.split("#", 1)[0].strip()
            data[key] = version.strip('"')
    if not data:
        raise ValueError("Unable to locate [package] section")
    return data


def discover_packages(root: Path) -> list[PackageSpec]:
    """Return all plugin packages whose name starts with 'dbx_'."""
    packages: list[PackageSpec] = []
    for toml_path in sorted(root.glob("*/Cargo.toml")):
        try:
            toml_text = toml_path.read_text(encoding="utf-8")
            fields = parse_package_fields(toml_text)
            name = fields.get("name")
            if not name:
                raise ValueError("missing 'name' in [package]")
            if not name.startswith("dbx_"):
                print(
                    f"[WARN] Skipping {toml_path}: package name '{name}' does not start with 'dbx_'",
                    file=sys.stderr,
                )
                continue
        except Exception as exc:  # pragma: no cover - defensive
            print(f"[WARN] Unable to inspect {toml_path}: {exc}", file=sys.stderr)
            continue
        packages.append(PackageSpec(name=name, cargo_toml=str(toml_path)))
    return packages


def main() -> int:
    packages = discover_packages(Path("plugins"))
    if not packages:
        print("No plugins with name starting with 'dbx_' found; nothing to schedule.", file=sys.stderr)
        return 0

    base_sha = os.environ.get("BASE_SHA", "").strip()
    changed: list[str] = []

    def note(message: str) -> None:
        print(message, file=sys.stderr)

    if not base_sha:
        note("No base revision detected; scheduling all packages.")
        changed = [pkg.name for pkg in packages]
    else:
        for pkg in packages:
            try:
                with open(pkg.cargo_toml, "r", encoding="utf-8") as handle:
                    head_fields = parse_package_fields(handle.read())
                    head_version = head_fields.get("version")
                    if not head_version:
                        raise ValueError("missing 'version' in [package]")
            except Exception as exc:  # pragma: no cover - defensive
                note(f"[{pkg.name}] unable to read current version ({exc}); scheduling.")
                changed.append(pkg.name)
                continue

            try:
                old_toml = subprocess.check_output(
                    ["git", "show", f"{base_sha}:{pkg.cargo_toml}"],
                    text=True,
                )
                base_fields = parse_package_fields(old_toml)
                base_version = base_fields.get("version")
                if not base_version:
                    raise ValueError("missing 'version' in base [package]")
            except subprocess.CalledProcessError:
                note(f"[{pkg.name}] missing from base revision; scheduling.")
                changed.append(pkg.name)
                continue
            except Exception as exc:  # pragma: no cover - defensive
                note(f"[{pkg.name}] unable to read base version ({exc}); scheduling.")
                changed.append(pkg.name)
                continue

            if head_version != base_version:
                note(f"[{pkg.name}] version changed: {base_version} -> {head_version}")
                changed.append(pkg.name)

    packages_json = json.dumps(changed)
    has_packages = "true" if changed else "false"

    print(f"Detected packages: {packages_json}")
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as handle:
            handle.write(f"packages={packages_json}\n")
            handle.write(f"has-packages={has_packages}\n")
    else:
        note("GITHUB_OUTPUT not set; unable to persist results.")
        print(packages_json)

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
