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


@dataclass
class PackageSpec:
    name: str
    cargo_toml: str


PACKAGES = [
    PackageSpec("dbx_example", "plugins/example/Cargo.toml"),
    PackageSpec("dbx_graphql", "plugins/graphql/Cargo.toml"),
    PackageSpec("dbx_grpc", "plugins/grpc/Cargo.toml"),
    PackageSpec("dbx_rest", "plugins/rest/Cargo.toml"),
    PackageSpec("queue", "plugins/queue/Cargo.toml"),
    PackageSpec("postgres", "plugins/postgres/Cargo.toml"),
    PackageSpec("search", "plugins/search/Cargo.toml"),
]


def extract_version(toml_text: str) -> str:
    """Return the value of the version key inside the [package] section."""
    in_package = False
    for raw_line in toml_text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            in_package = stripped == "[package]"
            continue
        if in_package and stripped.startswith("version"):
            key, _, value = stripped.partition("=")
            if not _:
                continue
            version = value.split("#", 1)[0].strip()
            return version.strip('"')
    raise ValueError("Unable to locate version inside [package] section")


def main() -> int:
    base_sha = os.environ.get("BASE_SHA", "").strip()
    changed: list[str] = []

    def note(message: str) -> None:
        print(message, file=sys.stderr)

    if not base_sha:
        note("No base revision detected; scheduling all packages.")
        changed = [pkg.name for pkg in PACKAGES]
    else:
        for pkg in PACKAGES:
            try:
                with open(pkg.cargo_toml, "r", encoding="utf-8") as handle:
                    head_version = extract_version(handle.read())
            except Exception as exc:  # pragma: no cover - defensive
                note(f"[{pkg.name}] unable to read current version ({exc}); scheduling.")
                changed.append(pkg.name)
                continue

            try:
                old_toml = subprocess.check_output(
                    ["git", "show", f"{base_sha}:{pkg.cargo_toml}"],
                    text=True,
                )
                base_version = extract_version(old_toml)
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
