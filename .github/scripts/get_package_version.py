#!/usr/bin/env python3
"""
Emit the version of the requested Cargo package via GitHub Actions outputs.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, help="Package name to inspect.")
    parser.add_argument(
        "--manifest-path",
        help="Optional path to Cargo.toml; defaults to workspace root.",
    )
    args = parser.parse_args()

    metadata_cmd = [
        "cargo",
        "metadata",
        "--no-deps",
        "--format-version",
        "1",
    ]
    if args.manifest_path:
        metadata_cmd.extend(["--manifest-path", args.manifest_path])

    try:
        result = subprocess.run(
            metadata_cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - defensive
        print(exc.stderr or exc.stdout, file=sys.stderr)
        return exc.returncode

    try:
        metadata = json.loads(result.stdout)
        package = next(
            pkg for pkg in metadata["packages"] if pkg["name"] == args.package
        )
    except (KeyError, StopIteration) as exc:
        print(f"package '{args.package}' not found in cargo metadata ({exc})", file=sys.stderr)
        return 1

    version = package.get("version")
    if not version:
        print(f"package '{args.package}' missing version field", file=sys.stderr)
        return 1

    output_line = f"version={version}"
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write(output_line + "\n")
    else:
        print(output_line)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
