#!/usr/bin/env python3
"""
Create a zip archive from the release directory for a compiled package.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import zipfile


def build_archive(
    package: str,
    version: str,
    target: str,
    release_dir: pathlib.Path,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    if not release_dir.exists():
        raise FileNotFoundError(f"release directory does not exist: {release_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    archive_name = f"{package}-{version}-{target}.zip"
    archive_path = output_dir / archive_name

    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in release_dir.rglob("*"):
            if path.is_file():
                zf.write(path, path.relative_to(release_dir))

    return archive_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, help="Package name")
    parser.add_argument("--version", required=True, help="Package version")
    parser.add_argument("--target", required=True, help="Target triple")
    parser.add_argument(
        "--release-dir",
        default="target/release",
        help="Directory containing built release artifacts",
    )
    parser.add_argument(
        "--output-dir",
        default="target/artifacts",
        help="Directory to place the archive file",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    release_dir = pathlib.Path(args.release_dir)
    output_dir = pathlib.Path(args.output_dir)

    try:
        archive_path = build_archive(
            package=args.package,
            version=args.version,
            target=args.target,
            release_dir=release_dir,
            output_dir=output_dir,
        )
    except Exception as exc:  # pragma: no cover - CLI error handling
        print(str(exc), file=sys.stderr)
        return 1

    print(archive_path.name, end="")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
