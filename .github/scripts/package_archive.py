#!/usr/bin/env python3
"""
Create a zip archive from the release directory for a compiled package.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys
import zipfile


def resolve_workspace_root(start: pathlib.Path) -> pathlib.Path:
    current = start
    for candidate in [current, *current.parents]:
        if (candidate / "Cargo.toml").exists() and (candidate / "plugins").exists():
            return candidate
    raise FileNotFoundError("Unable to locate workspace root (Cargo.toml + plugins directory)")


def resolve_plugin_dir(package: str, root_dir: pathlib.Path) -> pathlib.Path:
    plugins_root = root_dir / "plugins"
    for candidate in plugins_root.glob("*/Cargo.toml"):
        try:
            manifest = candidate.read_text(encoding="utf-8")
        except OSError:
            continue
        in_package = False
        for line in manifest.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("["):
                in_package = stripped == "[package]"
                continue
            if not in_package:
                continue
            if stripped.startswith("name") and "=" in stripped:
                _, value = stripped.split("=", 1)
                value = value.split("#", 1)[0].strip().strip('"')
                if value == package:
                    return candidate.parent
                break
    raise FileNotFoundError(f"Unable to find plugin directory for package '{package}'")


def build_archive(
    package: str,
    version: str,
    target: str,
    release_dir: pathlib.Path,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    if not release_dir.exists():
        raise FileNotFoundError(f"release directory does not exist: {release_dir}")

    root_dir = resolve_workspace_root(pathlib.Path.cwd())
    plugin_dir = resolve_plugin_dir(package, root_dir)

    package_dir = output_dir / package / version
    package_dir.mkdir(parents=True, exist_ok=True)
    archive_path = package_dir / f"{target}.zip"

    expected_names = [
        package,
        package.replace("-", "_"),
        package.replace("_", "-"),
    ]
    binaries = []
    seen = set()
    for path in sorted(release_dir.iterdir()):
        if not path.is_file():
            continue
        stem = path.stem if path.suffix else path.name
        if stem in expected_names and stem not in seen:
            binaries.append(path)
            seen.add(stem)

    if not binaries:
        for path in sorted(release_dir.iterdir()):
            if not path.is_file():
                continue
            if os.name == "nt":
                if path.suffix.lower() == ".exe":
                    binaries.append(path)
            else:
                if os.access(path, os.X_OK) and not path.suffix:
                    binaries.append(path)

    if not binaries:
        for path in sorted(release_dir.iterdir()):
            if path.is_file():
                binaries.append(path)
        if not binaries:
            raise FileNotFoundError(f"No files found to include in archive for {package} ({release_dir})")

    extra_files = []
    readme_path = plugin_dir / "README.md"
    if readme_path.exists():
        extra_files.append(readme_path)

    license_names = ["LICENSE", "LICENSE-MIT", "LICENSE-APACHE", "COPYING"]
    license_added = False
    for candidate in license_names:
        candidate_path = plugin_dir / candidate
        if candidate_path.exists():
            extra_files.append(candidate_path)
            license_added = True
    if not license_added:
        for candidate in license_names:
            candidate_path = root_dir / candidate
            if candidate_path.exists():
                extra_files.append(candidate_path)
                license_added = True
                break

    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for binary_path in binaries:
            zf.write(binary_path, binary_path.name)
        for extra_path in extra_files:
            zf.write(extra_path, extra_path.name)

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

    print(str(archive_path), end="")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
