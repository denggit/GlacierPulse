#!/usr/bin/env python3
"""Generate and optionally auto-update the repository directory tree."""

from __future__ import annotations

import argparse
import os
import shlex
import stat
import subprocess
import sys
from pathlib import Path


DEFAULT_OUTPUT = "PROJECT_STRUCTURE.md"

EXCLUDED_DIR_NAMES = {
    ".git",
    ".idea",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    ".vscode",
    "__pycache__",
    "data",
    "logs",
    "node_modules",
    "reports",
    "venv",
}

EXCLUDED_FILE_NAMES = {
    ".DS_Store",
    ".env",
}

EXCLUDED_SUFFIXES = {
    ".db",
    ".log",
    ".pyc",
    ".pyo",
    ".sqlite",
    ".sqlite3",
    ".zip",
}


def is_git_ignored(path: Path, root: Path) -> bool:
    """Return whether Git ignore rules exclude this path, including tracked files."""
    try:
        relative = path.relative_to(root).as_posix()
        completed = subprocess.run(
            ["git", "check-ignore", "--quiet", "--no-index", "--", relative],
            cwd=root,
            check=False,
        )
    except (subprocess.SubprocessError, ValueError, FileNotFoundError):
        return False
    return completed.returncode == 0


def find_repo_root() -> Path:
    """Return the git repository root, falling back to the current directory."""
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return Path.cwd().resolve()
    return Path(output).resolve()


def should_skip(path: Path, root: Path, output_path: Path) -> bool:
    if path == output_path:
        return True

    if is_git_ignored(path, root=root):
        return True

    relative = path.relative_to(root)
    if any(part in EXCLUDED_DIR_NAMES for part in relative.parts):
        return True

    if path.name in EXCLUDED_FILE_NAMES:
        return True

    return path.is_file() and path.suffix in EXCLUDED_SUFFIXES


def visible_children(path: Path, root: Path, output_path: Path) -> list[Path]:
    children = [
        child
        for child in path.iterdir()
        if not should_skip(child, root=root, output_path=output_path)
    ]
    return sorted(children, key=lambda child: (child.is_file(), child.name.lower()))


def build_tree(root: Path, output_path: Path) -> str:
    lines = [f"{root.name}/"]

    def walk(directory: Path, prefix: str) -> None:
        children = visible_children(directory, root=root, output_path=output_path)
        for index, child in enumerate(children):
            is_last = index == len(children) - 1
            connector = "`-- " if is_last else "|-- "
            suffix = "/" if child.is_dir() else ""
            lines.append(f"{prefix}{connector}{child.name}{suffix}")
            if child.is_dir():
                child_prefix = "    " if is_last else "|   "
                walk(child, prefix + child_prefix)

    walk(root, "")
    return "\n".join(lines) + "\n"


def render_markdown(root: Path, output_path: Path) -> str:
    relative_output = output_path.relative_to(root).as_posix()
    tree = build_tree(root=root, output_path=output_path)
    return (
        "# 项目目录架构\n\n"
        "> 此文件由 `tools/generate_project_tree.py` 自动生成，请勿手动编辑。\n"
        f"> 手动刷新：`python tools/generate_project_tree.py --output {relative_output}`\n"
        f"> 安装提交前自动刷新：`python tools/generate_project_tree.py --install-hook --output {relative_output}`\n\n"
        "```text\n"
        f"{tree}"
        "```\n"
    )


def generate(output: str) -> Path:
    root = find_repo_root()
    output_path = (root / output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown(root=root, output_path=output_path), encoding="utf-8")
    return output_path


def install_hook(output: str) -> Path:
    root = find_repo_root()
    hook_path = root / ".git" / "hooks" / "pre-commit"
    backup_path = root / ".git" / "hooks" / "pre-commit.before-project-tree"
    script_path = (root / "tools" / "generate_project_tree.py").relative_to(root).as_posix()
    output_path = (root / output).relative_to(root).as_posix()
    marker = "# Managed by tools/generate_project_tree.py"

    hook_body = f"""#!/bin/sh
{marker}
set -eu

if [ -x .git/hooks/pre-commit.before-project-tree ]; then
    .git/hooks/pre-commit.before-project-tree
fi

python3 {shlex.quote(script_path)} --output {shlex.quote(output_path)}
git add {shlex.quote(output_path)}
"""

    if hook_path.exists():
        current = hook_path.read_text(encoding="utf-8")
        if current == hook_body:
            return hook_path
        if marker not in current:
            if backup_path.exists():
                raise SystemExit(f"Refusing to overwrite existing backup: {backup_path.relative_to(root)}")
            hook_path.rename(backup_path)
            print(f"Existing pre-commit hook moved to {backup_path.relative_to(root)}")

    hook_path.write_text(hook_body, encoding="utf-8")
    hook_path.chmod(hook_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return hook_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a Markdown file containing the current project directory tree.",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("PROJECT_TREE_OUTPUT", DEFAULT_OUTPUT),
        help=f"Output Markdown path relative to repository root. Default: {DEFAULT_OUTPUT}",
    )
    parser.add_argument(
        "--install-hook",
        action="store_true",
        help="Install a local git pre-commit hook that refreshes and stages the tree file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = generate(args.output)
    print(f"Generated {output_path.relative_to(find_repo_root())}")

    if args.install_hook:
        hook_path = install_hook(args.output)
        print(f"Installed {hook_path.relative_to(find_repo_root())}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
