"""
io_sorted.py – Deterministic file traversal utilities for Phase‑1.

Provides sorted iteration over files to ensure reproducible order.
"""

from pathlib import Path
from typing import Iterator, Union

def walk_sorted(root: Union[str, Path], pattern: str = "*", recursive: bool = True) -> Iterator[Path]:
    """
    Traverse a directory deterministically, returning files in sorted order.

    Args:
        root: Directory to traverse.
        pattern: Glob pattern (e.g., "*.json").
        recursive: If True, traverse subdirectories.

    Returns:
        Iterator of Path objects sorted by path components then filename.
    """
    root_path = Path(root).resolve()
    if recursive:
        files = list(root_path.rglob(pattern))
    else:
        files = list(root_path.glob(pattern))
    # Sort by full path parts, then filename for deterministic order
    files.sort(key=lambda p: (p.parts, p.name))
    return iter(files)


def read_shards_sorted(directory: Union[str, Path]) -> Iterator[Path]:
    """
    Convenience wrapper: iterate over all .json shards in deterministic order.

    Args:
        directory: Directory containing shard files.

    Returns:
        Iterator of Path objects for each .json file.
    """
    return walk_sorted(directory, pattern="*.json", recursive=True)
