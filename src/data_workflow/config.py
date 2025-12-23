"""Simple configuration objects for project paths."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    """Group all important data directories under a single object."""

    root: Path      # project root directory
    raw: Path       # raw input data
    cache: Path     # cached intermediate data
    processed: Path # cleaned / processed data
    external: Path  # any external / reference data


def make_paths(root: Path) -> Paths:
    """Build a Paths object from a given project root directory."""
    data = root / "data"
    return Paths(
        root=root,
        raw=data / "raw",
        cache=data / "cache",
        processed=data / "processed",
        external=data / "external",
    )