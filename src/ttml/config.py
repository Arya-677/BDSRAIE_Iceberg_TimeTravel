"""Configuration loading. One YAML file drives the whole pipeline."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path("conf/pipeline.yaml")


@dataclass(frozen=True)
class Config:
    raw: dict
    root: Path = Path(".")          # project root = parent of conf/; relative paths in the YAML resolve against it

    def path(self, p) -> Path:
        p = Path(p)
        return p if p.is_absolute() else self.root / p

    # --- convenience accessors -------------------------------------------------
    @property
    def catalog(self) -> str:
        return self.raw["catalog"]["name"]

    @property
    def loans_table(self) -> str:
        return self.raw["tables"]["loans"]

    @property
    def registry_table(self) -> str:
        return self.raw["tables"]["registry"]

    @property
    def status_events_table(self) -> str:
        return self.raw["tables"]["status_events"]

    @property
    def ingest_log_table(self) -> str:
        return self.raw["tables"]["ingest_log"]

    @property
    def rollback_log_table(self) -> str:
        return self.raw["tables"]["rollback_log"]

    @property
    def training(self) -> dict:
        return self.raw["training"]

    @property
    def retention(self) -> dict:
        return self.raw["retention"]

    @property
    def validation(self) -> dict:
        return self.raw["validation"]

    @property
    def features_allowlist_path(self) -> Path:
        return self.path(self.raw["data"]["features_allowlist"])

    def features(self) -> list[str]:
        return load_allowlist(self.features_allowlist_path)

    def features_sha256(self) -> str:
        return allowlist_sha256(self.features_allowlist_path)


def load_config(path: str | Path = DEFAULT_CONFIG) -> Config:
    p = Path(path).resolve()
    root = p.parent.parent if p.parent.name == "conf" else p.parent
    with open(p, encoding="utf-8") as f:
        return Config(yaml.safe_load(f), root)


def load_allowlist(path: str | Path) -> list[str]:
    """Feature names, one per line. Blank lines and '#' comments are ignored."""
    out = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line)
    return out


def names_sha256(names: list[str]) -> str:
    return hashlib.sha256("\n".join(names).encode()).hexdigest()


def allowlist_sha256(path: str | Path) -> str:
    """Hash of the *effective* feature list (comments/whitespace do not change it)."""
    return names_sha256(load_allowlist(path))
