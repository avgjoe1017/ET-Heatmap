import os, yaml


def load_sources_cfg(path: str = "configs/sources.yml"):
    """Load source toggles and weights from YAML.
    Returns a dict like { name: {enabled: bool, weight: float} }
    """
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    return doc.get("sources", {})


def is_enabled(name: str) -> bool:
    s = load_sources_cfg()
    cfg = s.get(name, {})
    return bool(cfg.get("enabled", False))


def weight_of(name: str, default: float = 1.0) -> float:
    s = load_sources_cfg()
    cfg = s.get(name, {})
    try:
        return float(cfg.get("weight", default))
    except Exception:
        return float(default)
