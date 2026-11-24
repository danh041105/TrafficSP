"""Metric hồi quy + phân loại trạng thái giao thông suy ra từ tốc độ dự báo (thuần numpy/pandas)."""
import numpy as np
import pandas as pd

STATES = ["FREE_FLOW", "SLOW", "CONGESTED"]


def mae(y, p) -> float:
    y, p = np.asarray(y, float), np.asarray(p, float)
    return float(np.mean(np.abs(y - p)))


def rmse(y, p) -> float:
    y, p = np.asarray(y, float), np.asarray(p, float)
    return float(np.sqrt(np.mean((y - p) ** 2)))


def traffic_state(speed, free_flow, congested_lt=0.5, slow_lt=0.8) -> np.ndarray:
    """Cùng quy tắc common/quality_rules.traffic_state. Trả 'NO_BASELINE' nếu không có free-flow."""
    s, f = np.asarray(speed, float), np.asarray(free_flow, float)
    r = np.divide(s, f, out=np.full_like(s, np.nan), where=(f > 0))
    out = np.where(r < congested_lt, "CONGESTED", np.where(r < slow_lt, "SLOW", "FREE_FLOW")).astype(object)
    out[~(f > 0) | np.isnan(f)] = "NO_BASELINE"
    out[np.isnan(s)] = "NO_VALID_DATA"
    return out


def classification_report(y_state, p_state, labels=STATES) -> dict:
    y, p = np.asarray(y_state), np.asarray(p_state)
    keep = np.isin(y, labels) & np.isin(p, labels)
    y, p = y[keep], p[keep]
    cm = pd.crosstab(pd.Categorical(y, labels), pd.Categorical(p, labels), dropna=False)
    per = {}
    for c in labels:
        tp = int(((y == c) & (p == c)).sum())
        prec = tp / max(1, int((p == c).sum()))
        rec = tp / max(1, int((y == c).sum()))
        per[c] = {"precision": prec, "recall": rec, "f1": 0.0 if prec + rec == 0 else 2 * prec * rec / (prec + rec),
                  "support": int((y == c).sum())}
    return {"confusion": {"labels": labels, "matrix": cm.values.tolist()}, "per_class": per,
            "macro_f1": float(np.mean([per[c]["f1"] for c in labels])), "accuracy": float((y == p).mean()) if len(y) else None,
            "n": int(len(y))}


def grouped_mae(df: pd.DataFrame, by, y="y", p="pred") -> pd.DataFrame:
    g = df.assign(_ae=(df[y] - df[p]).abs(), _se=(df[y] - df[p]) ** 2).groupby(by, observed=True)
    return pd.DataFrame({"mae": g["_ae"].mean(), "rmse": np.sqrt(g["_se"].mean()), "n": g["_ae"].size()}).reset_index()
