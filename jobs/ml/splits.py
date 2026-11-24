"""Chia dữ liệu theo thời gian + purge (thuần Python/pandas, không cần Spark). Lý do chi tiết: docs/ml/03_validation_and_leakage.md.

Quy tắc purge: một mẫu train ở gốc t có nhãn là cửa sổ [t+H-5', t+H'), nhãn đó chỉ QUAN SÁT ĐƯỢC từ target_end + A
(cùng quy tắc availability như feature). Nếu mô hình được "huấn luyện tại" ranh giới b (gốc dự báo đầu tiên của tập đánh giá)
thì chỉ được dùng mẫu có target_end + A <= b. Khoảng cách tối thiểu giữa gốc train cuối và b vì vậy = H + A (= 25' với cấu hình
mặc định) - suy ra từ horizon và availability, không chọn tuỳ ý. Feature chỉ nhìn lùi (data_cutoff <= gốc) nên không cần embargo
phía sau tập đánh giá.
"""
from dataclasses import dataclass
import datetime as dt

import pandas as pd


@dataclass(frozen=True)
class Fold:
    name: str
    train_start: pd.Timestamp
    boundary: pd.Timestamp          # = gốc dự báo đầu tiên của tập đánh giá
    eval_start: pd.Timestamp
    eval_end: pd.Timestamp          # [eval_start, eval_end)


def _ts(s) -> pd.Timestamp:
    return pd.Timestamp(s, tz="UTC") if pd.Timestamp(s).tzinfo is None else pd.Timestamp(s).tz_convert("UTC")


def _next_month(t: pd.Timestamp) -> pd.Timestamp:
    return (t + pd.offsets.MonthBegin(1)).normalize()


def walk_forward_folds(split_cfg: dict, mode: str = "expanding") -> list[Fold]:
    """expanding: train từ dev_start; sliding: train chỉ sliding_train_days ngày ngay trước ranh giới."""
    folds = []
    for m in split_cfg["val_months"]:
        b = _ts(m)
        start = _ts(split_cfg["dev_start"]) if mode == "expanding" else b - pd.Timedelta(days=split_cfg["sliding_train_days"])
        folds.append(Fold(f"{mode}_{b:%Y-%m}", start, b, b, min(_next_month(b), _ts(split_cfg["dev_end"]))))
    return folds


def final_fold(split_cfg: dict) -> Fold:
    return Fold("final_test", _ts(split_cfg["dev_start"]), _ts(split_cfg["test_start"]),
                _ts(split_cfg["test_start"]), _ts(split_cfg["test_end"]))


def train_mask(df: pd.DataFrame, fold: Fold, horizon_min: int, avail_min: int) -> pd.Series:
    """Mẫu train hợp lệ: gốc trong [train_start, boundary) VÀ nhãn đã quan sát được trước ranh giới (purge)."""
    label_available = df["target_window_end_utc"] + pd.Timedelta(minutes=avail_min)
    return (df["origin_utc"] >= fold.train_start) & (df["origin_utc"] < fold.boundary) & (label_available <= fold.boundary)


def eval_mask(df: pd.DataFrame, fold: Fold) -> pd.Series:
    """Mẫu đánh giá: gốc trong [eval_start, eval_end) và cửa sổ target kết thúc trong khoảng đánh giá."""
    return (df["origin_utc"] >= fold.eval_start) & (df["origin_utc"] < fold.eval_end) & (df["target_window_end_utc"] <= fold.eval_end)


def inner_early_stopping_split(df_train: pd.DataFrame, days: int, horizon_min: int, avail_min: int):
    """Tách `days` ngày cuối của tập train làm tập early-stopping (cũng purge như trên) -> tập đánh giá không bị dùng để chọn
    số cây. Trả về (mask_fit, mask_es) trên df_train."""
    b = df_train["origin_utc"].max() - pd.Timedelta(days=days)
    label_available = df_train["target_window_end_utc"] + pd.Timedelta(minutes=avail_min)
    return (df_train["origin_utc"] < b) & (label_available <= b), df_train["origin_utc"] >= b


def spatial_holdout_links(links: pd.DataFrame, frac: float, seed: int) -> set:
    """Chọn ~frac số link mỗi borough (phân tầng) làm link KHÔNG xuất hiện trong train."""
    out = set()
    for _, g in links.groupby("borough"):
        k = max(1, round(len(g) * frac))
        out |= set(g.sample(n=k, random_state=seed)["link_id"])
    return out


def to_utc(x: dt.datetime | str) -> pd.Timestamp:
    return _ts(x)
