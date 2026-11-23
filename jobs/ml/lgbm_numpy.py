"""Chấm điểm mô hình LightGBM từ file JSON (Booster.dump_model) bằng numpy thuần.

Lý do: inference chạy trong container Spark (có numpy, KHÔNG có lightgbm và không được tự cài gói). Logic quyết định nút theo
mã nguồn LightGBM (Tree::NumericalDecision / CategoricalDecision):
  - số: NaN -> 0 nếu missing_type != NaN; (missing_type Zero và |x|<=1e-35) hoặc (missing_type NaN và x là NaN) -> nhánh
    default_left; còn lại x <= threshold -> trái.
  - phân loại: NaN hoặc âm -> phải; int(x) thuộc tập threshold ("a||b||c") -> trái.
Dự báo = tổng giá trị lá của mọi cây (shrinkage và giá trị khởi tạo đã nằm trong lá). Test so với Booster.predict.
"""
import json

import numpy as np

K_ZERO = 1e-35


class _Tree:
    def __init__(self, root: dict):
        feats, thr, cats, left, right, dleft, miss, leaf, val = [], [], [], [], [], [], [], [], []

        def add(node):
            i = len(feats)
            for lst in (feats, thr, cats, left, right, dleft, miss, leaf, val):
                lst.append(None)
            if "leaf_value" in node:
                feats[i], thr[i], cats[i], left[i], right[i] = -1, 0.0, None, -1, -1
                dleft[i], miss[i], leaf[i], val[i] = False, "None", True, float(node["leaf_value"])
                return i
            feats[i], leaf[i], val[i] = int(node["split_feature"]), False, 0.0
            dleft[i], miss[i] = bool(node.get("default_left", False)), node.get("missing_type", "None")
            if node["decision_type"] == "==":
                cats[i], thr[i] = np.array([int(x) for x in str(node["threshold"]).split("||")]), 0.0
            else:
                cats[i], thr[i] = None, float(node["threshold"])
            left[i] = add(node["left_child"])
            right[i] = add(node["right_child"])
            return i

        add(root)
        self.feat, self.thr, self.cats = np.array(feats), np.array(thr), cats
        self.left, self.right = np.array(left), np.array(right)
        self.dleft, self.miss, self.leaf, self.val = np.array(dleft), np.array(miss), np.array(leaf), np.array(val)

    def predict(self, X: np.ndarray) -> np.ndarray:
        node = np.zeros(len(X), dtype=np.int64)
        active = ~self.leaf[node]
        while active.any():
            idx = np.nonzero(active)[0]
            for n in np.unique(node[idx]):
                rows = idx[node[idx] == n]
                x = X[rows, self.feat[n]].astype(np.float64)
                if self.cats[n] is not None:
                    ok = ~np.isnan(x)
                    xi = np.where(ok, x, -1).astype(np.int64)
                    go_left = ok & (xi >= 0) & np.isin(xi, self.cats[n])
                else:
                    m = self.miss[n]
                    isnan = np.isnan(x)
                    if m != "NaN":
                        x = np.where(isnan, 0.0, x)
                    use_default = (np.abs(x) <= K_ZERO) if m == "Zero" else (isnan if m == "NaN" else np.zeros(len(x), bool))
                    go_left = np.where(use_default, self.dleft[n], x <= self.thr[n])
                node[rows] = np.where(go_left, self.left[n], self.right[n])
            active = ~self.leaf[node]
        return self.val[node]


class NumpyBooster:
    def __init__(self, dump: dict):
        self.feature_names = dump["feature_names"]
        self.trees = [_Tree(t["tree_structure"]) for t in dump["tree_info"]]

    @classmethod
    def load(cls, path: str) -> "NumpyBooster":
        with open(path, encoding="utf-8") as f:
            return cls(json.load(f))

    def predict(self, X: np.ndarray) -> np.ndarray:
        X = np.asarray(X, dtype=np.float64)
        out = np.zeros(len(X))
        for t in self.trees:
            out += t.predict(X)
        return out
