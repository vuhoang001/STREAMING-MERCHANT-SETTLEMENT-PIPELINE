"""Ground-truth sink: ghi mọi giao dịch gian lận ĐÃ TIÊM để sau đối chiếu với FLINK."""

import json 
from normal import now_iso_millis

class LabelWriter: 
    def __init__(self, path: str = "injected_labels.jsonl"):
        self._f = open(path, "a", encoding="utf-8")
    
    def write(self, fraud_type: str, transaction_ids: list, detail: dict = None) -> None: 
        record = {
            "fraud_type": fraud_type,
            "transaction_ids": transaction_ids,
            "detail": detail or {},
            "injected_at": now_iso_millis()
        }
        self._f.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._f.flush()

    def close(self) -> None:
        self._f.close()