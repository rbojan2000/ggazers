import json
import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def save_jsonl(data: List[Dict[str, Any]], filepath: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "a", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def add_column(list: List[Dict[str, Any]], column_name: str, value: Any) -> List[Dict[str, Any]]:
    return [{**item, column_name: value} for item in list if item is not None]
