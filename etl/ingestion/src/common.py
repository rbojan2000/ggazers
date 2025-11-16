import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)


def save_json(data: Dict[str, Any], filepath: str, encoding: str = "utf-8") -> None:
    logger.info(f"Saving data to {filepath}")
    dirpath = os.path.dirname(filepath)
    if dirpath and not os.path.exists(dirpath):
        logger.info(f"Creating directory {dirpath}")
        os.makedirs(dirpath)

    if os.path.exists(filepath):
        logger.info(f"Appending data to existing file {filepath}")
        with open(filepath, "r", encoding=encoding) as f:
            existing = json.load(f)
        existing.append(data)
        with open(filepath, "w", encoding=encoding) as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
    else:
        with open(filepath, "w", encoding=encoding) as f:
            json.dump([data], f, ensure_ascii=False, indent=2)
