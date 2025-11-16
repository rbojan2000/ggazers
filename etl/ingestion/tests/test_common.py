import json
import os
import tempfile

from src.common import save_json


def test_save_json_new_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        filepath = os.path.join(temp_dir, "test.json")
        test_data = {"key": "value", "number": 42}

        save_json(test_data, filepath)

        assert os.path.exists(filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            saved_data = json.load(f)
        assert saved_data == [test_data]


def test_save_json_append_to_existing():
    with tempfile.TemporaryDirectory() as temp_dir:
        filepath = os.path.join(temp_dir, "test.json")
        existing_data = [{"existing": "data"}]
        new_data = {"new": "data"}

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(existing_data, f)

        save_json(new_data, filepath)

        with open(filepath, "r", encoding="utf-8") as f:
            saved_data = json.load(f)
        assert len(saved_data) == 2
        assert saved_data[0] == {"existing": "data"}
        assert saved_data[1] == {"new": "data"}
