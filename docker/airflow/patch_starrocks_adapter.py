from pathlib import Path

import dbt.adapters.starrocks.connections as connections


path = Path(connections.__file__)
text = path.read_text(encoding="utf-8")

if "def add_begin_query(self):" not in text:
    marker = "    def cancel(self, connection: Connection):\n        connection.handle.close()\n"
    replacement = marker + """
    def add_begin_query(self):
        return None

    def add_commit_query(self):
        return None
"""
    text = text.replace(marker, replacement)
    path.write_text(text, encoding="utf-8")
