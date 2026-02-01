"""Read and print the first 50 lines from topics_enhanced.ndjson in Downloads."""

from pathlib import Path

downloads = Path.home() / "Downloads"
ndjson_path = downloads / "topics_enhanced.ndjson"

if not ndjson_path.exists():
    print(f"File not found: {ndjson_path}")
else:
    with open(ndjson_path, encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if i >= 50:
                break
            print(line.rstrip())
