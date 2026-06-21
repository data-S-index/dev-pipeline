import json
from pathlib import Path

ANALYSIS_PATH = (
    Path.home()
    / "Downloads"
    / "pulled-database"
    / "fuji-extrapolation-test"
    / "fuji-extrapolation-analysis.json"
)

EXTRA_SCORES = {
    "emdb": 42.31,
}


def main() -> None:
    with open(ANALYSIS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    consistent = [
        (publisher_id, info["score"], info.get("totalCount") or 0)
        for publisher_id, info in data.items()
        if info.get("consistent")
    ]
    consistent.sort(key=lambda x: -x[2])

    lines = ["const HARDCODED_PUBLISHER_ID_SCORES: Record<string, number> = {"]
    for publisher_id, score, _ in consistent:
        lines.append(f'  "{publisher_id}": {score},')
    for publisher_id, score in EXTRA_SCORES.items():
        lines.append(f'  "{publisher_id}": {score},')
    lines.append("};")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
