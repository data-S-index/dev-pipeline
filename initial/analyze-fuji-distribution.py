"""Analyze Fuji score distribution by DOI prefix from NDJSON files."""

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict

from tqdm import tqdm


def extract_doi_prefix(doi: str) -> str:
    """Extract DOI prefix (e.g., '10.5517' from '10.5517/cc7gs7p')."""
    if not doi or not doi.startswith("10."):
        return None

    # Find the first '/' after '10.'
    slash_index = doi.find("/")
    if slash_index > 0:
        return doi[:slash_index]
    # If no slash found, return the whole DOI as prefix
    return doi


def process_ndjson_files(input_dir: Path) -> Dict[str, Dict[str, int]]:
    """
    Process all NDJSON files and create score distribution by DOI prefix.

    Returns:
        Dictionary mapping DOI prefix to a dictionary of score -> count
    """
    # Structure: {doi_prefix: {score: count}}
    distribution: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    # Find all NDJSON files
    ndjson_files = sorted(input_dir.glob("*.ndjson"))

    if not ndjson_files:
        raise FileNotFoundError(f"No NDJSON files found in {input_dir}")

    print(f"Found {len(ndjson_files):,} NDJSON files to process")

    total_records = 0
    records_with_score = 0
    records_without_score = 0
    records_without_doi = 0

    # Process each file
    for ndjson_file in tqdm(ndjson_files, desc="Processing files", unit="file"):
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                    total_records += 1

                    doi = record.get("doi")
                    if not doi:
                        records_without_doi += 1
                        continue

                    score = record.get("score")
                    if score is None:
                        records_without_score += 1
                        continue

                    # Extract DOI prefix
                    doi_prefix = extract_doi_prefix(doi)
                    if not doi_prefix:
                        continue

                    # Convert score to string for consistent key (handle floats)
                    score_str = str(score)

                    # Increment count for this prefix and score combination
                    distribution[doi_prefix][score_str] += 1
                    records_with_score += 1

                except json.JSONDecodeError as e:
                    print(f"\n⚠️  Error parsing JSON in {ndjson_file}: {e}")
                    continue
                except Exception as e:
                    print(f"\n⚠️  Error processing record in {ndjson_file}: {e}")
                    continue

    print("\n📊 Processing Summary:")
    print(f"    - Total records processed: {total_records:,}")
    print(f"    - Records with score: {records_with_score:,}")
    print(f"    - Records without score: {records_without_score:,}")
    print(f"    - Records without DOI: {records_without_doi:,}")
    print(f"    - Unique DOI prefixes: {len(distribution):,}")

    return distribution


def save_distribution(
    distribution: Dict[str, Dict[str, int]], output_file: Path
) -> None:
    """Save the distribution to a JSON file."""
    # Convert defaultdict to regular dict for JSON serialization
    # Sort scores by count (highest first) for each prefix
    output_data = {}
    for prefix, score_counts in distribution.items():
        # Sort by count (descending) - highest occurring scores first
        sorted_scores = sorted(score_counts.items(), key=lambda x: x[1], reverse=True)
        output_data[prefix] = dict(sorted_scores)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Distribution saved to: {output_file}")


def main() -> None:
    """Main function to analyze score distribution by DOI prefix."""
    print("🚀 Starting Fuji score distribution analysis...")

    # Get OS-agnostic paths (matching generate-fuji-files.py pattern)
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    input_dir = downloads_dir / "database" / "fuji-score"
    output_file = downloads_dir / "database" / "fuji-score-distribution.json"

    print(f"Input directory: {input_dir}")
    print(f"Output file: {output_file}")

    # Check if input directory exists
    if not input_dir.exists():
        raise FileNotFoundError(
            f"Directory not found: {input_dir}. "
            f"Please run generate-fuji-files.py first to create the NDJSON files."
        )

    # Process all NDJSON files
    print("\n📖 Processing NDJSON files...")
    distribution = process_ndjson_files(input_dir)

    # Save distribution to JSON file
    print("\n💾 Saving distribution...")
    save_distribution(distribution, output_file)

    # Print some statistics
    print("\n📈 Distribution Statistics:")
    total_prefixes = len(distribution)
    total_score_combinations = sum(len(scores) for scores in distribution.values())
    total_records_in_distribution = sum(
        sum(counts.values()) for counts in distribution.values()
    )

    print(f"    - Total DOI prefixes: {total_prefixes:,}")
    print(
        f"    - Total unique (prefix, score) combinations: {total_score_combinations:,}"
    )
    print(f"    - Total records in distribution: {total_records_in_distribution:,}")

    # Show top 10 prefixes by record count
    prefix_counts = {
        prefix: sum(counts.values()) for prefix, counts in distribution.items()
    }
    top_prefixes = sorted(prefix_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    print("\n🔝 Top 10 DOI prefixes by record count:")
    for i, (prefix, count) in enumerate(top_prefixes, 1):
        unique_scores = len(distribution[prefix])
        print(
            f"    {i:2d}. {prefix:20s} - {count:>10,} records ({unique_scores} unique scores)"
        )

    print("\n✅ Analysis completed successfully!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
