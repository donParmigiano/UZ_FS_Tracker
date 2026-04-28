"""Run controlled CBU bankstats collection for April 2026 only."""

from src.collectors.cbu_bankstats_test import collect_cbu_april_2026


def main() -> None:
    inventory_df, parse_summary_df = collect_cbu_april_2026()
    print("CBU April 2026 test collection complete.")
    print(f"Inventory rows: {len(inventory_df)}")
    print(f"Parse summary rows: {len(parse_summary_df)}")


if __name__ == "__main__":
    main()
