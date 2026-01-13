"""
Runner script for the DAO Delegation Statements Fetcher module.

Usage:
    python run_statements.py

Ensure TALLY_API_KEY environment variable is set before running.
"""

import os
import sys


def main():
    api_key = os.getenv("TALLY_API_KEY")
    if not api_key:
        print("Error: TALLY_API_KEY environment variable is not set.")
        print("Set it using: set TALLY_API_KEY=your_api_key_here")
        sys.exit(1)

    from statement import DAODelegationStatementsFetcher

    fetcher = DAODelegationStatementsFetcher(api_key)

    print("=== DAO Delegation Statements Fetcher ===")
    print("This will fetch delegation statements in raw API format")
    print("Progress saved every 20 pages")
    print()

    try:
        dao_name, dao_slugs = fetcher.get_dao_configuration()

        print(f"\nFetching all {dao_name} delegation statements...")

        delegation_statements = fetcher.get_all_delegation_statements(dao_name, dao_slugs)

        if delegation_statements:
            dao_slug = dao_name.lower().replace(' ', '_').replace('dao', '').strip('_')
            filename = f"{dao_slug}_delegation_statements_raw.json"

            fetcher.save_delegation_statements(delegation_statements, filename, dao_name)
            print(f"\nSUCCESS: Saved {len(delegation_statements)} delegation statements for {dao_name}")
            print("Files saved in both JSON (raw API format) and CSV (flattened) formats")
        else:
            print(f"No delegation statements found for {dao_name}")

    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
