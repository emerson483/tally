"""
Runner script for the DAO Proposals Fetcher module.

Usage:
    python run_proposals.py

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

    from prop import DAOProposalsFetcher

    fetcher = DAOProposalsFetcher(api_key)

    print("DAO Proposal Fetcher")
    print("=" * 50)

    try:
        dao_name, dao_slugs, governor_ids = fetcher.get_dao_configuration()

        if not governor_ids:
            print("No governor IDs provided, searching for organization...")
            org_data = fetcher.find_dao_organization(dao_name, dao_slugs)
            if org_data:
                governor_ids = org_data.get('governorIds', [])

            if not governor_ids:
                print(f"Could not find governor IDs for {dao_name}")
                print("Please try providing a governor contract address manually.")
                sys.exit(1)

        print(f"\nUsing governor IDs: {governor_ids}")

        print(f"\nChoose an option:")
        print("1. Fetch first 50 proposals and save to CSV")
        print("2. Fetch ALL proposals and save to CSV")
        print("3. Fetch and display proposals (no CSV)")

        choice = input("Enter choice (1, 2, or 3): ").strip()

        if choice == "2":
            all_proposals = fetcher.fetch_all_proposals(dao_name, governor_ids)
            if all_proposals:
                print(f"\nSuccessfully fetched {len(all_proposals)} total proposals!")
                filename = fetcher.save_proposals_to_csv(all_proposals, dao_name)
                show_display = input(f"\nCSV saved as '{filename}'. Display proposals on screen too? (y/n): ").strip().lower()
                if show_display == 'y':
                    show_desc = input("Show full descriptions in display? (y/n): ").strip().lower()
                    fetcher.display_proposals(all_proposals, show_descriptions=(show_desc == 'y'), limit=20)

        elif choice == "1":
            if governor_ids:
                proposals_data = fetcher.fetch_proposals(governor_ids[0], limit=50)
                if proposals_data and "errors" not in proposals_data:
                    proposals = proposals_data["data"]["proposals"]["nodes"]
                    filename = fetcher.save_proposals_to_csv(proposals, dao_name)
                    show_display = input(f"\nCSV saved as '{filename}'. Display first 20 proposals? (y/n): ").strip().lower()
                    if show_display == 'y':
                        fetcher.display_proposals(proposals, limit=20)
                else:
                    print("Failed to fetch proposals")

        else:
            if governor_ids:
                proposals_data = fetcher.fetch_proposals(governor_ids[0], limit=50)
                if proposals_data and "errors" not in proposals_data:
                    proposals = proposals_data["data"]["proposals"]["nodes"]
                    fetcher.display_proposals(proposals, limit=20)
                else:
                    print("Failed to fetch proposals")

        print(f"\nTotal API requests made: {fetcher.request_count}")

    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
