"""
Runner script for the Voting Matrix Builder module.

Usage:
    python run_voting_matrix.py

Ensure TALLY_API_KEY environment variable is set before running.
"""

import os
import sys
import time


def main():
    api_key = os.getenv("TALLY_API_KEY")
    if not api_key:
        print("Error: TALLY_API_KEY environment variable is not set.")
        print("Set it using: set TALLY_API_KEY=your_api_key_here")
        sys.exit(1)

    from voting_matrix import FastDAOFetcher, DAOConfig
    import pandas as pd

    print("=== DAO Voting Matrix Builder ===")
    print("=" * 60)
    print("Optimized for maximum speed and efficiency")
    print("Smart rate limiting with adaptive delays")
    print("Minimal file I/O - only save final results")

    print(f"\nDAO SELECTION:")
    print(f"1. Compound DAO (17,894 delegates, high vote volume)")
    print(f"2. ENS DAO (37,150 delegates)")
    print(f"3. Custom DAO")

    try:
        choice = input(f"\nSelect (1-3): ").strip()

        if choice == "1":
            dao_config = DAOConfig.get_compound_config()
        elif choice == "2":
            dao_config = DAOConfig.get_ens_config()
        elif choice == "3":
            name = input("DAO name: ").strip()
            slug = input("DAO slug: ").strip()
            expected = int(input("Expected delegates: ").strip() or "1000")
            dao_config = DAOConfig.get_custom_config(name, slug, [], expected)
        else:
            print("Invalid choice, using Compound DAO")
            dao_config = DAOConfig.get_compound_config()
    except Exception:
        print("Error in selection, using Compound DAO")
        dao_config = DAOConfig.get_compound_config()

    print(f"\nSETTINGS:")
    print(f"   DAO: {dao_config.name}")
    print(f"   Vote batch size: 5,000 votes per request")
    print(f"   Max pages per proposal: 100,000 (virtually unlimited)")
    print(f"   Vote caching enabled")

    try:
        use_force_refresh = input("Force refresh votes (ignore cached)? (y/N): ").strip().lower() == 'y'
    except Exception:
        use_force_refresh = False

    try:
        confirm = input(f"\nStart extraction? (y/N): ").strip().lower()
        if confirm != 'y':
            print("Cancelled.")
            return
    except Exception:
        print("Cancelled.")
        return

    fetcher = FastDAOFetcher(api_key=api_key, dao_config=dao_config, force_refresh_votes=use_force_refresh)
    total_start_time = time.time()

    try:
        print(f"\nStep 1: Finding organization...")
        result = fetcher.get_organization_by_slug(dao_config.slug)

        if not result.get('data', {}).get('organization'):
            print(f"Could not find {dao_config.name}")
            sys.exit(1)

        org = result['data']['organization']
        org_id = org['id']
        print(f"Found: {org['name']} (ID: {org_id})")
        print(f"   Proposals: {org.get('proposalsCount', 'N/A')}")
        print(f"   Delegates: {org.get('delegatesCount', 'N/A')}")
        print(f"   Total votes: {org.get('delegatesVotesCount', 'N/A')}")

        print(f"\nStep 2: Fetching delegates...")
        delegates = fetcher.get_all_delegates_optimized(org_id)

        if not delegates:
            print(f"No delegates found")
            sys.exit(1)

        print(f"Retrieved {len(delegates):,} delegates")

        print(f"\nStep 3: Fetching proposals...")
        proposals = fetcher.get_all_proposals_optimized(org_id)

        if not proposals:
            print(f"No proposals found")
            sys.exit(1)

        print(f"Retrieved {len(proposals)} proposals")

        print(f"\nStep 4: Creating voting matrix...")
        fetcher.create_voting_matrix_fast(proposals, delegates, org_id)

        total_time_hours = (time.time() - total_start_time) / 3600

        print(f"\nEXTRACTION COMPLETE!")
        print("=" * 60)

        print(f"DAO: {dao_config.name}")
        print(f"Total time: {total_time_hours:.1f} hours")
        print(f"API requests: {fetcher.api.request_count:,}")

        api_stats = fetcher.api.get_stats()
        print(f"\nPERFORMANCE METRICS:")
        print(f"   API requests made: {api_stats['total_requests']:,}")
        print(f"   Success rate: {api_stats['success_rate']}")
        print(f"   Processing time: {total_time_hours:.1f} hours")
        print(f"   Rate limiting: {api_stats['current_delay']} between requests")
        print(f"   API efficiency: {api_stats['efficiency']}")

        print(f"\nCleaning up...")
        fetcher.cleanup_checkpoint_files()

        print(f"\nSUCCESS! {dao_config.name} extraction completed!")

    except KeyboardInterrupt:
        print(f"\nInterrupted by user")
        print(f"Progress saved - restart to resume")

    except Exception as e:
        print(f"\nUnexpected error: {e}")
        try:
            if 'delegates' in locals() and len(delegates) > 0:
                emergency_file = f"{dao_config.slug}_emergency_delegates.csv"
                pd.DataFrame(delegates).to_csv(emergency_file, index=False)
                print(f"Emergency save: {emergency_file}")
        except Exception:
            print(f"Emergency save failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
