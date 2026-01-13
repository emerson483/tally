"""
Runner script for the Basic DAO Governance Analyzer module.

Usage:
    python run_basic.py

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

    from basic import UniversalDAOAnalyzer, get_dao_choice, display_popular_daos, check_dependencies

    analyzer = UniversalDAOAnalyzer(api_key)

    try:
        if not analyzer.validate_api_key():
            print("Invalid API key. Please check and try again.")
            sys.exit(1)

        print("=== Universal DAO Governance Data Analyzer ===")
        print("This program can analyze ANY DAO on Tally.xyz\n")

        print("Checking dependencies...")
        check_dependencies()
        print()

        display_popular_daos()

        print("Choose how you want to select a DAO:")
        print("1. Enter DAO slug directly (e.g., 'arbitrum', 'uniswap')")
        print("2. Search for DAOs by name")
        print("3. Browse popular DAOs")

        choice = input("\nEnter your choice (1-3): ").strip()

        dao_slug = ""
        dao_info = None

        if choice == "1":
            dao_slug = input("\nEnter the DAO slug (e.g., 'arbitrum', 'uniswap'): ").strip().lower()
            if dao_slug:
                dao_info = analyzer.get_dao_by_slug(dao_slug)
                if not dao_info:
                    print(f"DAO '{dao_slug}' not found.")
                    sys.exit(1)

        elif choice == "2":
            search_term = input("\nEnter search term for DAO name: ").strip()
            print(f"\nSearching for DAOs matching '{search_term}'...")
            search_results = analyzer.search_daos_fast(search_term)
            if not search_results:
                print("No DAOs found matching your search.")
                sys.exit(1)

            print(f"\nFound {len(search_results)} DAOs:")
            for i, dao in enumerate(search_results[:20], 1):
                gov_count = len(dao.get("governorIds", []))
                print(f"{i:2d}. {dao['name']:25} (slug: {dao['slug']:15}) - {gov_count} governors")

            try:
                dao_choice = int(input(f"\nSelect DAO (1-{min(len(search_results), 20)}): ").strip())
                if 1 <= dao_choice <= len(search_results):
                    dao_info = search_results[dao_choice - 1]
                    dao_slug = dao_info['slug']
                else:
                    print("Invalid selection.")
                    sys.exit(1)
            except ValueError:
                print("Invalid input.")
                sys.exit(1)

        elif choice == "3":
            print("\nFetching popular DAOs...")
            popular_daos = analyzer.search_daos_fast()
            if not popular_daos:
                print("Failed to fetch DAOs.")
                sys.exit(1)

            print(f"\nFound {len(popular_daos)} DAOs:")
            display_list = popular_daos[:20]
            for i, dao in enumerate(display_list, 1):
                gov_count = len(dao.get("governorIds", []))
                print(f"{i:2d}. {dao['name']:25} (slug: {dao['slug']:15}) - {gov_count} governors")

            try:
                dao_choice = int(input(f"\nSelect DAO (1-{len(display_list)}): ").strip())
                if 1 <= dao_choice <= len(display_list):
                    dao_info = display_list[dao_choice - 1]
                    dao_slug = dao_info['slug']
                else:
                    print("Invalid selection.")
                    sys.exit(1)
            except ValueError:
                print("Invalid input.")
                sys.exit(1)
        else:
            print("Invalid choice.")
            sys.exit(1)

        if not dao_slug or not dao_info:
            print("Failed to select a DAO.")
            sys.exit(1)

        analyzer.set_dao_info(dao_slug, dao_info['name'])
        print(f"\nSelected DAO: {dao_info['name']} ({dao_slug})")

        governor_ids = analyzer.get_governor_ids(dao_slug)
        if not governor_ids:
            print(f"No governors found for DAO '{dao_slug}'.")
            sys.exit(1)

        print(f"\nChoose which governor to analyze:")
        for i, gov_id in enumerate(governor_ids, 1):
            if i == 1:
                print(f"{i}. {gov_id} (RECOMMENDED - Primary Governor)")
            else:
                print(f"{i}. {gov_id}")

        choice = input(f"\nEnter your choice (1-{len(governor_ids)}): ").strip()

        try:
            choice_num = int(choice)
        except ValueError:
            print("Invalid input.")
            sys.exit(1)

        if 1 <= choice_num <= len(governor_ids):
            selected_gov = governor_ids[choice_num - 1]
            print(f"\nAnalyzing governance data for: {selected_gov}")
            print("This may take several minutes...")

            governance_stats = analyzer.get_governance_stats_fast(selected_gov)
            proposals = analyzer.get_all_proposals_optimized(selected_gov)
            delegates = analyzer.get_all_delegates_optimized(selected_gov)

            print("\nSaving governance data...")

            clean_dao_slug = dao_slug.replace(":", "_").replace("/", "_").replace("-", "_")
            clean_gov_id = selected_gov.replace(":", "_").replace("/", "_")

            saved_file = analyzer.save_comprehensive_data(
                governance_stats,
                proposals,
                delegates,
                f"{clean_dao_slug}_governance_analysis_{clean_gov_id}"
            )

            summary_file = analyzer.export_summary_report(
                governance_stats,
                proposals,
                delegates,
                f"{clean_dao_slug}_governance_analysis_{clean_gov_id}"
            )

            print(f"\nANALYSIS COMPLETE FOR {dao_info['name'].upper()}!")
            if saved_file:
                print(f"Governance data saved to: {saved_file}")
            if summary_file:
                print(f"Summary report saved to: {summary_file}")

            print(f"\nData Summary:")
            print(f"- Found {len(proposals)} proposals")
            print(f"- Found {len(delegates)} delegates")

        else:
            print("Invalid choice.")

    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
