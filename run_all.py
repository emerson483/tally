"""
Unified Runner for Tally DAO Scrapy Tools

This script provides a single interface to run all Tally DAO data fetching modules.

Usage:
    python run_all.py

Ensure TALLY_API_KEY environment variable is set before running.
"""

import os
import sys


def check_api_key():
    """Check if API key is set"""
    api_key = os.getenv("TALLY_API_KEY")
    if not api_key:
        print("Error: TALLY_API_KEY environment variable is not set.")
        print("Set it using: set TALLY_API_KEY=your_api_key_here")
        print("Get your API key from: https://docs.tally.xyz/tally-api/authentication")
        return None
    return api_key


def run_basic_analyzer(api_key):
    """Run the Basic DAO Governance Analyzer"""
    print("\n" + "=" * 60)
    print("BASIC DAO GOVERNANCE ANALYZER")
    print("=" * 60)
    
    from basic import UniversalDAOAnalyzer, display_popular_daos, check_dependencies
    
    analyzer = UniversalDAOAnalyzer(api_key)
    
    if not analyzer.validate_api_key():
        print("Invalid API key.")
        return
    
    print("Checking dependencies...")
    check_dependencies()
    display_popular_daos()
    
    print("Choose how to select a DAO:")
    print("1. Enter DAO slug directly")
    print("2. Search for DAOs by name")
    print("3. Browse popular DAOs")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    dao_slug = ""
    dao_info = None
    
    if choice == "1":
        dao_slug = input("\nEnter the DAO slug: ").strip().lower()
        if dao_slug:
            dao_info = analyzer.get_dao_by_slug(dao_slug)
            if not dao_info:
                print(f"DAO '{dao_slug}' not found.")
                return
    elif choice == "2":
        search_term = input("\nEnter search term: ").strip()
        search_results = analyzer.search_daos_fast(search_term)
        if not search_results:
            print("No DAOs found.")
            return
        for i, dao in enumerate(search_results[:10], 1):
            print(f"{i}. {dao['name']} (slug: {dao['slug']})")
        try:
            dao_choice = int(input(f"\nSelect DAO (1-{min(len(search_results), 10)}): ").strip())
            if 1 <= dao_choice <= len(search_results):
                dao_info = search_results[dao_choice - 1]
                dao_slug = dao_info['slug']
        except ValueError:
            print("Invalid input.")
            return
    elif choice == "3":
        popular_daos = analyzer.search_daos_fast()
        if not popular_daos:
            print("Failed to fetch DAOs.")
            return
        for i, dao in enumerate(popular_daos[:10], 1):
            print(f"{i}. {dao['name']} (slug: {dao['slug']})")
        try:
            dao_choice = int(input(f"\nSelect DAO (1-{min(len(popular_daos), 10)}): ").strip())
            if 1 <= dao_choice <= len(popular_daos):
                dao_info = popular_daos[dao_choice - 1]
                dao_slug = dao_info['slug']
        except ValueError:
            print("Invalid input.")
            return
    
    if not dao_slug or not dao_info:
        print("Failed to select a DAO.")
        return
    
    analyzer.set_dao_info(dao_slug, dao_info['name'])
    governor_ids = analyzer.get_governor_ids(dao_slug)
    
    if not governor_ids:
        print(f"No governors found for DAO '{dao_slug}'.")
        return
    
    for i, gov_id in enumerate(governor_ids, 1):
        print(f"{i}. {gov_id}")
    
    try:
        choice_num = int(input(f"\nSelect governor (1-{len(governor_ids)}): ").strip())
    except ValueError:
        choice_num = 1
    
    if 1 <= choice_num <= len(governor_ids):
        selected_gov = governor_ids[choice_num - 1]
        print(f"\nAnalyzing {selected_gov}...")
        
        governance_stats = analyzer.get_governance_stats_fast(selected_gov)
        proposals = analyzer.get_all_proposals_optimized(selected_gov)
        delegates = analyzer.get_all_delegates_optimized(selected_gov)
        
        clean_dao_slug = dao_slug.replace(":", "_").replace("/", "_").replace("-", "_")
        clean_gov_id = selected_gov.replace(":", "_").replace("/", "_")
        
        saved_file = analyzer.save_comprehensive_data(
            governance_stats, proposals, delegates,
            f"{clean_dao_slug}_governance_analysis_{clean_gov_id}"
        )
        
        print(f"\nAnalysis complete! Saved to: {saved_file}")
        print(f"Found {len(proposals)} proposals, {len(delegates)} delegates")


def run_proposals_fetcher(api_key):
    """Run the DAO Proposals Fetcher"""
    print("\n" + "=" * 60)
    print("DAO PROPOSALS FETCHER")
    print("=" * 60)
    
    from prop import DAOProposalsFetcher
    
    fetcher = DAOProposalsFetcher(api_key)
    dao_name, dao_slugs, governor_ids = fetcher.get_dao_configuration()
    
    if not governor_ids:
        org_data = fetcher.find_dao_organization(dao_name, dao_slugs)
        if org_data:
            governor_ids = org_data.get('governorIds', [])
        if not governor_ids:
            print(f"Could not find governor IDs for {dao_name}")
            return
    
    print(f"\nUsing governor IDs: {governor_ids}")
    print("\n1. Fetch first 50 proposals")
    print("2. Fetch ALL proposals")
    print("3. Fetch and display only")
    
    choice = input("Enter choice (1-3): ").strip()
    
    if choice == "2":
        all_proposals = fetcher.fetch_all_proposals(dao_name, governor_ids)
        if all_proposals:
            filename = fetcher.save_proposals_to_csv(all_proposals, dao_name)
            print(f"\nSaved {len(all_proposals)} proposals to {filename}")
    elif choice == "1":
        proposals_data = fetcher.fetch_proposals(governor_ids[0], limit=50)
        if proposals_data and "errors" not in proposals_data:
            proposals = proposals_data["data"]["proposals"]["nodes"]
            filename = fetcher.save_proposals_to_csv(proposals, dao_name)
            print(f"\nSaved {len(proposals)} proposals to {filename}")
    else:
        proposals_data = fetcher.fetch_proposals(governor_ids[0], limit=50)
        if proposals_data and "errors" not in proposals_data:
            proposals = proposals_data["data"]["proposals"]["nodes"]
            fetcher.display_proposals(proposals, limit=10)
    
    print(f"\nTotal API requests: {fetcher.request_count}")


def run_statements_fetcher(api_key):
    """Run the Delegation Statements Fetcher"""
    print("\n" + "=" * 60)
    print("DELEGATION STATEMENTS FETCHER")
    print("=" * 60)
    
    from statement import DAODelegationStatementsFetcher
    
    fetcher = DAODelegationStatementsFetcher(api_key)
    dao_name, dao_slugs = fetcher.get_dao_configuration()
    
    print(f"\nFetching all {dao_name} delegation statements...")
    
    delegation_statements = fetcher.get_all_delegation_statements(dao_name, dao_slugs)
    
    if delegation_statements:
        dao_slug = dao_name.lower().replace(' ', '_').replace('dao', '').strip('_')
        filename = f"{dao_slug}_delegation_statements_raw.json"
        fetcher.save_delegation_statements(delegation_statements, filename, dao_name)
        print(f"\nSaved {len(delegation_statements)} statements")
    else:
        print(f"No delegation statements found for {dao_name}")


def run_voting_matrix(api_key):
    """Run the Voting Matrix Builder"""
    print("\n" + "=" * 60)
    print("VOTING MATRIX BUILDER")
    print("=" * 60)
    
    import time
    import pandas as pd
    from voting_matrix import FastDAOFetcher, DAOConfig
    
    print("1. Compound DAO")
    print("2. ENS DAO")
    print("3. Custom DAO")
    
    try:
        choice = input("\nSelect (1-3): ").strip()
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
            dao_config = DAOConfig.get_compound_config()
    except Exception:
        dao_config = DAOConfig.get_compound_config()
    
    confirm = input(f"\nStart extraction for {dao_config.name}? (y/N): ").strip().lower()
    if confirm != 'y':
        print("Cancelled.")
        return
    
    fetcher = FastDAOFetcher(api_key=api_key, dao_config=dao_config)
    total_start_time = time.time()
    
    result = fetcher.get_organization_by_slug(dao_config.slug)
    if not result.get('data', {}).get('organization'):
        print(f"Could not find {dao_config.name}")
        return
    
    org = result['data']['organization']
    org_id = org['id']
    print(f"Found: {org['name']} (ID: {org_id})")
    
    delegates = fetcher.get_all_delegates_optimized(org_id)
    proposals = fetcher.get_all_proposals_optimized(org_id)
    
    if delegates and proposals:
        fetcher.create_voting_matrix_fast(proposals, delegates, org_id)
        total_time_hours = (time.time() - total_start_time) / 3600
        print(f"\nComplete! Total time: {total_time_hours:.1f} hours")
    
    fetcher.cleanup_checkpoint_files()


def main():
    print("=" * 60)
    print("  TALLY DAO SCRAPY - UNIFIED RUNNER")
    print("=" * 60)
    
    api_key = check_api_key()
    if not api_key:
        sys.exit(1)
    
    print("\nSelect module to run:")
    print("1. Basic DAO Governance Analyzer")
    print("2. Proposals Fetcher")
    print("3. Delegation Statements Fetcher")
    print("4. Voting Matrix Builder")
    print("5. Run ALL modules sequentially")
    print("0. Exit")
    
    try:
        choice = input("\nEnter your choice (0-5): ").strip()
        
        if choice == "1":
            run_basic_analyzer(api_key)
        elif choice == "2":
            run_proposals_fetcher(api_key)
        elif choice == "3":
            run_statements_fetcher(api_key)
        elif choice == "4":
            run_voting_matrix(api_key)
        elif choice == "5":
            print("\nRunning ALL modules sequentially...")
            print("Each module will prompt for DAO selection.")
            
            run_basic_analyzer(api_key)
            run_proposals_fetcher(api_key)
            run_statements_fetcher(api_key)
            run_voting_matrix(api_key)
            
            print("\n" + "=" * 60)
            print("ALL MODULES COMPLETED!")
            print("=" * 60)
        elif choice == "0":
            print("Goodbye!")
        else:
            print("Invalid choice.")
    
    except KeyboardInterrupt:
        print("\n\nStopped by user")
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
