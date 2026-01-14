"""
Unified Runner for Tally DAO Scrapy Tools

This script provides a single interface to run all Tally DAO data fetching modules
with a single DAO configuration. All outputs are saved to a folder named after the DAO.

Usage:
    python main.py

Ensure TALLY_API_KEY environment variable is set before running.
"""

import os
import sys
import time


def get_api_key():
    """Get API key from environment variable."""
    api_key = os.getenv("TALLY_API_KEY")
    if not api_key:
        print("Error: TALLY_API_KEY environment variable is not set.")
        print("Set it using: set TALLY_API_KEY=your_api_key_here")
        print("Get your API key from: https://docs.tally.xyz/tally-api/authentication")
        sys.exit(1)
    return api_key


def get_dao_configuration():
    """Collect DAO configuration from user once."""
    print("\n" + "=" * 60)
    print("  DAO CONFIGURATION")
    print("=" * 60)
    
    dao_name = input("Enter DAO name (e.g., Compound, ENS, Arbitrum): ").strip()
    if not dao_name:
        print("Error: DAO name is required.")
        sys.exit(1)
    
    dao_slug = input("Enter DAO slug (e.g., compound, ens, arbitrum): ").strip().lower()
    if not dao_slug:
        dao_slug = dao_name.lower().replace(" ", "-")
        print(f"Using generated slug: {dao_slug}")
    
    alt_slugs_input = input("Enter alternative slugs (comma-separated, or press Enter to skip): ").strip()
    alternative_slugs = [s.strip() for s in alt_slugs_input.split(",") if s.strip()] if alt_slugs_input else []
    
    expected_delegates = input("Expected number of delegates (press Enter for default 1000): ").strip()
    expected_delegates = int(expected_delegates) if expected_delegates else 1000
    
    return {
        "name": dao_name,
        "slug": dao_slug,
        "alternative_slugs": alternative_slugs,
        "expected_delegates": expected_delegates
    }


def create_output_folder(dao_name):
    """Create output folder named after the DAO."""
    folder_name = dao_name.replace(" ", "_").replace("/", "_").replace(":", "_")
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Created output folder: {folder_name}/")
    else:
        print(f"Using existing folder: {folder_name}/")
    return folder_name


def run_statements(api_key, dao_config, output_folder):
    """Run the Delegation Statements Fetcher."""
    print("\n" + "=" * 60)
    print("[1/4] DELEGATION STATEMENTS FETCHER")
    print("=" * 60)
    
    from tally_scrapy.statement import DAODelegationStatementsFetcher
    
    fetcher = DAODelegationStatementsFetcher(api_key)
    dao_name = dao_config["name"]
    dao_slugs = [dao_config["slug"]] + dao_config.get("alternative_slugs", [])
    
    print(f"Fetching delegation statements for {dao_name}...")
    
    statements = fetcher.get_all_delegation_statements(dao_name, dao_slugs)
    
    if statements:
        filename = os.path.join(output_folder, f"{dao_config['slug']}_delegation_statements")
        fetcher.save_delegation_statements(statements, filename, dao_name)
        print(f"Saved {len(statements)} delegation statements")
        return True
    else:
        print(f"No delegation statements found for {dao_name}")
        return False


def run_basic_analyzer(api_key, dao_config, output_folder):
    """Run the Basic DAO Governance Analyzer."""
    print("\n" + "=" * 60)
    print("[2/4] BASIC DAO GOVERNANCE ANALYZER")
    print("=" * 60)
    
    from tally_scrapy.basic import UniversalDAOAnalyzer
    
    analyzer = UniversalDAOAnalyzer(api_key)
    
    if not analyzer.validate_api_key():
        print("Invalid API key.")
        return False
    
    dao_slug = dao_config["slug"]
    dao_name = dao_config["name"]
    
    print(f"Fetching governance data for {dao_name}...")
    
    dao_info = analyzer.get_dao_by_slug(dao_slug)
    if not dao_info:
        print(f"DAO '{dao_slug}' not found.")
        return False
    
    analyzer.set_dao_info(dao_slug, dao_info.get("name", dao_name))
    
    governor_ids = analyzer.get_governor_ids(dao_slug)
    if not governor_ids:
        print(f"No governors found for DAO '{dao_slug}'.")
        return False
    
    # Use the first (primary) governor
    selected_gov = governor_ids[0]
    print(f"Analyzing governor: {selected_gov}")
    
    governance_stats = analyzer.get_governance_stats_fast(selected_gov)
    proposals = analyzer.get_all_proposals_optimized(selected_gov)
    delegates = analyzer.get_all_delegates_optimized(selected_gov)
    
    clean_slug = dao_slug.replace(":", "_").replace("/", "_").replace("-", "_")
    clean_gov = selected_gov.replace(":", "_").replace("/", "_")
    
    # Save to output folder
    original_dir = os.getcwd()
    os.chdir(output_folder)
    
    saved_file = analyzer.save_comprehensive_data(
        governance_stats, proposals, delegates,
        f"{clean_slug}_governance_{clean_gov}"
    )
    
    summary_file = analyzer.export_summary_report(
        governance_stats, proposals, delegates,
        f"{clean_slug}_governance_{clean_gov}"
    )
    
    os.chdir(original_dir)
    
    print(f"Found {len(proposals)} proposals, {len(delegates)} delegates")
    return True


def run_proposals_fetcher(api_key, dao_config, output_folder):
    """Run the DAO Proposals Fetcher."""
    print("\n" + "=" * 60)
    print("[3/4] DAO PROPOSALS FETCHER")
    print("=" * 60)
    
    from tally_scrapy.prop import DAOProposalsFetcher
    
    fetcher = DAOProposalsFetcher(api_key)
    dao_name = dao_config["name"]
    dao_slugs = [dao_config["slug"]] + dao_config.get("alternative_slugs", [])
    
    print(f"Fetching proposals for {dao_name}...")
    
    org_data = fetcher.find_dao_organization(dao_name, dao_slugs)
    if not org_data:
        print(f"Could not find organization for {dao_name}")
        return False
    
    governor_ids = org_data.get("governorIds", [])
    if not governor_ids:
        print(f"No governor IDs found for {dao_name}")
        return False
    
    all_proposals = fetcher.fetch_all_proposals(dao_name, governor_ids)
    
    if all_proposals:
        # Save to output folder
        original_dir = os.getcwd()
        os.chdir(output_folder)
        
        filename = fetcher.save_proposals_to_csv(all_proposals, dao_name)
        
        os.chdir(original_dir)
        
        print(f"Saved {len(all_proposals)} proposals")
        return True
    else:
        print(f"No proposals found for {dao_name}")
        return False


def run_voting_matrix(api_key, dao_config, output_folder):
    """Run the Voting Matrix Builder."""
    print("\n" + "=" * 60)
    print("[4/4] VOTING MATRIX BUILDER")
    print("=" * 60)
    
    from tally_scrapy.voting_matrix import FastDAOFetcher, DAOConfig
    
    dao_cfg = DAOConfig.get_custom_config(
        name=dao_config["name"],
        slug=dao_config["slug"],
        alternative_slugs=dao_config.get("alternative_slugs", []),
        expected_delegates=dao_config.get("expected_delegates", 1000)
    )
    
    print(f"Building voting matrix for {dao_config['name']}...")
    
    fetcher = FastDAOFetcher(api_key=api_key, dao_config=dao_cfg)
    
    result = fetcher.get_organization_by_slug(dao_cfg.slug)
    if not result.get("data", {}).get("organization"):
        print(f"Could not find {dao_config['name']}")
        return False
    
    org = result["data"]["organization"]
    org_id = org["id"]
    print(f"Found: {org['name']} (ID: {org_id})")
    
    delegates = fetcher.get_all_delegates_optimized(org_id)
    proposals = fetcher.get_all_proposals_optimized(org_id)
    
    if delegates and proposals:
        # Save to output folder
        original_dir = os.getcwd()
        os.chdir(output_folder)
        
        fetcher.create_voting_matrix_fast(proposals, delegates, org_id)
        
        os.chdir(original_dir)
        
        print(f"Created voting matrix with {len(delegates)} delegates, {len(proposals)} proposals")
        fetcher.cleanup_checkpoint_files()
        return True
    else:
        print("Failed to create voting matrix")
        return False


def main():
    print("=" * 60)
    print("  TALLY DAO SCRAPY - UNIFIED RUNNER")
    print("  Run all 4 modules with a single DAO configuration")
    print("=" * 60)
    
    api_key = get_api_key()
    dao_config = get_dao_configuration()
    output_folder = create_output_folder(dao_config["name"])
    
    print("\n" + "=" * 60)
    print("  STARTING DATA EXTRACTION")
    print("=" * 60)
    print(f"DAO: {dao_config['name']}")
    print(f"Slug: {dao_config['slug']}")
    print(f"Output folder: {output_folder}/")
    
    start_time = time.time()
    results = {}
    
    try:
        # Run all 4 modules
        results["statements"] = run_statements(api_key, dao_config, output_folder)
        results["basic"] = run_basic_analyzer(api_key, dao_config, output_folder)
        results["proposals"] = run_proposals_fetcher(api_key, dao_config, output_folder)
        results["voting_matrix"] = run_voting_matrix(api_key, dao_config, output_folder)
        
    except KeyboardInterrupt:
        print("\n\nStopped by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
    
    # Summary
    elapsed = time.time() - start_time
    elapsed_mins = elapsed / 60
    
    print("\n" + "=" * 60)
    print("  EXTRACTION COMPLETE!")
    print("=" * 60)
    print(f"DAO: {dao_config['name']}")
    print(f"Total time: {elapsed_mins:.1f} minutes")
    print(f"Output folder: {output_folder}/")
    print()
    print("Results:")
    for module, success in results.items():
        status = "✓ Success" if success else "✗ Failed/Skipped"
        print(f"  {module}: {status}")
    print()
    print(f"All files saved to: {os.path.abspath(output_folder)}/")


if __name__ == "__main__":
    main()
