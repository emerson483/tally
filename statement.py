import requests
import json
import time
import csv
import os
from typing import List, Dict, Optional

class DAODelegationStatementsFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api_url = "https://api.tally.xyz/query"
        self.headers = {
            "Content-Type": "application/json",
            "Api-Key": api_key
        }

        # Rate limiting settings
        self.request_delay = 3.0  # 3 seconds between requests
        self.retry_delay = 60.0   # 1 minute wait on 429 errors
        self.max_retries = 3
        self.request_count = 0

    def make_request_with_retry(self, query: str, variables: Optional[Dict] = None) -> Optional[Dict]:
        """Make a GraphQL request with retry logic for rate limits"""
        self.request_count += 1

        for attempt in range(self.max_retries):
            try:
                payload = {"query": query}
                if variables:
                    payload["variables"] = variables

                response = requests.post(self.api_url, json=payload, headers=self.headers)

                if response.status_code == 200:
                    data = response.json()
                    if "errors" in data:
                        print(f"GraphQL Errors: {data['errors']}")
                        return None
                    return data

                elif response.status_code == 429:
                    print(f"Rate limited (429). Waiting {self.retry_delay} seconds before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(self.retry_delay)
                    continue

                else:
                    print(f"HTTP Error {response.status_code}: {response.text}")
                    return None

            except Exception as e:
                print(f"Request error: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                return None

        print(f"Failed after {self.max_retries} attempts")
        return None

    def get_dao_configuration(self):
        """Interactive DAO configuration selection"""
        print("\n" + "="*70)
        print("DAO CONFIGURATION SELECTOR")
        print("="*70)

        dao_configs = {
            '1': {
                'name': 'Arbitrum DAO',
                'slugs': ['arbitrum', 'arbitrum-dao', 'arbitrumfoundation']
            },
            '2': {
                'name': 'Nouns DAO',
                'slugs': ['nounsdao', 'nouns', 'nouns-dao', 'nounsDAO']
            },
            '3': {
                'name': 'Compound',
                'slugs': ['compound', 'compound-dao', 'compounddao']
            },
            '4': {
                'name': 'Uniswap',
                'slugs': ['uniswap', 'uniswap-dao', 'uni']
            },
            '5': {
                'name': 'ENS DAO',
                'slugs': ['ens', 'ens-dao', 'ensdao', 'ethereum-name-service']
            }
        }

        print("\nChoose your DAO:")
        print("1. Arbitrum DAO (Layer 2 Scaling Solution)")
        print("2. Nouns DAO (Daily NFT Auctions & Community Governance)")
        print("3. Compound (Decentralized Lending Protocol)")
        print("4. Uniswap (Decentralized Exchange Protocol)")
        print("5. ENS DAO (Ethereum Name Service)")
        print("6. Custom DAO (Enter your own)")

        while True:
            try:
                choice = input("\nEnter your choice (1-6): ").strip()

                if choice in dao_configs:
                    config = dao_configs[choice]
                    print(f"Selected: {config['name']}")
                    return config['name'], config['slugs']

                elif choice == "6":
                    return self.get_custom_dao_config()

                else:
                    print("Invalid choice. Please enter 1, 2, 3, 4, 5, or 6.")

            except KeyboardInterrupt:
                print("\nGoodbye!")
                exit()

    def get_custom_dao_config(self):
        """Get custom DAO configuration from user"""
        print("\nCUSTOM DAO CONFIGURATION")
        print("-" * 50)

        try:
            name = input("DAO Name (e.g., 'Compound DAO'): ").strip()
            if not name:
                raise ValueError("DAO name is required")

            print("\nIMPORTANT: Enter only the slug part, not the full URL!")
            print("   Example: For https://www.tally.xyz/gov/compound, enter just 'compound'")

            slug = input("Primary DAO Slug from Tally URL: ").strip()
            if not slug:
                raise ValueError("DAO slug is required")

            # Clean up common mistakes
            if "tally.xyz/gov/" in slug:
                slug = slug.split("tally.xyz/gov/")[-1]

            alt_slugs_input = input("Alternative slugs (comma-separated, optional): ").strip()
            alt_slugs = [s.strip() for s in alt_slugs_input.split(",") if s.strip()] if alt_slugs_input else []

            all_slugs = [slug] + alt_slugs

            print(f"Custom DAO configured: {name}")
            print(f"   Will try slugs: {', '.join(all_slugs)}")
            return name, all_slugs

        except KeyboardInterrupt:
            print("\nGoodbye!")
            exit()
        except ValueError as e:
            print(f"Error: {e}")
            return self.get_custom_dao_config()

    def find_dao_organization(self, dao_name: str, dao_slugs: List[str]) -> Optional[Dict]:
        """Enhanced organization finder with multiple DAO support"""
        query = """
        query GetOrganization($input: OrganizationInput!) {
          organization(input: $input) {
            id
            name
            slug
            chainIds
            proposalsCount
            delegatesCount
            delegatesVotesCount
            tokenOwnersCount
            hasActiveProposals
            governorIds
            metadata {
              icon
              description
            }
          }
        }
        """

        print(f"\nSearching for {dao_name} organization...")

        for slug in dao_slugs:
            print(f"   Testing slug: '{slug}'...")
            try:
                data = self.make_request_with_retry(query, {
                    'input': {
                        'slug': slug
                    }
                })

                if data and 'data' in data and data['data'] and data['data']['organization']:
                    org = data['data']['organization']
                    print(f"Found organization: {org.get('name')} (slug: {org.get('slug')})")
                    print(f"   Organization ID: {org.get('id')}")
                    print(f"   Total delegates: {org.get('delegatesCount', 'Unknown')}")
                    return org
                else:
                    print(f"   No data for slug: '{slug}'")

            except Exception as e:
                print(f"   Error testing slug '{slug}': {e}")

        # If specific slugs don't work, search for organizations
        print(f"Searching for {dao_name}-related organizations...")
        search_query = """
        query {
          organizations(input: {limit: 50}) {
            nodes {
              id
              name
              slug
              delegatesCount
            }
          }
        }
        """

        data = self.make_request_with_retry(search_query)

        if data and data.get("data") and data["data"].get("organizations"):
            orgs = data["data"]["organizations"]["nodes"]
            # Search for organizations that match any part of the DAO name
            search_terms = dao_name.lower().split()
            matching_orgs = []

            for org in orgs:
                org_name_lower = org["name"].lower()
                org_slug_lower = org["slug"].lower()

                if any(term in org_name_lower or term in org_slug_lower for term in search_terms):
                    matching_orgs.append(org)

            if matching_orgs:
                print(f"Found {dao_name}-related organizations:")
                for org in matching_orgs:
                    print(f"  - {org['name']} (slug: {org['slug']}, id: {org['id']}, delegates: {org.get('delegatesCount', 'Unknown')})")

                # Use the first one found
                selected_org = matching_orgs[0]
                print(f"Using: {selected_org['name']}")
                return selected_org

        print(f"Could not find organization for {dao_name} with any provided slugs")
        return None

    def get_all_delegation_statements(self, dao_name: str, dao_slugs: List[str]) -> List[Dict]:
        """Get ALL delegation statements for specified DAO in raw API format"""
        print(f"\n=== Fetching {dao_name} delegation statements ===")

        # Find the organization
        org_data = self.find_dao_organization(dao_name, dao_slugs)
        if not org_data:
            print(f"Failed to get {dao_name} organization")
            return []

        org_id = org_data.get('id')
        print(f"Using organization ID: {org_id}")

        all_statements = []
        after_cursor = None
        page_count = 0

        query = """
        query Delegates($input: DelegatesInput!) {
          delegates(input: $input) {
            nodes {
              ... on Delegate {
                account {
                  address
                }
                statement {
                  id
                  address
                  organizationID
                  statement
                  statementSummary
                  isSeekingDelegation
                  issues {
                    id
                    name
                  }
                }
              }
            }
            pageInfo {
              count
              lastCursor
            }
          }
        }
        """

        while True:
            page_count += 1
            print(f"Fetching page {page_count}...")

            variables = {
                "input": {
                    "filters": {
                        "organizationId": org_id
                    },
                    "page": {
                        "limit": 50
                    }
                }
            }

            if after_cursor:
                variables["input"]["page"]["afterCursor"] = after_cursor

            data = self.make_request_with_retry(query, variables)

            if not data or not data.get("data"):
                print(f"Failed to get page {page_count}")
                break

            delegates_data = data["data"].get("delegates", {})
            nodes = delegates_data.get("nodes", [])
            page_info = delegates_data.get("pageInfo", {})

            if not nodes:
                print(f"No more delegates found on page {page_count}")
                break

            # Extract delegation statements that exist
            page_statements = []
            for delegate in nodes:
                if delegate is None:
                    continue

                statement_data = delegate.get("statement")
                if statement_data and statement_data.get("statement"):
                    # Safely handle issues array
                    issues = statement_data.get("issues")
                    if issues is None:
                        issues = []
                    elif not isinstance(issues, list):
                        issues = []

                    # Format the statement in the exact API structure
                    formatted_statement = {
                        "id": statement_data.get("id"),
                        "address": statement_data.get("address"),
                        "organizationID": statement_data.get("organizationID"),
                        "statement": statement_data.get("statement"),
                        "statementSummary": statement_data.get("statementSummary"),
                        "isSeekingDelegation": statement_data.get("isSeekingDelegation"),
                        "issues": issues
                    }
                    page_statements.append(formatted_statement)

            all_statements.extend(page_statements)
            print(f"Found {len(page_statements)} delegation statements on page {page_count}")
            print(f"Total delegation statements so far: {len(all_statements)}")

            # Check if there are more pages
            last_cursor = page_info.get("lastCursor")
            if not last_cursor:
                print(f"Reached end of pages - no more data available")
                break

            after_cursor = last_cursor

            # Save progress periodically (every 20 pages)
            if page_count % 20 == 0:
                temp_filename = f"{dao_name.lower().replace(' ', '_')}_delegation_statements_temp_{page_count}.json"
                self.save_delegation_statements(all_statements, temp_filename, dao_name)
                print(f"Progress saved to {temp_filename}")

            # Wait between requests to avoid rate limiting
            print(f"Waiting {self.request_delay} seconds before next request...")
            time.sleep(self.request_delay)

        print(f"\n=== COMPLETED: Fetched {len(all_statements)} delegation statements from {page_count} pages ===")
        return all_statements

    def save_delegation_statements(self, statements: List[Dict], filename: str, dao_name: str = "DAO"):
        """Save delegation statements in both JSON and CSV formats"""
        if not statements:
            print("No delegation statements to save")
            return

        # Clean filename
        def clean_filename(name):
            invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
            for char in invalid_chars:
                name = name.replace(char, '_')
            return name

        # Add timestamp
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        base_name = clean_filename(filename.replace('.json', ''))
        timestamped_filename = f"{base_name}_{timestamp}.json"

        # Save to JSON (raw API format)
        with open(timestamped_filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(statements, jsonfile, indent=2, ensure_ascii=False)

        # Also save latest version
        latest_json = clean_filename(filename)
        with open(latest_json, 'w', encoding='utf-8') as jsonfile:
            json.dump(statements, jsonfile, indent=2, ensure_ascii=False)

        # Save to CSV for easy viewing
        csv_filename = timestamped_filename.replace('.json', '.csv')
        latest_csv = clean_filename(filename.replace('.json', '.csv'))

        # Prepare CSV data - flatten the issues array
        csv_data = []
        for statement in statements:
            if statement is None:
                continue

            issues_list = statement.get("issues", [])
            if issues_list is None:
                issues_list = []

            issues_names = []
            for issue in issues_list:
                if issue is not None and isinstance(issue, dict):
                    issue_name = issue.get("name", "")
                    if issue_name:
                        issues_names.append(issue_name)

            issues_str = ", ".join(issues_names)

            csv_row = {
                "id": statement.get("id"),
                "address": statement.get("address"),
                "organizationID": statement.get("organizationID"),
                "statement": statement.get("statement", ""),
                "statementSummary": statement.get("statementSummary", ""),
                "isSeekingDelegation": statement.get("isSeekingDelegation", False),
                "issues": issues_str
            }
            csv_data.append(csv_row)

        fieldnames = ["id", "address", "organizationID", "statement", "statementSummary", "isSeekingDelegation", "issues"]

        # Save timestamped CSV
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)

        # Save latest CSV
        with open(latest_csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)

        print(f"Saved {len(statements)} delegation statements to:")
        print(f"  JSON: {timestamped_filename} and {latest_json}")
        print(f"  CSV:  {csv_filename} and {latest_csv}")

        # Show statistics
        seeking_delegation = len([s for s in statements if s.get("isSeekingDelegation")])
        with_summary = len([s for s in statements if s.get("statementSummary")])
        with_issues = len([s for s in statements if s.get("issues")])

        print(f"\n=== {dao_name.upper()} DELEGATION STATEMENTS STATISTICS ===")
        print(f"Total delegation statements: {len(statements)}")
        print(f"Seeking delegation: {seeking_delegation}")
        print(f"With summaries: {with_summary}")
        print(f"With issues specified: {with_issues}")
        print(f"Total API requests made: {self.request_count}")

        # Show sample statements
        print(f"\n=== SAMPLE {dao_name.upper()} DELEGATION STATEMENTS ===")
        for i, statement in enumerate(statements[:3], 1):
            if statement is None:
                continue

            print(f"\n{i}. Statement ID: {statement.get('id')}")
            print(f"   Address: {statement.get('address')}")
            print(f"   Seeking Delegation: {statement.get('isSeekingDelegation')}")
            statement_text = statement.get('statement', '')
            if len(statement_text) > 200:
                statement_text = statement_text[:200] + "..."
            print(f"   Statement: {statement_text}")
            if statement.get('statementSummary'):
                summary = statement.get('statementSummary', '')
                if len(summary) > 100:
                    summary = summary[:100] + "..."
                print(f"   Summary: {summary}")

            issues = statement.get('issues', [])
            if issues and isinstance(issues, list):
                issues_names = []
                for issue in issues:
                    if issue is not None and isinstance(issue, dict):
                        issue_name = issue.get("name", "")
                        if issue_name:
                            issues_names.append(issue_name)
                if issues_names:
                    print(f"   Issues: {', '.join(issues_names)}")
            print("-" * 80)

def main():
    # Get API key from environment variable
    API_KEY = os.getenv("TALLY_API_KEY", "")

    if not API_KEY:
        print("Error: TALLY_API_KEY environment variable is not set.")
        print("Set it using: set TALLY_API_KEY=your_api_key_here")
        return

    fetcher = DAODelegationStatementsFetcher(API_KEY)

    print("=== DAO Delegation Statements Fetcher ===")
    print("This will fetch delegation statements in raw API format")
    print("Progress saved every 20 pages")
    print()

    try:
        # Get DAO configuration
        dao_name, dao_slugs = fetcher.get_dao_configuration()

        print(f"\nFetching all {dao_name} delegation statements...")

        delegation_statements = fetcher.get_all_delegation_statements(dao_name, dao_slugs)

        if delegation_statements:
            # Create filename based on DAO name
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

if __name__ == "__main__":
    main()