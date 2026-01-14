"""DAO Delegation Statements Fetcher - Fetches delegation statements from Tally API."""

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
        self.headers = {"Content-Type": "application/json", "Api-Key": api_key}
        self.request_delay = 3.0
        self.retry_delay = 60.0
        self.max_retries = 3
        self.request_count = 0

    def make_request_with_retry(self, query: str, variables: Optional[Dict] = None) -> Optional[Dict]:
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
                        return None
                    return data
                elif response.status_code == 429:
                    time.sleep(self.retry_delay)
                    continue
                else:
                    return None
            except Exception:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                return None
        return None

    def get_dao_configuration(self):
        dao_configs = {
            '1': {'name': 'Arbitrum DAO', 'slugs': ['arbitrum', 'arbitrum-dao']},
            '2': {'name': 'Nouns DAO', 'slugs': ['nounsdao', 'nouns']},
            '3': {'name': 'Compound', 'slugs': ['compound']},
            '4': {'name': 'Uniswap', 'slugs': ['uniswap']},
            '5': {'name': 'ENS DAO', 'slugs': ['ens']}
        }
        print("\nSelect DAO:")
        print("1. Arbitrum  2. Nouns  3. Compound  4. Uniswap  5. ENS  6. Custom")
        choice = input("Choice (1-6): ").strip()
        if choice in dao_configs:
            config = dao_configs[choice]
            return config['name'], config['slugs']
        elif choice == "6":
            return self.get_custom_dao_config()
        return dao_configs['1']['name'], dao_configs['1']['slugs']

    def get_custom_dao_config(self):
        name = input("DAO Name: ").strip() or "Custom DAO"
        slug = input("DAO Slug: ").strip()
        if "tally.xyz/gov/" in slug:
            slug = slug.split("tally.xyz/gov/")[-1]
        alt_input = input("Alternative slugs (comma-separated, optional): ").strip()
        alt_slugs = [s.strip() for s in alt_input.split(",") if s.strip()] if alt_input else []
        return name, [slug] + alt_slugs

    def find_dao_organization(self, dao_name: str, dao_slugs: List[str]) -> Optional[Dict]:
        query = """
        query GetOrganization($input: OrganizationInput!) {
          organization(input: $input) {
            id name slug chainIds proposalsCount delegatesCount
            delegatesVotesCount tokenOwnersCount governorIds
          }
        }
        """
        for slug in dao_slugs:
            data = self.make_request_with_retry(query, {'input': {'slug': slug}})
            if data and data.get('data', {}).get('organization'):
                return data['data']['organization']
        return None

    def get_all_delegation_statements(self, dao_name: str, dao_slugs: List[str]) -> List[Dict]:
        org_data = self.find_dao_organization(dao_name, dao_slugs)
        if not org_data:
            return []
        org_id = org_data.get('id')
        all_statements = []
        after_cursor = None
        page_count = 0

        query = """
        query Delegates($input: DelegatesInput!) {
          delegates(input: $input) {
            nodes {
              ... on Delegate {
                account { address }
                statement {
                  id address organizationID statement statementSummary
                  isSeekingDelegation issues { id name }
                }
              }
            }
            pageInfo { count lastCursor }
          }
        }
        """

        while True:
            page_count += 1
            variables = {
                "input": {
                    "filters": {"organizationId": org_id},
                    "page": {"limit": 50}
                }
            }
            if after_cursor:
                variables["input"]["page"]["afterCursor"] = after_cursor

            data = self.make_request_with_retry(query, variables)
            if not data or not data.get("data"):
                break

            delegates_data = data["data"].get("delegates", {})
            nodes = delegates_data.get("nodes", [])
            page_info = delegates_data.get("pageInfo", {})

            if not nodes:
                break

            for delegate in nodes:
                if delegate is None:
                    continue
                statement_data = delegate.get("statement")
                if statement_data and statement_data.get("statement"):
                    issues = statement_data.get("issues") or []
                    all_statements.append({
                        "id": statement_data.get("id"),
                        "address": statement_data.get("address"),
                        "organizationID": statement_data.get("organizationID"),
                        "statement": statement_data.get("statement"),
                        "statementSummary": statement_data.get("statementSummary"),
                        "isSeekingDelegation": statement_data.get("isSeekingDelegation"),
                        "issues": issues if isinstance(issues, list) else []
                    })

            last_cursor = page_info.get("lastCursor")
            if not last_cursor:
                break
            after_cursor = last_cursor
            time.sleep(self.request_delay)

        return all_statements

    def save_delegation_statements(self, statements: List[Dict], filename: str, dao_name: str = "DAO"):
        if not statements:
            return

        def clean_filename(name):
            for char in ['<', '>', ':', '"', '/', '\\', '|', '?', '*']:
                name = name.replace(char, '_')
            return name

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        base_name = clean_filename(filename.replace('.json', ''))
        json_file = f"{base_name}_{timestamp}.json"
        csv_file = f"{base_name}_{timestamp}.csv"

        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(statements, f, indent=2, ensure_ascii=False)

        csv_data = []
        for s in statements:
            if s is None:
                continue
            issues_list = s.get("issues", []) or []
            issues_str = ", ".join([i.get("name", "") for i in issues_list if isinstance(i, dict)])
            csv_data.append({
                "id": s.get("id"),
                "address": s.get("address"),
                "organizationID": s.get("organizationID"),
                "statement": s.get("statement", ""),
                "statementSummary": s.get("statementSummary", ""),
                "isSeekingDelegation": s.get("isSeekingDelegation", False),
                "issues": issues_str
            })

        fieldnames = ["id", "address", "organizationID", "statement", "statementSummary", "isSeekingDelegation", "issues"]
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)

        print(f"Saved {len(statements)} statements to {json_file} and {csv_file}")


def main():
    api_key = os.getenv("TALLY_API_KEY", "")
    if not api_key:
        print("Error: TALLY_API_KEY not set")
        return

    fetcher = DAODelegationStatementsFetcher(api_key)
    dao_name, dao_slugs = fetcher.get_dao_configuration()
    statements = fetcher.get_all_delegation_statements(dao_name, dao_slugs)

    if statements:
        dao_slug = dao_name.lower().replace(' ', '_').replace('dao', '').strip('_')
        fetcher.save_delegation_statements(statements, f"{dao_slug}_statements.json", dao_name)


if __name__ == "__main__":
    main()