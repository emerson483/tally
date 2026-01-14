"""DAO Proposals Fetcher - Fetches proposal data from Tally API."""

import requests
import json
from datetime import datetime
import time
import csv
import os
from typing import Dict, List, Optional, Tuple


class DAOProposalsFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api_url = "https://api.tally.xyz/query"
        self.headers = {"Api-Key": api_key, "Content-Type": "application/json"}
        self.request_count = 0

    def get_dao_configuration(self) -> Tuple[str, List[str], List[str]]:
        dao_configs = {
            '1': {'name': 'Arbitrum DAO', 'slugs': ['arbitrum'], 'governor_ids': []},
            '2': {'name': 'Nouns DAO', 'slugs': ['nounsdao'], 'governor_ids': []},
            '3': {'name': 'Compound', 'slugs': ['compound'], 'governor_ids': []},
            '4': {'name': 'Uniswap', 'slugs': ['uniswap'], 'governor_ids': []},
            '5': {'name': 'ENS DAO', 'slugs': ['ens'], 'governor_ids': []}
        }
        print("\nSelect DAO:")
        print("1. Arbitrum  2. Nouns  3. Compound  4. Uniswap  5. ENS  6. Custom")
        choice = input("Choice (1-6): ").strip()
        if choice in dao_configs:
            c = dao_configs[choice]
            return c['name'], c['slugs'], c['governor_ids']
        elif choice == "6":
            return self.get_custom_dao_config()
        return dao_configs['1']['name'], dao_configs['1']['slugs'], []

    def get_custom_dao_config(self) -> Tuple[str, List[str], List[str]]:
        name = input("DAO Name: ").strip() or "Custom DAO"
        slug = input("DAO Slug: ").strip()
        if "tally.xyz/gov/" in slug:
            slug = slug.split("tally.xyz/gov/")[-1]
        alt_input = input("Alternative slugs (optional): ").strip()
        alt_slugs = [s.strip() for s in alt_input.split(",") if s.strip()] if alt_input else []
        gov_input = input("Governor Address (optional): ").strip()
        governor_ids = [gov_input] if gov_input else []
        return name, [slug] + alt_slugs, governor_ids

    def find_dao_organization(self, dao_name: str, dao_slugs: List[str]) -> Optional[Dict]:
        query = """
        query GetOrganization($input: OrganizationInput!) {
          organization(input: $input) {
            id name slug proposalsCount delegatesCount governorIds
          }
        }
        """
        for slug in dao_slugs:
            self.request_count += 1
            try:
                response = requests.post(
                    self.api_url, headers=self.headers,
                    json={'query': query, 'variables': {'input': {'slug': slug}}}
                )
                if response.status_code == 200:
                    data = response.json()
                    if 'errors' not in data and data.get('data', {}).get('organization'):
                        return data['data']['organization']
            except Exception:
                continue
        return None

    def fetch_proposals(self, governor_id: str, limit: int = 50, after_cursor: str = None) -> Optional[Dict]:
        page_input = f"limit: {limit}"
        if after_cursor:
            page_input += f', afterCursor: "{after_cursor}"'

        query = f"""
        query ProposalsDetailed {{
            proposals(input: {{
                filters: {{ governorId: "{governor_id}" }}
                page: {{ {page_input} }}
                sort: {{ isDescending: true, sortBy: id }}
            }}) {{
                nodes {{
                    ... on Proposal {{
                        id onchainId
                        metadata {{ title description }}
                        status
                        creator {{ address name }}
                        proposer {{ address name }}
                        voteStats {{ type votesCount votersCount percent }}
                        start {{ ... on Block {{ number timestamp }} }}
                        end {{ ... on Block {{ number timestamp }} }}
                        block {{ number timestamp }}
                        executableCalls {{ calldata signature target value }}
                    }}
                }}
                pageInfo {{ count firstCursor lastCursor }}
            }}
        }}
        """
        self.request_count += 1
        response = requests.post(self.api_url, headers=self.headers, json={"query": query})
        if response.status_code == 200:
            return response.json()
        return None

    def format_timestamp(self, timestamp: str) -> str:
        if not timestamp:
            return "N/A"
        try:
            if timestamp.isdigit():
                return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
            if 'T' in timestamp:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return timestamp
        except (ValueError, TypeError):
            return timestamp

    def format_votes(self, votes: str) -> str:
        try:
            return f"{int(votes) / 10**18:,.2f}"
        except (ValueError, TypeError):
            return str(votes) if votes else "0"

    def fetch_all_proposals(self, dao_name: str, governor_ids: List[str]) -> List[Dict]:
        all_proposals = []
        if not governor_ids:
            return []

        for governor_id in governor_ids:
            after_cursor = None
            while True:
                data = self.fetch_proposals(governor_id, limit=50, after_cursor=after_cursor)
                if not data or "errors" in data:
                    break
                proposals = data["data"]["proposals"]["nodes"]
                if not proposals:
                    break
                all_proposals.extend(proposals)
                page_info = data["data"]["proposals"]["pageInfo"]
                if not page_info.get("lastCursor"):
                    break
                after_cursor = page_info["lastCursor"]
                time.sleep(1.1)
        return all_proposals

    def save_proposals_to_csv(self, proposals: List[Dict], dao_name: str, filename: str = None) -> str:
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dao_slug = dao_name.lower().replace(' ', '_').replace('dao', '').strip('_')
            filename = f"{dao_slug}_proposals_{timestamp}.csv"

        headers = [
            'proposal_id', 'onchain_id', 'title', 'description', 'status',
            'creator_address', 'proposer_address',
            'start_timestamp', 'end_timestamp', 'created_timestamp',
            'for_votes', 'for_percent', 'against_votes', 'against_percent',
            'abstain_votes', 'abstain_percent', 'executable_calls_count'
        ]

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            for proposal in proposals:
                metadata = proposal.get('metadata', {})
                creator = proposal.get('creator', {})
                proposer = proposal.get('proposer', {}) or {}
                start_info = proposal.get('start', {}) or {}
                end_info = proposal.get('end', {}) or {}
                block_info = proposal.get('block', {}) or {}

                vote_data = {'for': ('', ''), 'against': ('', ''), 'abstain': ('', '')}
                for stat in proposal.get('voteStats', []):
                    vtype = stat.get('type', '').lower()
                    if vtype in vote_data:
                        vote_data[vtype] = (self.format_votes(stat.get('votesCount', '')), 
                                           f"{stat.get('percent', 0):.2f}")

                row = [
                    proposal.get('id', ''),
                    proposal.get('onchainId', ''),
                    metadata.get('title', ''),
                    metadata.get('description', ''),
                    proposal.get('status', ''),
                    creator.get('address', ''),
                    proposer.get('address', ''),
                    self.format_timestamp(start_info.get('timestamp', '')),
                    self.format_timestamp(end_info.get('timestamp', '')),
                    self.format_timestamp(block_info.get('timestamp', '')),
                    vote_data['for'][0], vote_data['for'][1],
                    vote_data['against'][0], vote_data['against'][1],
                    vote_data['abstain'][0], vote_data['abstain'][1],
                    len(proposal.get('executableCalls', []))
                ]
                writer.writerow(row)

        print(f"Saved {len(proposals)} proposals to {filename}")
        return filename

    def display_proposals(self, proposals: List[Dict], limit: int = None):
        display_list = proposals[:limit] if limit else proposals
        for i, p in enumerate(display_list, 1):
            title = p.get('metadata', {}).get('title', 'No Title')
            print(f"{i}. [{p.get('status', '')}] {title}")


def main():
    api_key = os.getenv("TALLY_API_KEY", "")
    if not api_key:
        print("Error: TALLY_API_KEY not set")
        return

    fetcher = DAOProposalsFetcher(api_key)
    dao_name, dao_slugs, governor_ids = fetcher.get_dao_configuration()

    if not governor_ids:
        org_data = fetcher.find_dao_organization(dao_name, dao_slugs)
        if org_data:
            governor_ids = org_data.get('governorIds', [])

    if not governor_ids:
        print(f"Could not find governor IDs for {dao_name}")
        return

    print("\n1. Fetch 50 proposals  2. Fetch ALL proposals  3. Display only")
    choice = input("Choice: ").strip()

    if choice == "2":
        all_proposals = fetcher.fetch_all_proposals(dao_name, governor_ids)
        if all_proposals:
            fetcher.save_proposals_to_csv(all_proposals, dao_name)
    elif choice == "1":
        data = fetcher.fetch_proposals(governor_ids[0], limit=50)
        if data and "errors" not in data:
            proposals = data["data"]["proposals"]["nodes"]
            fetcher.save_proposals_to_csv(proposals, dao_name)
    else:
        data = fetcher.fetch_proposals(governor_ids[0], limit=50)
        if data and "errors" not in data:
            proposals = data["data"]["proposals"]["nodes"]
            fetcher.display_proposals(proposals, limit=20)


if __name__ == "__main__":
    main()