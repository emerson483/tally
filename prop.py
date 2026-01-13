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
        self.headers = {
            "Api-Key": api_key,
            "Content-Type": "application/json"
        }
        self.request_count = 0

    def get_dao_configuration(self) -> Tuple[str, List[str]]:
        """Interactive DAO configuration selection"""
        print("\n" + "="*70)
        print("DAO CONFIGURATION SELECTOR")
        print("="*70)

        dao_configs = {
            '1': {
                'name': 'Arbitrum DAO',
                'slugs': ['arbitrum', 'arbitrum-dao', 'arbitrumfoundation'],
                'governor_ids': ['eip155:42161:0xf07DeD9dC292157749B6Fd268E37DF6EA38395B9']
            },
            '2': {
                'name': 'Nouns DAO',
                'slugs': ['nounsdao', 'nouns', 'nouns-dao', 'nounsDAO'],
                'governor_ids': []
            },
            '3': {
                'name': 'Compound',
                'slugs': ['compound', 'compound-dao', 'compounddao'],
                'governor_ids': []
            },
            '4': {
                'name': 'Uniswap',
                'slugs': ['uniswap', 'uniswap-dao', 'uni'],
                'governor_ids': []
            },
            '5': {
                'name': 'ENS DAO',
                'slugs': ['ens', 'ens-dao', 'ensdao', 'ethereum-name-service'],
                'governor_ids': []
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
                    return config['name'], config['slugs'], config['governor_ids']

                elif choice == "6":
                    return self.get_custom_dao_config()

                else:
                    print("Invalid choice. Please enter 1, 2, 3, 4, 5, or 6.")

            except KeyboardInterrupt:
                print("\nGoodbye!")
                exit()

    def get_custom_dao_config(self) -> Tuple[str, List[str], List[str]]:
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

            print("\nOPTIONAL: Governor Contract Address")
            print("   If you know the governor contract address, enter it here.")
            print("   Format: eip155:CHAIN_ID:0x... or just 0x...")
            print("   Leave empty to auto-discover from organization")

            governor_input = input("Governor Address (optional): ").strip()
            governor_ids = []
            if governor_input:
                # Clean up the governor address
                if not governor_input.startswith('eip155:'):
                    # Ask for chain ID if not provided
                    chain_id = input("Chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum): ").strip()
                    if chain_id.isdigit():
                        governor_input = f"eip155:{chain_id}:{governor_input}"
                governor_ids = [governor_input]

            print(f"Custom DAO configured: {name}")
            print(f"   Will try slugs: {', '.join(all_slugs)}")
            if governor_ids:
                print(f"   Governor IDs: {', '.join(governor_ids)}")
            return name, all_slugs, governor_ids

        except KeyboardInterrupt:
            print("\nGoodbye!")
            exit()
        except ValueError as e:
            print(f"Error: {e}")
            return self.get_custom_dao_config()

    def find_dao_organization(self, dao_name: str, dao_slugs: List[str]) -> Optional[Dict]:
        """Find DAO organization and get governor IDs"""
        query = """
        query GetOrganization($input: OrganizationInput!) {
          organization(input: $input) {
            id
            name
            slug
            chainIds
            proposalsCount
            delegatesCount
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
                self.request_count += 1
                response = requests.post(
                    self.api_url,
                    headers=self.headers,
                    json={
                        'query': query,
                        'variables': {
                            'input': {'slug': slug}
                        }
                    }
                )

                if response.status_code == 200:
                    data = response.json()
                    if 'errors' not in data and data.get('data', {}).get('organization'):
                        org = data['data']['organization']
                        print(f"Found organization: {org.get('name')} (slug: {org.get('slug')})")
                        print(f"   Organization ID: {org.get('id')}")
                        print(f"   Total proposals: {org.get('proposalsCount', 'Unknown')}")
                        print(f"   Governor IDs: {org.get('governorIds', [])}")
                        return org
                    else:
                        print(f"   No data for slug: '{slug}'")
                else:
                    print(f"   HTTP Error {response.status_code}")

            except Exception as e:
                print(f"   Error testing slug '{slug}': {e}")

        return None

    def fetch_proposals(self, governor_id: str, limit: int = 50, after_cursor: str = None) -> Optional[Dict]:
        """Fetch proposals for a specific governor"""
        # Build page input
        page_input = f"limit: {limit}"
        if after_cursor:
            page_input += f', afterCursor: "{after_cursor}"'

        query = f"""
        query ProposalsDetailed {{
            proposals(input: {{
                filters: {{
                    governorId: "{governor_id}"
                }}
                page: {{
                    {page_input}
                }}
                sort: {{
                    isDescending: true
                    sortBy: id
                }}
            }}) {{
                nodes {{
                    ... on Proposal {{
                        id
                        onchainId
                        metadata {{
                            title
                            description
                        }}
                        status
                        creator {{
                            address
                            name
                            picture
                        }}
                        proposer {{
                            address
                            name
                        }}
                        voteStats {{
                            type
                            votesCount
                            votersCount
                            percent
                        }}
                        start {{
                            ... on Block {{
                                number
                                timestamp
                            }}
                        }}
                        end {{
                            ... on Block {{
                                number
                                timestamp
                            }}
                        }}
                        block {{
                            number
                            timestamp
                        }}
                        executableCalls {{
                            calldata
                            signature
                            target
                            value
                        }}
                    }}
                }}
                pageInfo {{
                    count
                    firstCursor
                    lastCursor
                }}
            }}
        }}
        """

        self.request_count += 1
        response = requests.post(self.api_url, headers=self.headers, json={"query": query})

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text}")
            return None

    def format_timestamp(self, timestamp: str) -> str:
        """Convert timestamp to readable date"""
        if not timestamp:
            return "N/A"

        try:
            if timestamp.isdigit():
                return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S UTC")
            else:
                if 'T' in timestamp:
                    clean_timestamp = timestamp.replace('Z', '+00:00')
                    dt = datetime.fromisoformat(clean_timestamp.replace('Z', ''))
                    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                else:
                    return timestamp
        except (ValueError, TypeError):
            return f"Invalid timestamp: {timestamp}"

    def format_votes(self, votes: str) -> str:
        """Format vote counts in a more readable way"""
        if votes:
            try:
                readable_votes = int(votes) / 10**18
                return f"{readable_votes:,.2f}"
            except (ValueError, TypeError):
                return str(votes)
        return "0"

    def fetch_all_proposals(self, dao_name: str, governor_ids: List[str]) -> List[Dict]:
        """Fetch ALL proposals for the DAO using governor IDs"""
        all_proposals = []

        if not governor_ids:
            print("No governor IDs available")
            return []

        for governor_id in governor_ids:
            print(f"\nFetching proposals for governor: {governor_id}")
            after_cursor = None
            page_num = 1

            while True:
                print(f"Fetching page {page_num}...")

                proposals_data = self.fetch_proposals(governor_id, limit=50, after_cursor=after_cursor)

                if not proposals_data or "errors" in proposals_data:
                    if proposals_data and "errors" in proposals_data:
                        print("GraphQL Errors:")
                        for error in proposals_data["errors"]:
                            print(f"- {error['message']}")
                    break

                proposals = proposals_data["data"]["proposals"]["nodes"]

                if not proposals:
                    break

                all_proposals.extend(proposals)
                print(f"  Found {len(proposals)} proposals on page {page_num}")

                # Check if there are more pages
                page_info = proposals_data["data"]["proposals"]["pageInfo"]
                if not page_info.get("lastCursor"):
                    break

                after_cursor = page_info["lastCursor"]
                page_num += 1
                time.sleep(1.1)

        print(f"\nTotal proposals fetched for {dao_name}: {len(all_proposals)}")
        return all_proposals

    def save_proposals_to_csv(self, proposals: List[Dict], dao_name: str, filename: str = None) -> str:
        """Save proposals data to CSV file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dao_slug = dao_name.lower().replace(' ', '_').replace('dao', '').strip('_')
            filename = f"{dao_slug}_dao_proposals_{timestamp}.csv"

        headers = [
            'proposal_id', 'onchain_id', 'title', 'description', 'status',
            'creator_address', 'creator_name', 'proposer_address', 'proposer_name',
            'start_timestamp', 'start_block', 'end_timestamp', 'end_block',
            'created_timestamp', 'created_block',
            'for_votes', 'for_votes_count', 'for_voters_count', 'for_percent',
            'against_votes', 'against_votes_count', 'against_voters_count', 'against_percent',
            'abstain_votes', 'abstain_votes_count', 'abstain_voters_count', 'abstain_percent',
            'executable_calls_count', 'executable_calls_details'
        ]

        print(f"Saving {len(proposals)} proposals to {filename}...")

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            for proposal in proposals:
                # Extract basic info
                proposal_id = proposal.get('id', '')
                onchain_id = proposal.get('onchainId', '')
                title = proposal.get('metadata', {}).get('title', '')
                description = proposal.get('metadata', {}).get('description', '')
                status = proposal.get('status', '')

                # Creator info
                creator = proposal.get('creator', {})
                creator_address = creator.get('address', '')
                creator_name = creator.get('name', '')

                # Proposer info
                proposer = proposal.get('proposer', {})
                proposer_address = proposer.get('address', '') if proposer else ''
                proposer_name = proposer.get('name', '') if proposer else ''

                # Time info
                start_info = proposal.get('start', {})
                start_timestamp = self.format_timestamp(start_info.get('timestamp', '')) if start_info else ''
                start_block = start_info.get('number', '') if start_info else ''

                end_info = proposal.get('end', {})
                end_timestamp = self.format_timestamp(end_info.get('timestamp', '')) if end_info else ''
                end_block = end_info.get('number', '') if end_info else ''

                block_info = proposal.get('block', {})
                created_timestamp = self.format_timestamp(block_info.get('timestamp', '')) if block_info else ''
                created_block = block_info.get('number', '') if block_info else ''

                # Initialize vote data
                for_votes = for_votes_count = for_voters_count = for_percent = ''
                against_votes = against_votes_count = against_voters_count = against_percent = ''
                abstain_votes = abstain_votes_count = abstain_voters_count = abstain_percent = ''

                # Parse vote statistics
                vote_stats = proposal.get('voteStats', [])
                for stat in vote_stats:
                    vote_type = stat.get('type', '').upper()
                    votes_count = stat.get('votesCount', '')
                    voters_count = stat.get('votersCount', '')
                    percent = stat.get('percent', 0)
                    formatted_votes = self.format_votes(votes_count)

                    # Format percentage properly
                    formatted_percent = f"{percent:.2f}" if isinstance(percent, (int, float)) else percent

                    if vote_type == 'FOR':
                        for_votes = formatted_votes
                        for_votes_count = votes_count
                        for_voters_count = voters_count
                        for_percent = formatted_percent
                    elif vote_type == 'AGAINST':
                        against_votes = formatted_votes
                        against_votes_count = votes_count
                        against_voters_count = voters_count
                        against_percent = formatted_percent
                    elif vote_type == 'ABSTAIN':
                        abstain_votes = formatted_votes
                        abstain_votes_count = votes_count
                        abstain_voters_count = voters_count
                        abstain_percent = formatted_percent

                # Executable calls info
                executable_calls = proposal.get('executableCalls', [])
                executable_calls_count = len(executable_calls)

                calls_details = []
                for call in executable_calls:
                    call_detail = f"Target: {call.get('target', 'N/A')}, Function: {call.get('signature', 'N/A')}, Value: {call.get('value', '0')}"
                    calls_details.append(call_detail)
                executable_calls_details = ' | '.join(calls_details)

                row = [
                    proposal_id, onchain_id, title, description, status,
                    creator_address, creator_name, proposer_address, proposer_name,
                    start_timestamp, start_block, end_timestamp, end_block,
                    created_timestamp, created_block,
                    for_votes, for_votes_count, for_voters_count, for_percent,
                    against_votes, against_votes_count, against_voters_count, against_percent,
                    abstain_votes, abstain_votes_count, abstain_voters_count, abstain_percent,
                    executable_calls_count, executable_calls_details
                ]

                writer.writerow(row)

        print(f"Successfully saved {len(proposals)} proposals to {filename}")
        return filename

    def display_proposals(self, proposals: List[Dict], show_descriptions: bool = True, limit: int = None):
        """Display proposals in a formatted way"""
        display_proposals = proposals[:limit] if limit else proposals

        print(f"Found {len(proposals)} proposals (showing {len(display_proposals)}):")
        print("="*80)

        for i, proposal in enumerate(display_proposals, 1):
            print(f"\n[{i}] PROPOSAL #{proposal['onchainId']}")
            print("="*60)

            title = proposal.get('metadata', {}).get('title', 'No Title Available')
            description = proposal.get('metadata', {}).get('description', 'No Description Available')

            print(f"Title: {title}")
            print(f"Status: {proposal['status'].upper()}")
            print(f"Creator: {proposal['creator']['address']}")
            if proposal['creator'].get('name'):
                print(f"Creator Name: {proposal['creator']['name']}")

            # Timestamps
            if proposal.get('start'):
                print(f"Start Time: {self.format_timestamp(proposal['start']['timestamp'])}")
                print(f"Start Block: {proposal['start']['number']}")
            if proposal.get('end'):
                print(f"End Time: {self.format_timestamp(proposal['end']['timestamp'])}")
                print(f"End Block: {proposal['end']['number']}")

            # Creation info
            if proposal.get('block'):
                print(f"Created: {self.format_timestamp(proposal['block']['timestamp'])}")
                print(f"Creation Block: {proposal['block']['number']}")

            # Description
            if show_descriptions:
                print(f"\nDescription:")
                print("-" * 40)
                if description and description != 'No Description Available':
                    if len(description) > 1000:
                        print(f"{description[:1000]}...")
                        print(f"\n[Description truncated - Full length: {len(description)} characters]")
                    else:
                        print(description)
                else:
                    print("No detailed description available in metadata")

            # Vote Statistics
            if proposal['voteStats']:
                print(f"\nVote Statistics:")
                print("-" * 40)
                for stat in proposal['voteStats']:
                    votes_formatted = self.format_votes(stat['votesCount'])
                    percent = stat.get('percent', 0)
                    formatted_percent = f"{percent:.2f}" if isinstance(percent, (int, float)) else percent
                    print(f"  {stat['type'].upper()}: {votes_formatted} tokens ({formatted_percent}%)")
                    if stat.get('votersCount'):
                        print(f"    Number of voters: {stat['votersCount']}")

            # Executable calls
            if proposal.get('executableCalls') and len(proposal['executableCalls']) > 0:
                print(f"\nContract Calls:")
                print("-" * 40)
                for j, call in enumerate(proposal['executableCalls'], 1):
                    print(f"  Call {j}:")
                    print(f"    Target: {call.get('target', 'N/A')}")
                    print(f"    Function: {call.get('signature', 'N/A')}")
                    print(f"    Value: {call.get('value', '0')} ETH")

            print("\n" + "="*80)

def main():
    # Get API key from environment variable
    API_KEY = os.getenv("TALLY_API_KEY", "")

    if not API_KEY:
        print("Error: TALLY_API_KEY environment variable is not set.")
        print("Set it using: set TALLY_API_KEY=your_api_key_here")
        return

    fetcher = DAOProposalsFetcher(API_KEY)

    print("DAO Proposal Fetcher")
    print("="*50)

    try:
        # Get DAO configuration
        dao_name, dao_slugs, governor_ids = fetcher.get_dao_configuration()

        # Find organization if no governor IDs provided
        if not governor_ids:
            print("No governor IDs provided, searching for organization...")
            org_data = fetcher.find_dao_organization(dao_name, dao_slugs)
            if org_data:
                governor_ids = org_data.get('governorIds', [])

            if not governor_ids:
                print(f"Could not find governor IDs for {dao_name}")
                print("Please try providing a governor contract address manually.")
                return

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

        else:  # Option 3
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

if __name__ == "__main__":
    main()