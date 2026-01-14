"""Voting Matrix Builder - Creates comprehensive voting matrices for DAO analysis."""

import requests
import json
import pandas as pd
import csv
from typing import Dict, List, Optional
import time
import datetime
from dataclasses import dataclass
import os
import threading


@dataclass
class DAOConfig:
    name: str
    slug: str
    alternative_slugs: List[str]
    expected_delegates: int
    description: str

    @classmethod
    def get_ens_config(cls):
        return cls("ENS DAO", "ens", ["ens-dao", "ensdao"], 37150, "Ethereum Name Service DAO")

    @classmethod
    def get_compound_config(cls):
        return cls("Compound DAO", "compound", ["compound-dao"], 17894, "Compound Finance DAO")

    @classmethod
    def get_custom_config(cls, name: str, slug: str, alternative_slugs: List[str] = None, expected_delegates: int = 1000):
        return cls(name, slug, alternative_slugs or [f"{slug}-dao"], expected_delegates, f"Custom DAO: {name}")


class OptimizedTallyAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.endpoint = "https://api.tally.xyz/query"
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        self.rate_limit_count = 0
        self.min_delay = 0.60
        self.max_delay = 5.0
        self.last_request_time = 0
        self.rate_limit_lock = threading.Lock()
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Api-Key': self.api_key,
            'Connection': 'keep-alive'
        })

    def smart_rate_limit(self):
        with self.rate_limit_lock:
            current_time = time.time()
            if current_time - self.last_request_time < self.min_delay:
                time.sleep(self.min_delay - (current_time - self.last_request_time))
            self.last_request_time = time.time()

    def make_request_optimized(self, query: str, variables: Optional[Dict] = None) -> Optional[Dict]:
        if variables is None:
            variables = {}
        self.request_count += 1
        payload = {'query': query, 'variables': variables}

        for attempt in range(3):
            try:
                self.smart_rate_limit()
                response = self.session.post(self.endpoint, json=payload, timeout=30)

                if response.status_code == 200:
                    data = response.json()
                    if 'errors' in data:
                        self.error_count += 1
                        if attempt == 2:
                            return None
                        continue
                    self.min_delay = max(0.50, self.min_delay * 0.98)
                    self.success_count += 1
                    return data
                elif response.status_code == 429:
                    self.rate_limit_count += 1
                    self.min_delay = min(self.min_delay * 1.5, 2.0)
                    time.sleep(min(2 * (2 ** attempt), self.max_delay))
                    continue
                elif response.status_code in [502, 503, 504]:
                    self.error_count += 1
                    time.sleep(min(2 * (2 ** attempt), self.max_delay))
                    continue
                else:
                    self.error_count += 1
                    return None
            except Exception:
                self.error_count += 1
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                    continue
        return None

    def get_stats(self):
        total = max(self.request_count, 1)
        return {
            'total_requests': self.request_count,
            'successful_requests': self.success_count,
            'failed_requests': self.error_count,
            'rate_limited_requests': self.rate_limit_count,
            'success_rate': f"{(self.success_count / total) * 100:.1f}%",
            'current_delay': f"{self.min_delay:.2f}s",
            'efficiency': f"{self.success_count / total:.2%}"
        }


class FastDAOFetcher:
    def __init__(self, api_key: str = None, dao_config: DAOConfig = None, force_refresh_votes: bool = False):
        self.api = OptimizedTallyAPI(api_key or os.getenv("TALLY_API_KEY", ""))
        self.dao_config = dao_config or DAOConfig.get_compound_config()
        self.force_refresh_votes = force_refresh_votes
        self.vote_batch_size = 5000
        self.delegate_batch_size = 200
        self.max_vote_pages = 100000
        self.session_start_time = time.time()
        self.checkpoint_file = f"{self.dao_config.slug}_checkpoint.json"
        self.vote_checkpoint_file = f"{self.dao_config.slug}_vote_checkpoint.json"
        self.checkpoint_data = self.load_checkpoint()

    def load_checkpoint(self) -> Dict:
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {'delegates': [], 'proposals': [], 'last_delegate_cursor': None, 'votes_cache': {}, 'processed_proposals': []}

    def save_checkpoint(self, delegates=None, proposals=None, cursor=None, votes_cache=None, processed_proposals=None):
        if delegates is not None:
            self.checkpoint_data['delegates'] = delegates
        if proposals is not None:
            self.checkpoint_data['proposals'] = proposals
        if cursor is not None:
            self.checkpoint_data['last_delegate_cursor'] = cursor
        if votes_cache is not None:
            self.checkpoint_data['votes_cache'] = votes_cache
        if processed_proposals is not None:
            self.checkpoint_data['processed_proposals'] = processed_proposals
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f)
        except:
            pass

    def load_vote_cursor(self, proposal_id: int) -> Optional[str]:
        try:
            if os.path.exists(self.vote_checkpoint_file):
                with open(self.vote_checkpoint_file, 'r') as f:
                    return json.load(f).get(str(proposal_id), {}).get('after_cursor')
        except:
            pass
        return None

    def save_vote_cursor(self, proposal_id: int, after_cursor: Optional[str]):
        try:
            data = {}
            if os.path.exists(self.vote_checkpoint_file):
                with open(self.vote_checkpoint_file, 'r') as f:
                    data = json.load(f) or {}
            data[str(proposal_id)] = {'after_cursor': after_cursor, 'timestamp': int(time.time())}
            with open(self.vote_checkpoint_file, 'w') as f:
                json.dump(data, f)
        except:
            pass

    def safe_get_nested(self, data: Dict, *keys, default=''):
        try:
            result = data
            for key in keys:
                if isinstance(result, dict) and key in result:
                    result = result[key]
                else:
                    return default
            return result if result is not None else default
        except:
            return default

    def get_organization_by_slug(self, slug: str) -> Dict:
        query = """
        query GetOrganization($input: OrganizationInput!) {
            organization(input: $input) {
                id slug name chainIds governorIds proposalsCount
                delegatesCount delegatesVotesCount tokenOwnersCount
            }
        }
        """
        return self.api.make_request_optimized(query, {"input": {"slug": slug}})

    def get_all_delegates_optimized(self, org_id: int) -> List[Dict]:
        existing = self.checkpoint_data.get('delegates', [])
        start_cursor = self.checkpoint_data.get('last_delegate_cursor')
        all_delegates = existing.copy() if existing and start_cursor else []
        after_cursor = start_cursor if existing and start_cursor else None

        query = """
        query GetDelegates($input: DelegatesInput!) {
            delegates(input: $input) {
                nodes {
                    ... on Delegate {
                        id
                        account { address name bio picture twitter ens }
                        votesCount delegatorsCount
                        organization { id name }
                        statement { statement statementSummary isSeekingDelegation }
                    }
                }
                pageInfo { firstCursor lastCursor count }
            }
        }
        """

        consecutive_failures = 0
        while consecutive_failures < 15:
            page_input = {"limit": self.delegate_batch_size}
            if after_cursor:
                page_input["afterCursor"] = after_cursor

            variables = {"input": {"filters": {"organizationId": org_id}, "page": page_input, "sort": {"sortBy": "id", "isDescending": False}}}
            result = self.api.make_request_optimized(query, variables)

            if not result or not result.get('data'):
                consecutive_failures += 1
                self.save_checkpoint(delegates=all_delegates, cursor=after_cursor)
                time.sleep(min(self.api.max_delay, 2 * 1.7 ** consecutive_failures))
                continue

            nodes = result.get('data', {}).get('delegates', {}).get('nodes', [])
            page_info = result.get('data', {}).get('delegates', {}).get('pageInfo', {})

            if not nodes:
                new_cursor = page_info.get('lastCursor')
                if new_cursor and new_cursor != after_cursor:
                    after_cursor = new_cursor
                    continue
                break

            consecutive_failures = 0
            valid = [d for d in nodes if self.validate_delegate_data(d)]
            existing_addrs = {d['account']['address'].lower() for d in all_delegates}
            valid = [d for d in valid if d['account']['address'].lower() not in existing_addrs]
            all_delegates.extend(valid)

            new_cursor = page_info.get('lastCursor')
            if not new_cursor or new_cursor == after_cursor:
                break
            after_cursor = new_cursor

        self.save_checkpoint(delegates=all_delegates, cursor=None)
        return all_delegates

    def validate_delegate_data(self, delegate: Dict) -> bool:
        try:
            if not delegate.get('account', {}).get('address'):
                return False
            if 'votesCount' in delegate:
                delegate['votesCount'] = float(delegate.get('votesCount', 0))
            if 'delegatorsCount' in delegate:
                delegate['delegatorsCount'] = int(delegate.get('delegatorsCount', 0))
            return True
        except:
            return False

    def get_all_proposals_optimized(self, org_id: int) -> List[Dict]:
        existing = self.checkpoint_data.get('proposals', [])
        if existing and len(existing) >= 37:
            return existing

        query = """
        query GetProposals($input: ProposalsInput!) {
            proposals(input: $input) {
                nodes {
                    ... on Proposal {
                        id onchainId
                        metadata { title description }
                        proposer { address name }
                        status
                        start { ... on Block { timestamp number } ... on BlocklessTimestamp { timestamp } }
                        end { ... on Block { timestamp number } ... on BlocklessTimestamp { timestamp } }
                        voteStats { type votesCount votersCount percent }
                        quorum
                    }
                }
                pageInfo { firstCursor lastCursor count }
            }
        }
        """

        all_proposals = []
        after_cursor = None
        for _ in range(100):
            page_input = {"limit": 100}
            if after_cursor:
                page_input["afterCursor"] = after_cursor

            variables = {"input": {"filters": {"organizationId": org_id, "includeArchived": True}, "page": page_input, "sort": {"sortBy": "id", "isDescending": True}}}
            result = self.api.make_request_optimized(query, variables)

            if not result or not result.get('data'):
                continue

            nodes = result.get('data', {}).get('proposals', {}).get('nodes', [])
            page_info = result.get('data', {}).get('proposals', {}).get('pageInfo', {})

            if not nodes:
                continue

            valid = [p for p in nodes if self.validate_proposal_data(p)]
            all_proposals.extend(valid)

            new_cursor = page_info.get('lastCursor')
            if not new_cursor or new_cursor == after_cursor:
                break
            after_cursor = new_cursor

        self.save_checkpoint(proposals=all_proposals)
        return all_proposals

    def validate_proposal_data(self, proposal: Dict) -> bool:
        return bool(proposal.get('id'))

    def get_votes_for_proposal_high_volume(self, proposal_id: int, expected_total_votes: Optional[int] = None) -> List[Dict]:
        votes_cache = self.checkpoint_data.get('votes_cache', {})
        if not self.force_refresh_votes and str(proposal_id) in votes_cache:
            return votes_cache[str(proposal_id)]

        query = """
        query GetVotes($input: VotesInput!) {
            votes(input: $input) {
                nodes {
                    ... on OnchainVote {
                        id type amount reason
                        voter { address name ens }
                        block { timestamp number }
                        txHash
                    }
                }
                pageInfo { firstCursor lastCursor count }
            }
        }
        """

        all_votes = []
        after_cursor = self.load_vote_cursor(proposal_id)
        consecutive_failures = 0

        while len(all_votes) < self.max_vote_pages * self.vote_batch_size and consecutive_failures < 50:
            page_input = {"limit": self.vote_batch_size}
            if after_cursor:
                page_input["afterCursor"] = after_cursor

            variables = {"input": {"filters": {"proposalId": proposal_id}, "page": page_input, "sort": {"sortBy": "id", "isDescending": False}}}
            result = self.api.make_request_optimized(query, variables)

            if not result or not result.get('data'):
                consecutive_failures += 1
                self.save_vote_cursor(proposal_id, after_cursor)
                time.sleep(min(self.api.max_delay, 2 * 1.7 ** (consecutive_failures // 5)))
                continue

            nodes = result.get('data', {}).get('votes', {}).get('nodes', [])
            page_info = result.get('data', {}).get('votes', {}).get('pageInfo', {})

            if not nodes:
                new_cursor = page_info.get('lastCursor')
                if new_cursor and new_cursor != after_cursor:
                    after_cursor = new_cursor
                    self.save_vote_cursor(proposal_id, after_cursor)
                    consecutive_failures += 1
                    continue
                break

            consecutive_failures = 0
            valid = [v for v in nodes if self.validate_vote_data(v)]
            all_votes.extend(valid)

            new_cursor = page_info.get('lastCursor')
            if not new_cursor or new_cursor == after_cursor:
                break
            after_cursor = new_cursor
            self.save_vote_cursor(proposal_id, after_cursor)

        if all_votes:
            votes_cache[str(proposal_id)] = all_votes
            self.save_checkpoint(votes_cache=votes_cache)

        return all_votes

    def normalize_vote_type(self, vote: Dict) -> str:
        vote_type = vote.get('type', '').strip().lower()
        mappings = {'for': 'For', 'yes': 'For', 'support': 'For', 'against': 'Against', 'no': 'Against', 'abstain': 'Abstain'}
        if vote_type in mappings:
            return mappings[vote_type]
        if vote.get('txHash'):
            return 'Voted'
        return 'Unknown'

    def validate_vote_data(self, vote: Dict) -> bool:
        voter = vote.get('voter')
        return voter and voter.get('address')

    def create_voting_matrix_fast(self, proposals: List[Dict], delegates: List[Dict], org_id: int):
        all_voting_data = []
        total_votes_found = 0
        proposals_with_votes = 0

        for i, proposal in enumerate(proposals):
            proposal_id = proposal['id']
            expected = None
            try:
                stats = proposal.get('voteStats') or []
                expected = sum(int(s.get('votersCount') or 0) for s in stats if isinstance(s, dict))
            except:
                pass

            votes = self.get_votes_for_proposal_high_volume(proposal_id, expected_total_votes=expected)
            if votes:
                total_votes_found += len(votes)
                proposals_with_votes += 1

            vote_map = {}
            for vote in votes:
                voter_info = vote.get('voter', {})
                if not voter_info.get('address'):
                    continue
                vote_map[voter_info['address'].lower()] = {
                    'vote': self.normalize_vote_type(vote),
                    'amount': str(vote.get('amount', '0') or '0'),
                    'raw_type': vote.get('type'),
                    'reason': self.safe_get_nested(vote, 'reason', default=''),
                    'block_timestamp': self.safe_get_nested(vote, 'block', 'timestamp', default=''),
                    'block_number': self.safe_get_nested(vote, 'block', 'number', default=''),
                    'tx_hash': self.safe_get_nested(vote, 'txHash', default='')
                }

            proposal_data = self.process_all_delegates_for_proposal(delegates, proposal, votes, vote_map)
            all_voting_data.extend(proposal_data)

        self.save_optimized_results(all_voting_data, total_votes_found, proposals_with_votes)

    def process_all_delegates_for_proposal(self, delegates: List[Dict], proposal: Dict, votes: List[Dict], vote_map: Dict) -> List[Dict]:
        proposal_data = []
        for delegate in delegates:
            account = delegate.get('account', {})
            addr = account.get('address', '').lower()
            if not addr:
                continue

            record = {
                'delegate_address': account.get('address', ''),
                'delegate_name': account.get('name', '') or addr[:10] + '...',
                'delegate_ens': account.get('ens', ''),
                'delegate_votes_count': float(delegate.get('votesCount', 0)),
                'delegate_delegators_count': int(delegate.get('delegatorsCount', 0)),
                'has_statement': bool((delegate.get('statement') or {}).get('statement', '').strip()),
                'seeking_delegation': (delegate.get('statement') or {}).get('isSeekingDelegation', False),
                'proposal_id': proposal['id'],
                'proposal_onchain_id': self.safe_get_nested(proposal, 'onchainId', default=''),
                'proposal_title': proposal.get('metadata', {}).get('title', f'Proposal {proposal["id"]}'),
                'proposal_status': proposal.get('status', 'unknown'),
                'proposal_start_timestamp': self.safe_get_nested(proposal, 'start', 'timestamp', default=''),
                'proposal_end_timestamp': self.safe_get_nested(proposal, 'end', 'timestamp', default=''),
                'is_active_delegate': float(delegate.get('votesCount', 0)) > 0,
                'dao_name': self.dao_config.name
            }

            if addr in vote_map:
                v = vote_map[addr]
                record.update({'vote': v['vote'], 'voting_amount': v['amount'], 'vote_type_raw': v['raw_type'],
                              'vote_reason': v['reason'], 'vote_timestamp': v['block_timestamp'],
                              'vote_block_number': v['block_number'], 'vote_tx_hash': v['tx_hash'], 'participated': True})
            else:
                record.update({'vote': 'Did Not Vote', 'voting_amount': '0', 'vote_type_raw': '', 'vote_reason': '',
                              'vote_timestamp': '', 'vote_block_number': '', 'vote_tx_hash': '', 'participated': False})
            proposal_data.append(record)
        return proposal_data

    def save_optimized_results(self, all_voting_data: List[Dict], total_votes_found: int, proposals_with_votes: int):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        dao_slug = self.dao_config.slug.replace(' ', '_').lower()

        try:
            df = pd.DataFrame(all_voting_data)
            df.to_csv(f'{dao_slug}_voting_matrix_{timestamp}.csv', index=False)

            summary = df.groupby(['delegate_address', 'delegate_name']).agg({
                'participated': 'sum', 'vote': 'count', 'delegate_votes_count': 'first',
                'delegate_delegators_count': 'first', 'has_statement': 'first'
            }).reset_index()
            summary.to_csv(f'{dao_slug}_delegate_summary_{timestamp}.csv', index=False)

            proposal_summary = df.groupby(['proposal_id', 'proposal_title', 'proposal_status']).agg({
                'participated': 'sum', 'delegate_address': 'nunique'
            }).reset_index()
            proposal_summary.to_csv(f'{dao_slug}_proposal_analysis_{timestamp}.csv', index=False)

            print(f"Saved: {dao_slug}_voting_matrix_{timestamp}.csv ({len(df)} records)")
        except Exception as e:
            # Fallback to CSV streaming
            filename = f'{dao_slug}_voting_matrix_{timestamp}.csv'
            if all_voting_data:
                fieldnames = list(all_voting_data[0].keys())
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(all_voting_data)
            print(f"Saved (fallback): {filename}")

    def cleanup_checkpoint_files(self):
        for f in [self.checkpoint_file, self.vote_checkpoint_file]:
            if os.path.exists(f):
                os.remove(f)


def main():
    api_key = os.getenv("TALLY_API_KEY", "")
    if not api_key:
        print("Error: TALLY_API_KEY not set")
        return

    print("DAO Voting Matrix Builder")
    print("1. Compound  2. ENS  3. Custom")
    choice = input("Select (1-3): ").strip()

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

    confirm = input(f"Start extraction for {dao_config.name}? (y/N): ").strip().lower()
    if confirm != 'y':
        return

    fetcher = FastDAOFetcher(api_key=api_key, dao_config=dao_config)

    result = fetcher.get_organization_by_slug(dao_config.slug)
    if not result or not result.get('data', {}).get('organization'):
        print(f"Could not find {dao_config.name}")
        return

    org = result['data']['organization']
    org_id = org['id']
    print(f"Found: {org['name']} (ID: {org_id})")

    delegates = fetcher.get_all_delegates_optimized(org_id)
    proposals = fetcher.get_all_proposals_optimized(org_id)

    if delegates and proposals:
        fetcher.create_voting_matrix_fast(proposals, delegates, org_id)
        fetcher.cleanup_checkpoint_files()
        print("Extraction complete!")


if __name__ == "__main__":
    main()
