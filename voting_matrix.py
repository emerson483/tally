import requests
import json
import pandas as pd
import csv
from typing import Dict, List, Optional
import time
import datetime
from dataclasses import dataclass
import random
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aiohttp
 

@dataclass
class DAOConfig:
    """Configuration class for different DAOs"""
    name: str
    slug: str
    alternative_slugs: List[str]
    expected_delegates: int
    description: str

    @classmethod
    def get_ens_config(cls):
        return cls(
            name="ENS DAO",
            slug="ens",
            alternative_slugs=["ens-dao", "ensdao", "ens_dao", "ethereum-name-service", "ensdomains"],
            expected_delegates=37150,
            description="Ethereum Name Service DAO"
        )

    @classmethod
    def get_compound_config(cls):
        return cls(
            name="Compound DAO",
            slug="compound",
            alternative_slugs=["compound-dao", "compounddao", "compound_dao"],
            expected_delegates=17894,
            description="Compound Finance DAO"
        )

    @classmethod
    def get_custom_config(cls, name: str, slug: str, alternative_slugs: List[str] = None, expected_delegates: int = 1000):
        return cls(
            name=name,
            slug=slug,
            alternative_slugs=alternative_slugs or [f"{slug}-dao", f"{slug}dao", f"{slug}_dao"],
            expected_delegates=expected_delegates,
            description=f"Custom DAO: {name}"
        )

class OptimizedTallyAPI:
    """Optimized Tally API client for maximum speed"""
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.endpoint = "https://api.tally.xyz/query"
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        self.rate_limit_count = 0

        # Optimized adaptive rate limiting
        self.min_delay = 0.60  # Start a bit aggressive; adapt on 429s
        self.max_delay = 5.0   # Maximum backoff delay
        self.last_request_time = 0
        self.rate_limit_lock = threading.Lock()

        # Persistent session for connection reuse
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Api-Key': self.api_key,
            'Connection': 'keep-alive',
            'User-Agent': 'Optimized-DAO-Analyzer/2.0'
        })

    def smart_rate_limit(self):
        """Intelligent rate limiting with adaptive delays"""
        with self.rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time

            if time_since_last < self.min_delay:
                sleep_time = self.min_delay - time_since_last
                time.sleep(sleep_time)

            self.last_request_time = time.time()

    def make_request_optimized(self, query: str, variables: Optional[Dict] = None) -> Optional[Dict]:
        """Optimized request with intelligent retry logic"""
        if variables is None:
            variables = {}

        self.request_count += 1
        payload = {'query': query, 'variables': variables}

        max_retries = 3
        base_delay = 2

        for attempt in range(max_retries):
            try:
                self.smart_rate_limit()

                response = self.session.post(
                    self.endpoint,
                    json=payload,
                    timeout=30,
                    allow_redirects=True
                )

                if response.status_code == 200:
                    data = response.json()

                    if 'errors' in data:
                        self.error_count += 1
                        print(f"GraphQL Errors: {data['errors']}")
                        if attempt == max_retries - 1:
                            return None
                        continue

                    # Slightly decrease delay on successful responses (down to a floor)
                    self.min_delay = max(0.50, self.min_delay * 0.98)
                    self.success_count += 1
                    return data

                elif response.status_code == 429:
                    # Rate limited - implement exponential backoff
                    self.rate_limit_count += 1
                    backoff_time = min(base_delay * (2 ** attempt), self.max_delay)
                    self.min_delay = min(self.min_delay * 1.5, 2.0)  # Adaptive delay increase

                    print(f"Rate limited. Backing off for {backoff_time}s (attempt {attempt + 1})")
                    time.sleep(backoff_time)
                    continue

                elif response.status_code in [502, 503, 504]:
                    # Server errors - retry with exponential backoff
                    self.error_count += 1
                    backoff_time = min(base_delay * (2 ** attempt), self.max_delay)
                    print(f"Server error {response.status_code}. Retrying in {backoff_time}s")
                    time.sleep(backoff_time)
                    continue

                else:
                    self.error_count += 1
                    print(f"HTTP Error {response.status_code}: {response.text[:200]}")
                    return None

            except requests.exceptions.Timeout:
                self.error_count += 1
                print(f"Request timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(base_delay * (attempt + 1))
                    continue

            except requests.exceptions.ConnectionError:
                self.error_count += 1
                print(f"Connection error on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(base_delay * (attempt + 1))
                    continue

            except Exception as e:
                self.error_count += 1
                print(f"Unexpected error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(base_delay * (attempt + 1))
                    continue

        print(f"Failed after {max_retries} attempts")
        return None

    def get_stats(self):
        """Get current API usage statistics"""
        total_requests = max(self.request_count, 1)
        return {
            'total_requests': self.request_count,
            'successful_requests': self.success_count,
            'failed_requests': self.error_count,
            'rate_limited_requests': self.rate_limit_count,
            'success_rate': f"{(self.success_count / total_requests) * 100:.1f}%",
            'current_delay': f"{self.min_delay:.2f}s",
            'efficiency': f"{self.success_count / total_requests:.2%}"
        }


class FastDAOFetcher:
    """
    FAST DAO DATA FETCHER - OPTIMIZED FOR SPEED
    
    This class is designed for maximum speed and efficiency.
    Key optimizations:
    - Minimal file I/O during extraction
    - Process all delegates at once per proposal
    - Only save final results, not intermediate chunks
    - Optimized memory usage
    """
    def __init__(self, api_key: str = None, dao_config: DAOConfig = None, force_refresh_votes: bool = False):
        self.api = OptimizedTallyAPI(api_key or os.getenv("TALLY_API_KEY", ""))
        self.dao_config = dao_config or DAOConfig.get_compound_config()
        self.force_refresh_votes = force_refresh_votes

        # Optimized settings for fast processing
        self.vote_batch_size = 5000  # Large batch size for votes
        self.delegate_batch_size = 200  # Larger batch size for delegates
        self.max_vote_pages = 100000  # Virtually unlimited pages
        self.vote_timeout = 300  # Extended timeout for large queries
        self.session_start_time = time.time()
        
        # Checkpoint system
        self.checkpoint_file = f"{self.dao_config.slug}_checkpoint.json"
        self.vote_checkpoint_file = f"{self.dao_config.slug}_vote_checkpoint.json"
        self.checkpoint_data = self.load_checkpoint()
 

        print(f"=� FAST MODE: {self.dao_config.name}")
        print(f"� Optimized for maximum speed")
        print(f"=� Batch sizes: {self.delegate_batch_size} delegates, {self.vote_batch_size} votes")
        print(f"=� Max vote pages: {self.max_vote_pages:,} (virtually unlimited)")
        print(f"<� Target: {self.dao_config.expected_delegates:,} delegates")
        print(f"=� Minimal file I/O - only save final results")

    def load_checkpoint(self) -> Dict:
        """Load checkpoint data if it exists"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                print(f"=� Loaded checkpoint: {len(data.get('delegates', []))} delegates saved")
                return data
            except:
                pass
        return {
            'delegates': [],
            'proposals': [],
            'last_delegate_cursor': None,
            'votes_cache': {},
            'processed_proposals': []
        }

    def save_checkpoint(self, delegates: List[Dict] = None, proposals: List[Dict] = None, cursor: str = None, votes_cache: Dict = None, processed_proposals: List[int] = None):
        """Save checkpoint data"""
        if delegates is not None:
            self.checkpoint_data['delegates'] = delegates
        if proposals is not None:
            self.checkpoint_data['proposals'] = proposals
        # Update cursor and completion flag
        if cursor is not None:
            self.checkpoint_data['last_delegate_cursor'] = cursor
            # If a cursor exists, delegate fetching is not complete
            self.checkpoint_data['delegates_complete'] = False
        if votes_cache is not None:
            self.checkpoint_data['votes_cache'] = votes_cache
        if processed_proposals is not None:
            self.checkpoint_data['processed_proposals'] = processed_proposals

        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f)
        except Exception as e:
            print(f"� Could not save checkpoint: {e}")

    def load_vote_cursor(self, proposal_id: int) -> Optional[str]:
        """Load the last vote cursor for a proposal if available."""
        try:
            if os.path.exists(self.vote_checkpoint_file):
                with open(self.vote_checkpoint_file, 'r') as f:
                    data = json.load(f)
                return data.get(str(proposal_id), {}).get('after_cursor')
        except Exception:
            pass
        return None

    def save_vote_cursor(self, proposal_id: int, after_cursor: Optional[str]) -> None:
        """Persist the current vote cursor for resume on interruptions."""
        try:
            data = {}
            if os.path.exists(self.vote_checkpoint_file):
                try:
                    with open(self.vote_checkpoint_file, 'r') as f:
                        data = json.load(f) or {}
                except Exception:
                    data = {}
            data[str(proposal_id)] = {
                'after_cursor': after_cursor,
                'timestamp': int(time.time())
            }
            with open(self.vote_checkpoint_file, 'w') as f:
                json.dump(data, f)
        except Exception:
            # Non-fatal
            pass

    def safe_get_nested(self, data: Dict, *keys, default=''):
        """Safely get nested dictionary values"""
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
        """Get organization by slug using optimized API client"""
        query = """
        query GetOrganization($input: OrganizationInput!) {
            organization(input: $input) {
                id
                slug
                name
                chainIds
                governorIds
                proposalsCount
                delegatesCount
                delegatesVotesCount
                tokenOwnersCount
                hasActiveProposals
            }
        }
        """
        variables = {"input": {"slug": slug}}
        return self.api.make_request_optimized(query, variables)

    def get_all_delegates_optimized(self, org_id: int) -> List[Dict]:
        """Get all delegates with optimized pagination"""
        existing_delegates = self.checkpoint_data.get('delegates', [])
        start_cursor = self.checkpoint_data.get('last_delegate_cursor')

        if existing_delegates and start_cursor:
            print(f"=� Resuming from checkpoint: {len(existing_delegates)} delegates already fetched")
            all_delegates = existing_delegates.copy()
            after_cursor = start_cursor
        else:
            all_delegates = []
            after_cursor = None

        query = """
        query GetDelegates($input: DelegatesInput!) {
            delegates(input: $input) {
                nodes {
                    ... on Delegate {
                        id
                        account {
                            address
                            name
                            bio
                            picture
                            twitter
                            ens
                        }
                        votesCount
                        delegatorsCount
                        organization {
                            id
                            name
                        }
                        statement {
                            statement
                            statementSummary
                            isSeekingDelegation
                        }
                    }
                }
                pageInfo {
                    firstCursor
                    lastCursor
                    count
                }
            }
        }
        """

        page_count = 0
        batch_size = self.delegate_batch_size
        consecutive_failures = 0
        backoff_seconds = 2
        max_consecutive_failures = 15

        print(f"=� OPTIMIZED DELEGATE FETCHING")
        print(f"=� Batch size: {batch_size}")
        print(f"� Using optimized API client with smart rate limiting")

        start_time = time.time()
        last_checkpoint_time = start_time

        while consecutive_failures < max_consecutive_failures:
            page_input = {"limit": batch_size}
            if after_cursor:
                page_input["afterCursor"] = after_cursor

            variables = {
                "input": {
                    "filters": {"organizationId": org_id},
                    "page": page_input,
                    "sort": {"sortBy": "id", "isDescending": False}
                }
            }

            # Checkpoint every 5 minutes
            current_time = time.time()
            if current_time - last_checkpoint_time > 300:
                elapsed_hours = (current_time - start_time) / 3600
                progress_pct = (len(all_delegates) / self.dao_config.expected_delegates * 100) if self.dao_config.expected_delegates > 0 else 0
                print(f"=� Progress: {len(all_delegates):,}/{self.dao_config.expected_delegates:,} ({progress_pct:.1f}%)")
                self.save_checkpoint(delegates=all_delegates, cursor=after_cursor)
                last_checkpoint_time = current_time

            result = self.api.make_request_optimized(query, variables)

            if not result or not result.get('data'):
                consecutive_failures += 1
                print(f"� Failed to fetch delegates page {page_count + 1}")
                # Save progress and back off before retrying
                self.save_checkpoint(delegates=all_delegates, cursor=after_cursor)
                time.sleep(min(self.api.max_delay, backoff_seconds))
                backoff_seconds = min(self.api.max_delay, max(2, backoff_seconds * 1.7))
                continue

            delegates_data = result.get('data', {}).get('delegates', {})
            delegates_batch = delegates_data.get('nodes', [])
            page_info = delegates_data.get('pageInfo', {})

            if not delegates_batch:
                consecutive_failures += 1
                new_cursor = page_info.get('lastCursor')
                if new_cursor and new_cursor != after_cursor:
                    after_cursor = new_cursor
                    page_count += 1
                    continue
                else:
                    print("� No more delegates available")
                    break

            consecutive_failures = 0

            # Validate and add delegates
            valid_delegates = []
            for delegate in delegates_batch:
                if self.validate_delegate_data(delegate):
                    valid_delegates.append(delegate)

            # Check for duplicates
            new_addresses = set(d['account']['address'].lower() for d in valid_delegates)
            existing_addresses = set(d['account']['address'].lower() for d in all_delegates)
            duplicates = new_addresses.intersection(existing_addresses)

            if duplicates:
                valid_delegates = [d for d in valid_delegates
                                 if d['account']['address'].lower() not in existing_addresses]

            all_delegates.extend(valid_delegates)
            page_count += 1

            # Progress reporting with API stats
            api_stats = self.api.get_stats()
            print(f"=� Page {page_count}: +{len(valid_delegates)} delegates (total: {len(all_delegates):,}) | API: {api_stats['success_rate']} success rate")

            new_cursor = page_info.get('lastCursor')
            if not new_cursor or new_cursor == after_cursor:
                print("� Reached end of pagination")
                break

            after_cursor = new_cursor

        # Mark completion by saving delegates with no cursor
        self.save_checkpoint(delegates=all_delegates, cursor=None)
        
        # Final API statistics
        final_stats = self.api.get_stats()
        elapsed_time = time.time() - start_time
        rate = len(all_delegates) / elapsed_time if elapsed_time > 0 else 0
        
        print(f"<� Retrieved {len(all_delegates):,} delegates total")
        print(f"� Fetch rate: {rate:.1f} delegates/sec")
        print(f"=� API Performance: {final_stats['success_rate']} success rate, {final_stats['total_requests']} requests")
        
        return all_delegates

    def validate_delegate_data(self, delegate: Dict) -> bool:
        """Validate delegate data"""
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
        """Get ALL proposals with robust pagination"""
        existing_proposals = self.checkpoint_data.get('proposals', [])
        if existing_proposals and len(existing_proposals) >= 37:
            print(f"=� Using cached proposals: {len(existing_proposals)}")
            return existing_proposals
        elif existing_proposals:
            print(f"� Cached proposals incomplete: {len(existing_proposals)}/37 expected")
            print(f"   Will fetch fresh data to ensure completeness")

        query = """
        query GetProposals($input: ProposalsInput!) {
            proposals(input: $input) {
                nodes {
                    ... on Proposal {
                        id
                        onchainId
                        metadata {
                            title
                            description
                        }
                        proposer {
                            address
                            name
                        }
                        status
                        start {
                            ... on Block {
                                timestamp
                                number
                            }
                            ... on BlocklessTimestamp {
                                timestamp
                            }
                        }
                        end {
                            ... on Block {
                                timestamp
                                number
                            }
                            ... on BlocklessTimestamp {
                                timestamp
                            }
                        }
                        voteStats {
                            type
                            votesCount
                            votersCount
                            percent
                        }
                        quorum
                    }
                }
                pageInfo {
                    firstCursor
                    lastCursor
                    count
                }
            }
        }
        """

        all_proposals = []
        after_cursor = None
        page_count = 0
        batch_size = 100
        consecutive_failures = 0
        max_pages = 100

        print(f"=� Fetching ALL proposals (expecting 37+)...")

        while page_count < max_pages and consecutive_failures < 10:
            page_input = {"limit": batch_size}
            if after_cursor:
                page_input["afterCursor"] = after_cursor

            variables = {
                "input": {
                    "filters": {
                        "organizationId": org_id,
                        "includeArchived": True
                    },
                    "page": page_input,
                    "sort": {"sortBy": "id", "isDescending": True}
                }
            }

            result = self.api.make_request_optimized(query, variables)

            if not result.get('data'):
                consecutive_failures += 1
                print(f"      � Failed to fetch proposals page {page_count + 1}")
                if 'errors' in result:
                    print(f"         GraphQL errors: {result['errors']}")
                continue

            proposals_data = result.get('data', {}).get('proposals', {})
            proposals_batch = proposals_data.get('nodes', [])
            page_info = proposals_data.get('pageInfo', {})

            if not proposals_batch:
                print(f"      =� No proposals in page {page_count + 1}")
                consecutive_failures += 1
                continue

            consecutive_failures = 0

            # More lenient validation - accept proposals even without titles
            valid_proposals = []
            for proposal in proposals_batch:
                if self.validate_proposal_data_improved(proposal):
                    valid_proposals.append(proposal)
                else:
                    print(f"      � Skipping invalid proposal: {proposal.get('id', 'unknown')}")

            all_proposals.extend(valid_proposals)
            page_count += 1

            print(f"      =� Page {page_count}: +{len(valid_proposals)} proposals (total: {len(all_proposals)})")

            # Better cursor handling
            new_cursor = page_info.get('lastCursor')
            if not new_cursor:
                print(f"      � No more cursor, stopping pagination")
                break
                
            if new_cursor == after_cursor:
                print(f"      � Cursor didn't advance, stopping pagination")
                break

            after_cursor = new_cursor

        self.save_checkpoint(proposals=all_proposals)
        print(f"� Found {len(all_proposals)} total proposals")
        
        return all_proposals

    def validate_proposal_data_improved(self, proposal: Dict) -> bool:
        """Validate proposal data - improved to handle missing titles"""
        try:
            # Must have an ID
            if not proposal.get('id'):
                return False
            
            # Check if we have basic proposal structure
            has_basic_structure = (
                proposal.get('status') is not None or
                proposal.get('onchainId') is not None or
                proposal.get('metadata', {}).get('title') or
                proposal.get('metadata', {}).get('description')
            )
            
            if not has_basic_structure:
                # If no title/description, at least ensure we have some identifying info
                if not proposal.get('proposer', {}).get('address'):
                    return False
            
            return True
        except:
            return False

    def get_votes_for_proposal_high_volume(self, proposal_id: int, expected_total_votes: Optional[int] = None) -> List[Dict]:
        """Get ALL votes for a proposal with optimized pagination"""

        # Check if we have cached votes for this proposal
        votes_cache = self.checkpoint_data.get('votes_cache', {})
        if not self.force_refresh_votes and str(proposal_id) in votes_cache:
            cached_votes = votes_cache[str(proposal_id)]
            print(f"      =� Using cached votes: {len(cached_votes)} votes")
            return cached_votes

        query = """
        query GetVotes($input: VotesInput!) {
            votes(input: $input) {
                nodes {
                    ... on OnchainVote {
                        id
                        type
                        amount
                        reason
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
        # Try resuming from vote cursor checkpoint
        after_cursor = self.load_vote_cursor(proposal_id)
        page_count = 0
        consecutive_failures = 0
        start_time = time.time()
        last_save_time = start_time
        backoff_seconds = 2

        print(f"      =� FAST vote extraction (batch: {self.vote_batch_size:,})")

        while page_count < self.max_vote_pages and consecutive_failures < 50:
            page_input = {"limit": self.vote_batch_size}
            if after_cursor:
                page_input["afterCursor"] = after_cursor

            variables = {
                "input": {
                    "filters": {"proposalId": proposal_id},
                    "page": page_input,
                    "sort": {"sortBy": "id", "isDescending": False}
                }
            }

            # Robust request with persistent retry on connectivity issues
            result = self.api.make_request_optimized(query, variables)

            if not result or not result.get('data'):
                consecutive_failures += 1
                # Save progress so far (votes + cursor) before backing off
                try:
                    votes_cache[str(proposal_id)] = all_votes
                    self.save_checkpoint(votes_cache=votes_cache)
                    self.save_vote_cursor(proposal_id, after_cursor)
                except Exception:
                    pass

                # Adaptive backoff on persistent failures
                if consecutive_failures % 5 == 0:
                    self.vote_batch_size = max(100, self.vote_batch_size // 2)
                    print(f"      � Failures: {consecutive_failures}. Reducing batch to {self.vote_batch_size}")
                sleep_for = min(self.api.max_delay, backoff_seconds)
                print(f"      � No data/connection issue. Waiting {sleep_for:.1f}s then retrying...")
                time.sleep(sleep_for)
                backoff_seconds = min(self.api.max_delay, max(2, backoff_seconds * 1.7))
                continue

            votes_data = result.get('data', {}).get('votes', {})
            votes_batch = votes_data.get('nodes', [])
            page_info = votes_data.get('pageInfo', {})

            if not votes_batch:
                new_cursor = page_info.get('lastCursor')
                if new_cursor and new_cursor != after_cursor:
                    after_cursor = new_cursor
                    consecutive_failures += 1
                    print(f"      =� Empty page but have cursor, continuing...")
                    # Persist the cursor so we can resume exactly here
                    self.save_vote_cursor(proposal_id, after_cursor)
                    continue
                else:
                    print(f"      � No more votes available")
                    break

            consecutive_failures = 0

            # Validate votes
            valid_votes = [vote for vote in votes_batch if self.validate_vote_data(vote)]
            all_votes.extend(valid_votes)
            page_count += 1

            # Persist progress periodically (time-based)
            if time.time() - last_save_time > 60:
                try:
                    votes_cache[str(proposal_id)] = all_votes
                    self.save_checkpoint(votes_cache=votes_cache)
                    self.save_vote_cursor(proposal_id, after_cursor)
                except Exception:
                    pass
                last_save_time = time.time()

            # Progress updates for large vote sets
            if page_count % 10 == 0 or len(all_votes) > 1000:
                elapsed = time.time() - start_time
                votes_per_second = len(all_votes) / elapsed if elapsed > 0 else 0
                print(f"      =� Page {page_count}: {len(all_votes):,} votes ({votes_per_second:.0f}/sec)")

            # Check pagination
            new_cursor = page_info.get('lastCursor')
            expected_total = expected_total_votes
            if not new_cursor or new_cursor == after_cursor:
                if expected_total is None or len(all_votes) >= expected_total:
                    print(f"      � Reached end (no cursor or same cursor)")
                    break
                else:
                    consecutive_failures += 1
                    print(f"      � Cursor stalled with {len(all_votes):,}/{expected_total} votes. Retrying...")
                    time.sleep(1.0)
                    continue

            after_cursor = new_cursor
            # Update cursor checkpoint after each successful page
            self.save_vote_cursor(proposal_id, after_cursor)

        # Cache the votes
        if all_votes:
            votes_cache[str(proposal_id)] = all_votes
            self.save_checkpoint(votes_cache=votes_cache)
            # Clear proposal-specific vote cursor once finished
            try:
                if os.path.exists(self.vote_checkpoint_file):
                    with open(self.vote_checkpoint_file, 'r') as f:
                        data = json.load(f) or {}
                    if str(proposal_id) in data:
                        del data[str(proposal_id)]
                        with open(self.vote_checkpoint_file, 'w') as f:
                            json.dump(data, f)
            except Exception:
                pass

        elapsed = time.time() - start_time
        print(f"      � TOTAL: {len(all_votes):,} votes in {elapsed:.1f}s ({page_count} pages)")

        return all_votes

    def normalize_vote_type(self, vote: Dict) -> str:
        """Enhanced vote type normalization"""
        vote_type = vote.get('type', '').strip()
        if vote_type:
            type_normalized = vote_type.lower()

            type_mappings = {
                'for': 'For',
                'yes': 'For',
                'support': 'For',
                'approve': 'For',
                'in_favor': 'For',
                'infavor': 'For',
                'aye': 'For',
                '1': 'For',
                'true': 'For',

                'against': 'Against',
                'no': 'Against',
                'oppose': 'Against',
                'nay': 'Against',
                '0': 'Against',
                'false': 'Against',

                'abstain': 'Abstain',
                'abstention': 'Abstain',
                'present': 'Abstain',
                '2': 'Abstain'
            }

            if type_normalized in type_mappings:
                return type_mappings[type_normalized]

        # Amount-based inference
        amount = vote.get('amount', 0)
        try:
            amount = float(amount) if amount else 0
        except:
            amount = 0

        if amount > 0:
            return 'For'

        # Reason analysis
        reason = vote.get('reason', '').lower()
        if reason:
            if any(word in reason for word in ['support', 'favor', 'yes', 'approve', 'agree', 'for']):
                return 'For'
            elif any(word in reason for word in ['against', 'oppose', 'no', 'disagree', 'reject']):
                return 'Against'
            elif any(word in reason for word in ['abstain', 'neutral', 'present']):
                return 'Abstain'

        # If we have a transaction hash, they definitely voted
        if vote.get('txHash'):
            return 'Voted'

        return 'Unknown'

    def validate_vote_data(self, vote: Dict) -> bool:
        """Validate vote data"""
        try:
            voter = vote.get('voter')
            if not voter or not voter.get('address'):
                return False

            has_id = vote.get('id') is not None
            has_type = vote.get('type') is not None
            has_onchain_signal = vote.get('txHash') is not None or vote.get('block', {}).get('timestamp') is not None
            has_offchain_signal = vote.get('reason') is not None or vote.get('createdAt') is not None or vote.get('platform') is not None

            return has_id or has_type or has_onchain_signal or has_offchain_signal
        except:
            return False

    def create_voting_matrix_fast(self, proposals: List[Dict], delegates: List[Dict], org_id: int) -> None:
        """Create voting matrix optimized for maximum speed"""

        print(f"=� FAST VOTING MATRIX CREATION")
        print(f"=� {len(proposals)} proposals � {len(delegates):,} delegates")
        print(f"=� Processing efficiently with minimal file I/O")

        # Statistics tracking
        total_votes_found = 0
        proposals_with_votes = 0
        all_voting_data = []  # Store all data in memory for final processing

        # Process proposals one by one for maximum efficiency
        for i, proposal in enumerate(proposals):
            proposal_id = proposal['id']
            proposal_title = self.safe_get_nested(proposal, 'metadata', 'title', default=f'Proposal {proposal_id}')
            short_title = proposal_title[:50] + "..." if len(proposal_title) > 50 else proposal_title

            print(f"   =� {i + 1}/{len(proposals)}: {short_title}")

            # Determine expected total voters from Tally's proposal stats, if available
            expected_total_votes = None
            try:
                stats = proposal.get('voteStats') or []
                expected_total_votes = sum(int(s.get('votersCount') or 0) for s in stats if isinstance(s, dict))
            except Exception:
                expected_total_votes = None

            # Get ALL votes for this proposal with high-volume method
            votes = self.get_votes_for_proposal_high_volume(proposal_id, expected_total_votes=expected_total_votes)

            if votes:
                total_votes_found += len(votes)
                proposals_with_votes += 1

            # Report expected vs actual when we know expected totals
            if expected_total_votes is not None:
                print(f"      � {len(votes):,}/{expected_total_votes:,} votes extracted")
            else:
                print(f"      � {len(votes):,} votes extracted")

            # Create comprehensive vote mapping
            vote_map = {}
            vote_stats = {'For': 0, 'Against': 0, 'Abstain': 0, 'Other': 0}

            for vote in votes:
                voter_info = vote.get('voter', {})
                if not voter_info or not voter_info.get('address'):
                    continue

                voter_address = voter_info['address'].lower()
                normalized_vote = self.normalize_vote_type(vote)

                # Count vote types
                if normalized_vote in vote_stats:
                    vote_stats[normalized_vote] += 1
                else:
                    vote_stats['Other'] += 1

                vote_map[voter_address] = {
                    'vote': normalized_vote,
                    'amount': str(vote.get('amount', '0') or '0'),
                    'raw_type': vote.get('type'),
                    'reason': self.safe_get_nested(vote, 'reason', default=''),
                    'block_timestamp': self.safe_get_nested(vote, 'block', 'timestamp', default=self.safe_get_nested(vote, 'createdAt', default='')),
                    'block_number': self.safe_get_nested(vote, 'block', 'number', default=''),
                    'tx_hash': self.safe_get_nested(vote, 'txHash', default='')
                }

            # Show vote distribution
            if votes:
                print(f"      =� Vote breakdown: For:{vote_stats['For']} Against:{vote_stats['Against']} Abstain:{vote_stats['Abstain']} Other:{vote_stats['Other']}")

            # Process ALL delegates at once for this proposal (much faster)
            proposal_data = self.process_all_delegates_for_proposal(delegates, proposal, votes, vote_map)
            all_voting_data.extend(proposal_data)

            # Show participation for this proposal
            delegates_who_voted = sum(1 for d in proposal_data if d.get('participated', False))
            participation_rate = (delegates_who_voted / len(delegates)) * 100 if delegates else 0
            print(f"      =� Delegate participation: {delegates_who_voted:,}/{len(delegates):,} ({participation_rate:.2f}%)")

            # Clear vote data to free memory
            del votes
            del vote_map
            del proposal_data

        # Save all data to a single optimized file
        print(f"\n=� Saving optimized results...")
        self.save_optimized_results(all_voting_data, total_votes_found, proposals_with_votes)

        # Final statistics
        print(f"\n<� FAST VOTING MATRIX COMPLETE!")
        print(f"   =� Total records processed: {len(all_voting_data):,}")
        print(f"   =� Total votes extracted: {total_votes_found:,}")
        print(f"   =� Proposals with votes: {proposals_with_votes}/{len(proposals)}")

        if total_votes_found > 0:
            avg_votes_per_proposal = total_votes_found / proposals_with_votes
            print(f"   =� Average votes per active proposal: {avg_votes_per_proposal:.0f}")

        print(f"=� All data processed efficiently with minimal file I/O")

    def process_all_delegates_for_proposal(self, delegates: List[Dict], proposal: Dict, votes: List[Dict], vote_map: Dict) -> List[Dict]:
        """Process all delegates for a specific proposal efficiently"""
        proposal_data = []
        
        for delegate in delegates:
            account_info = delegate.get('account', {})
            delegate_address = account_info.get('address', '').lower()

            if not delegate_address:
                continue

            delegate_name = account_info.get('name', '') or f"{account_info.get('address', '')[:10]}..."
            delegate_ens = account_info.get('ens', '')

            try:
                votes_count = float(delegate.get('votesCount', 0))
                delegators_count = int(delegate.get('delegatorsCount', 0))
            except:
                votes_count = 0
                delegators_count = 0

            statement_info = delegate.get('statement', {})
            has_statement = bool(statement_info.get('statement', '').strip())
            seeking_delegation = statement_info.get('isSeekingDelegation', False)

            # Base record data
            record_data = {
                'delegate_address': account_info.get('address', ''),
                'delegate_name': delegate_name,
                'delegate_ens': delegate_ens,
                'delegate_votes_count': votes_count,
                'delegate_delegators_count': delegators_count,
                'has_statement': has_statement,
                'seeking_delegation': seeking_delegation,
                'proposal_id': proposal['id'],
                'proposal_onchain_id': self.safe_get_nested(proposal, 'onchainId', default=''),
                'proposal_title': proposal.get('metadata', {}).get('title', f'Proposal {proposal["id"]}'),
                'proposal_status': proposal.get('status', 'unknown'),
                'proposal_start_timestamp': self.safe_get_nested(proposal, 'start', 'timestamp', default=''),
                'proposal_end_timestamp': self.safe_get_nested(proposal, 'end', 'timestamp', default=''),
                'proposal_start_block': self.safe_get_nested(proposal, 'start', 'number', default=''),
                'proposal_end_block': self.safe_get_nested(proposal, 'end', 'number', default=''),
                'is_active_delegate': votes_count > 0,
                'dao_name': self.dao_config.name
            }

            # Add voting information
            if delegate_address in vote_map:
                vote_info = vote_map[delegate_address]
                record_data.update({
                    'vote': vote_info['vote'],
                    'voting_amount': vote_info['amount'],
                    'vote_type_raw': vote_info['raw_type'],
                    'vote_reason': vote_info['reason'],
                    'vote_timestamp': vote_info['block_timestamp'],
                    'vote_block_number': vote_info['block_number'],
                    'vote_tx_hash': vote_info['tx_hash'],
                    'participated': True
                })
            else:
                record_data.update({
                    'vote': 'Did Not Vote',
                    'voting_amount': '0',
                    'vote_type_raw': '',
                    'vote_reason': '',
                    'vote_timestamp': '',
                    'vote_block_number': '',
                    'vote_tx_hash': '',
                    'participated': False
                })

            proposal_data.append(record_data)

        return proposal_data

    def save_optimized_results(self, all_voting_data: List[Dict], total_votes_found: int, proposals_with_votes: int) -> Dict[str, str]:
        """Save optimized results efficiently"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        dao_slug = self.dao_config.slug.replace(' ', '_').lower()
        files_saved = {}

        print(f"=� SAVING OPTIMIZED RESULTS...")

        try:
                # Convert to DataFrame for analysis
                voting_df = pd.DataFrame(all_voting_data)

                # 1. Complete voting matrix
                complete_filename = f'{dao_slug}_FAST_voting_matrix_{timestamp}.csv'
                voting_df.to_csv(complete_filename, index=False)
                files_saved['complete_matrix'] = complete_filename
                print(f"� Complete voting matrix: {complete_filename} ({len(voting_df):,} records)")

                # 2. Delegate summary
                delegate_summary = voting_df.groupby(['delegate_address', 'delegate_name', 'delegate_ens']).agg({
                        'participated': ['sum', 'count'],
                        'vote': lambda x: (x != 'Did Not Vote').sum(),
                        'delegate_votes_count': 'first',
                        'delegate_delegators_count': 'first',
                        'has_statement': 'first',
                        'seeking_delegation': 'first',
                        'is_active_delegate': 'first'
                }).round(2)

                delegate_summary.columns = [
                        'votes_cast', 'total_proposals', 'actual_votes',
                        'delegate_power', 'delegators_count', 'has_statement',
                        'seeking_delegation', 'is_active_delegate'
                ]
                delegate_summary['participation_rate'] = (
                        delegate_summary['votes_cast'] / delegate_summary['total_proposals'] * 100
                ).round(2)

                # Add vote type breakdown
                vote_breakdown = voting_df[voting_df['vote'] != 'Did Not Vote'].groupby(['delegate_address', 'vote']).size().unstack(fill_value=0)
                for col in ['For', 'Against', 'Abstain', 'Voted', 'Unknown']:
                        if col in vote_breakdown.columns:
                                delegate_summary[f'votes_{col.lower()}'] = vote_breakdown[col]
                        else:
                                delegate_summary[f'votes_{col.lower()}'] = 0

                delegate_summary = delegate_summary.sort_values('delegate_power', ascending=False).reset_index()

                summary_filename = f'{dao_slug}_FAST_delegate_summary_{timestamp}.csv'
                delegate_summary.to_csv(summary_filename, index=False)
                files_saved['delegate_summary'] = summary_filename
                print(f"� Delegate summary: {summary_filename} ({len(delegate_summary):,} delegates)")

                # 3. Proposal analysis
                proposal_analysis = voting_df.groupby(['proposal_id', 'proposal_title', 'proposal_status']).agg({
                        'participated': 'sum',
                        'vote': 'count',
                        'delegate_address': 'nunique',
                        'proposal_start_timestamp': 'first',
                        'proposal_end_timestamp': 'first'
                })

                proposal_analysis.columns = ['actual_votes', 'total_delegates', 'unique_delegates', 'start_time', 'end_time']
                proposal_analysis['participation_rate'] = (
                        proposal_analysis['actual_votes'] / proposal_analysis['unique_delegates'] * 100
                ).round(2)

                # Add vote type breakdown per proposal
                prop_vote_breakdown = voting_df[voting_df['vote'] != 'Did Not Vote'].groupby(['proposal_id', 'vote']).size().unstack(fill_value=0)
                for col in ['For', 'Against', 'Abstain', 'Voted', 'Unknown']:
                        if col in prop_vote_breakdown.columns:
                                proposal_analysis[f'votes_{col.lower()}'] = prop_vote_breakdown[col]
                        else:
                                proposal_analysis[f'votes_{col.lower()}'] = 0

                proposal_analysis = proposal_analysis.sort_values('actual_votes', ascending=False).reset_index()

                proposal_filename = f'{dao_slug}_FAST_proposal_analysis_{timestamp}.csv'
                proposal_analysis.to_csv(proposal_filename, index=False)
                files_saved['proposal_analysis'] = proposal_filename
                print(f"� Proposal analysis: {proposal_filename} ({len(proposal_analysis)} proposals)")

                # 4. Performance report
                actual_votes = voting_df[voting_df['vote'] != 'Did Not Vote']
                total_votes_extracted = len(actual_votes)
                unique_voters = actual_votes['delegate_address'].nunique() if len(actual_votes) > 0 else 0
                proposals_with_votes = actual_votes['proposal_id'].nunique() if len(actual_votes) > 0 else 0

                performance_report = {
                        'extraction_metadata': {
                                'dao_name': self.dao_config.name,
                                'dao_slug': self.dao_config.slug,
                                'extraction_mode': 'FAST_OPTIMIZED',
                                'analysis_timestamp': timestamp,
                                'total_api_requests': self.api.request_count,
                                'session_time_hours': (time.time() - self.session_start_time) / 3600,
                                'vote_batch_size': self.vote_batch_size,
                                'max_vote_pages': self.max_vote_pages
                        },
                        'data_statistics': {
                                'total_records': int(len(voting_df)),
                                'unique_delegates': int(voting_df['delegate_address'].nunique()),
                                'unique_proposals': int(voting_df['proposal_id'].nunique()),
                                'total_votes_extracted': int(total_votes_extracted),
                                'unique_voters': int(unique_voters),
                                'proposals_with_votes': int(proposals_with_votes),
                                'overall_participation_rate': float(voting_df['participated'].sum() / len(voting_df) * 100),
                                'active_delegates_count': int((voting_df['is_active_delegate'] == True).sum()),
                        },
                        'performance_improvements': {
                                'vote_extraction_method': 'Fast optimized pagination with large batches',
                                'batch_size_votes': self.vote_batch_size,
                                'max_pages_per_proposal': self.max_vote_pages,
                                'caching_enabled': True,
                                'optimized_rate_limiting': True,
                                'minimal_file_io': 'Only save final results, no intermediate chunks',
                                'memory_efficient': 'Process all delegates at once per proposal'
                        }
                }

                performance_filename = f'{dao_slug}_FAST_performance_report_{timestamp}.json'
                with open(performance_filename, 'w') as f:
                        json.dump(performance_report, f, indent=2, default=str)
                files_saved['performance_report'] = performance_filename
                print(f"� Performance report: {performance_filename}")

        except Exception as e:
                print(f"L Error saving results: {e}")
                # Memory-safe fallback: stream CSV and compute summaries without pandas DataFrame
                try:
                        import csv
                        from collections import defaultdict

                        # 1) Stream complete voting matrix to CSV (no large DataFrame)
                        complete_filename = f'{dao_slug}_FAST_voting_matrix_{timestamp}.csv'
                        fieldnames = list(all_voting_data[0].keys()) if all_voting_data else []
                        with open(complete_filename, 'w', newline='', encoding='utf-8') as f:
                                writer = csv.DictWriter(f, fieldnames=fieldnames)
                                if fieldnames:
                                        writer.writeheader()
                                        for row in all_voting_data:
                                                writer.writerow(row)
                        files_saved['complete_matrix'] = complete_filename
                        print(f"� Complete voting matrix (streamed): {complete_filename} ({len(all_voting_data):,} records)")

                        # Prepare aggregations
                        # Delegate summary aggregations
                        delegate_key_fields = ('delegate_address', 'delegate_name', 'delegate_ens')
                        delegate_agg = {}
                        # Proposal analysis aggregations
                        proposal_agg = {}

                        # Performance counters
                        total_records = 0
                        unique_delegates_set = set()
                        unique_proposals_set = set()
                        total_votes_extracted = 0
                        unique_voters_set = set()
                        proposals_with_votes_set = set()
                        participated_sum = 0
                        active_delegates_count = 0

                        for row in all_voting_data:
                                total_records += 1
                                addr = row.get('delegate_address')
                                vote_val = row.get('vote')
                                prop_id = row.get('proposal_id')
                                unique_delegates_set.add(addr)
                                unique_proposals_set.add(prop_id)
                                if row.get('participated'):
                                        participated_sum += 1
                                if row.get('is_active_delegate') is True:
                                        active_delegates_count += 1

                                # Count actual votes (exclude "Did Not Vote")
                                if vote_val and vote_val != 'Did Not Vote':
                                        total_votes_extracted += 1
                                        unique_voters_set.add(addr)
                                        proposals_with_votes_set.add(prop_id)

                                # Delegate summary rollup
                                dkey = (row.get('delegate_address'), row.get('delegate_name'), row.get('delegate_ens'))
                                if dkey not in delegate_agg:
                                        delegate_agg[dkey] = {
                                                'votes_cast': 0,
                                                'total_proposals': 0,
                                                'actual_votes': 0,
                                                'vote_for': 0,
                                                'vote_against': 0,
                                                'vote_abstain': 0,
                                                'vote_voted': 0,
                                                'vote_unknown': 0,
                                                'delegate_power': row.get('delegate_votes_count'),
                                                'delegators_count': row.get('delegate_delegators_count'),
                                                'has_statement': row.get('has_statement'),
                                                'seeking_delegation': row.get('seeking_delegation'),
                                                'is_active_delegate': row.get('is_active_delegate'),
                                        }
                                aggd = delegate_agg[dkey]
                                aggd['total_proposals'] += 1
                                if row.get('participated'):
                                        aggd['votes_cast'] += 1
                                if vote_val and vote_val != 'Did Not Vote':
                                        aggd['actual_votes'] += 1
                                        # vote breakdown
                                        vnorm = str(vote_val)
                                        if vnorm == 'For':
                                                aggd['vote_for'] += 1
                                        elif vnorm == 'Against':
                                                aggd['vote_against'] += 1
                                        elif vnorm == 'Abstain':
                                                aggd['vote_abstain'] += 1
                                        elif vnorm == 'Voted':
                                                aggd['vote_voted'] += 1
                                        else:
                                                aggd['vote_unknown'] += 1

                                # Proposal analysis rollup
                                if prop_id not in proposal_agg:
                                        proposal_agg[prop_id] = {
                                                'proposal_title': row.get('proposal_title'),
                                                'proposal_status': row.get('proposal_status', 'unknown'),
                                                'start_time': row.get('proposal_start_timestamp'),
                                                'end_time': row.get('proposal_end_timestamp'),
                                                'actual_votes': 0,
                                                'total_delegates': 0,
                                                'unique_delegates_set': set(),
                                                'vote_for': 0,
                                                'vote_against': 0,
                                                'vote_abstain': 0,
                                                'vote_voted': 0,
                                                'vote_unknown': 0,
                                        }
                                aggp = proposal_agg[prop_id]
                                aggp['total_delegates'] += 1
                                aggp['unique_delegates_set'].add(addr)
                                if vote_val and vote_val != 'Did Not Vote':
                                        aggp['actual_votes'] += 1
                                        vnorm = str(vote_val)
                                        if vnorm == 'For':
                                                aggp['vote_for'] += 1
                                        elif vnorm == 'Against':
                                                aggp['vote_against'] += 1
                                        elif vnorm == 'Abstain':
                                                aggp['vote_abstain'] += 1
                                        elif vnorm == 'Voted':
                                                aggp['vote_voted'] += 1
                                        else:
                                                aggp['vote_unknown'] += 1

                        # 2) Write delegate summary CSV
                        summary_filename = f'{dao_slug}_FAST_delegate_summary_{timestamp}.csv'
                        delegate_headers = [
                                'delegate_address','delegate_name','delegate_ens',
                                'votes_cast','total_proposals','actual_votes',
                                'delegate_power','delegators_count','has_statement',
                                'seeking_delegation','is_active_delegate',
                                'participation_rate','votes_for','votes_against','votes_abstain','votes_voted','votes_unknown'
                        ]
                        with open(summary_filename, 'w', newline='', encoding='utf-8') as f:
                                writer = csv.writer(f)
                                writer.writerow(delegate_headers)
                                for (addr, name, ens), aggd in sorted(delegate_agg.items(), key=lambda x: (x[1].get('delegate_power') or 0), reverse=True):
                                        total_props = aggd['total_proposals'] or 1
                                        part_rate = round((aggd['votes_cast'] / total_props) * 100, 2)
                                        writer.writerow([
                                                addr, name, ens,
                                                aggd['votes_cast'], aggd['total_proposals'], aggd['actual_votes'],
                                                aggd['delegate_power'], aggd['delegators_count'], aggd['has_statement'],
                                                aggd['seeking_delegation'], aggd['is_active_delegate'],
                                                part_rate,
                                                aggd['vote_for'], aggd['vote_against'], aggd['vote_abstain'], aggd['vote_voted'], aggd['vote_unknown']
                                        ])
                        files_saved['delegate_summary'] = summary_filename
                        print(f"� Delegate summary (streamed): {summary_filename} ({len(delegate_agg):,} delegates)")

                        # 3) Write proposal analysis CSV
                        proposal_filename = f'{dao_slug}_FAST_proposal_analysis_{timestamp}.csv'
                        proposal_headers = [
                                'proposal_id','proposal_title','proposal_status','actual_votes','total_delegates','unique_delegates','start_time','end_time',
                                'votes_for','votes_against','votes_abstain','votes_voted','votes_unknown','participation_rate'
                        ]
                        with open(proposal_filename, 'w', newline='', encoding='utf-8') as f:
                                writer = csv.writer(f)
                                writer.writerow(proposal_headers)
                                for pid, aggp in proposal_agg.items():
                                        unique_del = len(aggp['unique_delegates_set']) or 1
                                        part_rate = round((aggp['actual_votes'] / unique_del) * 100, 2)
                                        writer.writerow([
                                                pid, aggp['proposal_title'], aggp['proposal_status'], aggp['actual_votes'], aggp['total_delegates'], unique_del,
                                                aggp['start_time'], aggp['end_time'],
                                                aggp['vote_for'], aggp['vote_against'], aggp['vote_abstain'], aggp['vote_voted'], aggp['vote_unknown'],
                                                part_rate
                                        ])
                        files_saved['proposal_analysis'] = proposal_filename
                        print(f"� Proposal analysis (streamed): {proposal_filename} ({len(proposal_agg)} proposals)")

                        # 4) Performance report JSON (computed without pandas)
                        performance_report = {
                                'extraction_metadata': {
                                        'dao_name': self.dao_config.name,
                                        'dao_slug': self.dao_config.slug,
                                        'extraction_mode': 'FAST_OPTIMIZED_STREAM_FALLBACK',
                                        'analysis_timestamp': timestamp,
                                        'total_api_requests': self.api.request_count,
                                        'session_time_hours': (time.time() - self.session_start_time) / 3600,
                                        'vote_batch_size': self.vote_batch_size,
                                        'max_vote_pages': self.max_vote_pages
                                },
                                'data_statistics': {
                                        'total_records': int(total_records),
                                        'unique_delegates': int(len(unique_delegates_set)),
                                        'unique_proposals': int(len(unique_proposals_set)),
                                        'total_votes_extracted': int(total_votes_extracted),
                                        'unique_voters': int(len(unique_voters_set)),
                                        'proposals_with_votes': int(len(proposals_with_votes_set)),
                                        'overall_participation_rate': float((participated_sum / total_records) * 100) if total_records else 0.0,
                                        'active_delegates_count': int(active_delegates_count),
                                },
                                'performance_improvements': {
                                        'vote_extraction_method': 'Fast optimized pagination with large batches',
                                        'batch_size_votes': self.vote_batch_size,
                                        'max_pages_per_proposal': self.max_vote_pages,
                                        'caching_enabled': True,
                                        'optimized_rate_limiting': True,
                                        'minimal_file_io': 'Only save final results; streamed fallback enabled',
                                        'memory_efficient': 'No giant DataFrame allocations during save'
                                }
                        }
                        performance_filename = f'{dao_slug}_FAST_performance_report_{timestamp}.json'
                        with open(performance_filename, 'w') as f:
                                json.dump(performance_report, f, indent=2, default=str)
                        files_saved['performance_report'] = performance_filename
                        print(f"� Performance report (fallback): {performance_filename}")

                except Exception as e2:
                        print(f"L Fallback save failed: {e2}")
                        # Minimal emergency save: write raw rows as CSV
                        try:
                                emergency_filename = f'{dao_slug}_EMERGENCY_fast_{timestamp}.csv'
                                with open(emergency_filename, 'w', newline='', encoding='utf-8') as f:
                                        import csv
                                        fieldnames = list(all_voting_data[0].keys()) if all_voting_data else []
                                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                                        if fieldnames:
                                                writer.writeheader()
                                                for row in all_voting_data:
                                                        writer.writerow(row)
                                files_saved['emergency_save'] = emergency_filename
                                print(f"=� Emergency save: {emergency_filename}")
                        except Exception as e3:
                                print(f"L Emergency save failed: {e3}")

        return files_saved

    def cleanup_checkpoint_files(self):
        """Clean up checkpoint files"""
        try:
            files_to_clean = [
                self.checkpoint_file,
                self.vote_checkpoint_file
            ]

            for file_path in files_to_clean:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f">� Cleaned up: {file_path}")

        except Exception as e:
            print(f"� Cleanup error: {e}")

def main():
    """Fast optimized main function"""
    print("=� FAST OPTIMIZED DAO DATA FETCHER")
    print("=" * 60)
    print("� OPTIMIZED for maximum speed and efficiency")
    print("=� Smart rate limiting with adaptive delays")
    print("=� Minimal file I/O - only save final results")
    print("=� Process all delegates at once per proposal")
    print("=� Designed for maximum speed")

    # DAO selection
    print(f"\n=' DAO SELECTION:")
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
            print("L Invalid choice, using Compound DAO")
            dao_config = DAOConfig.get_compound_config()
    except:
        print("L Error in selection, using Compound DAO")
        dao_config = DAOConfig.get_compound_config()

    # Confirmation
    print(f"\n� FAST MODE SETTINGS:")
    print(f"   <� DAO: {dao_config.name}")
    print(f"   =� Vote batch size: 5,000 votes per request")
    print(f"   =� Max pages per proposal: 100,000 (virtually unlimited)")
    print(f"   � Optimized rate limiting (1.05s between requests)")
    print(f"   =� Vote caching enabled")
    print(f"   =� Minimal file I/O - only save final results")

    try:
        use_force_refresh = input("Force refresh votes (ignore cached)? (y/N): ").strip().lower() == 'y'
    except Exception:
        use_force_refresh = False

    try:
        confirm = input(f"\n=� Start fast extraction? (y/N): ").strip().lower()
        if confirm != 'y':
            print("=K Cancelled.")
            return
    except:
        print("=K Cancelled.")
        return

    # Initialize fast fetcher
    fetcher = FastDAOFetcher(dao_config=dao_config, force_refresh_votes=use_force_refresh)
    total_start_time = time.time()

    try:
        # Step 1: Find organization
        print(f"\n=� Step 1: Finding organization...")
        result = fetcher.get_organization_by_slug(dao_config.slug)

        if not result.get('data', {}).get('organization'):
            print(f"L Could not find {dao_config.name}")
            return

        org = result['data']['organization']
        org_id = org['id']
        print(f"� Found: {org['name']} (ID: {org_id})")
        print(f"   =� Proposals: {org.get('proposalsCount', 'N/A')}")
        print(f"   =� Delegates: {org.get('delegatesCount', 'N/A')}")
        print(f"   =� Total votes: {org.get('delegatesVotesCount', 'N/A')}")

        # Step 2: Get all delegates
        print(f"\n=� Step 2: Fetching delegates...")
        delegates = fetcher.get_all_delegates_optimized(org_id)

        if not delegates:
            print(f"L No delegates found")
            return

        print(f"<� Retrieved {len(delegates):,} delegates")

        # Step 3: Get all proposals
        print(f"\n=� Step 3: Fetching proposals...")
        proposals = fetcher.get_all_proposals_optimized(org_id)

        if not proposals:
            print(f"L No proposals found")
            return

        print(f"<� Retrieved {len(proposals)} proposals")

        # Step 4: Create fast voting matrix
        print(f"\n=� Step 4: Fast voting matrix creation...")
        fetcher.create_voting_matrix_fast(proposals, delegates, org_id)

        # Step 5: Final analysis and summary
        total_time_hours = (time.time() - total_start_time) / 3600

        print(f"\n<� FAST EXTRACTION COMPLETE!")
        print("=" * 60)

        print(f"<� DAO: {dao_config.name}")
        print(f"� Total time: {total_time_hours:.1f} hours")
        print(f"=� API requests: {fetcher.api.request_count:,}")
        print(f"=� Processing mode: Fast optimized processing")
        print(f"=� Minimal file I/O - maximum speed")

        # Performance metrics
        print(f"\n=� PERFORMANCE METRICS:")
        api_stats = fetcher.api.get_stats()
        print(f"   =� API requests made: {api_stats['total_requests']:,}")
        print(f"   � Success rate: {api_stats['success_rate']}")
        print(f"   � Processing time: {total_time_hours:.1f} hours")
        print(f"   =� Speed optimization: EXCELLENT!")
        print(f"   =� Rate limiting: {api_stats['current_delay']} between requests")
        print(f"   =� API efficiency: {api_stats['efficiency']}")

        # Cleanup
        print(f"\n>� Cleaning up...")
        fetcher.cleanup_checkpoint_files()

        print(f"\n<� SUCCESS! Fast {dao_config.name} extraction completed!")
        print(f"� All data processed efficiently with minimal file I/O")
        print(f"=� Comprehensive analysis files ready")
        print(f"=� Maximum speed achieved!")

    except KeyboardInterrupt:
        print(f"\n� Interrupted by user")
        print(f"=� Progress saved - restart to resume")

    except Exception as e:
        print(f"\nL Unexpected error: {e}")
        # Emergency save
        try:
            # Check if we have any data to save
            if 'delegates' in locals() and len(delegates) > 0:
                emergency_file = f"{dao_config.slug}_emergency_delegates.csv"
                pd.DataFrame(delegates).to_csv(emergency_file, index=False)
                print(f"=� Emergency save: {emergency_file}")
        except:
            print(f"L Emergency save failed")

if __name__ == "__main__":
    main()
