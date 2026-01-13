import requests
import json
import time
import csv
import pandas as pd
from typing import List, Dict, Optional
import os
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aiohttp


class EnhancedTallyAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.endpoint = "https://api.tally.xyz/query"
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        self.rate_limit_count = 0

        # Optimized rate limiting
        self.min_delay = 1.05  # Slightly above 1 req/sec limit
        self.max_delay = 5.0   # Maximum backoff delay
        self.last_request_time = 0
        self.rate_limit_lock = threading.Lock()

        # Persistent session for connection reuse
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Api-Key': self.api_key,
            'Connection': 'keep-alive',
            'User-Agent': 'Enhanced-DAO-Analyzer/1.0'
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
                    timeout=30,  # Increased timeout
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

                    self.success_count += 1
                    return data

                elif response.status_code == 429:
                    # Rate limited - implement exponential backoff
                    self.rate_limit_count += 1
                    backoff_time = min(base_delay * (2 ** attempt), self.max_delay)
                    self.min_delay = min(self.min_delay * 1.2, 2.0)  # Adaptive delay increase

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


class UniversalDAOAnalyzer:
    def __init__(self, api_key: str):
        self.api = EnhancedTallyAPI(api_key)

        # DAO info will be set when selected
        self.dao_slug = ""
        self.dao_name = ""
        self.base_proposal_url = ""
        self.base_delegate_url = ""

    def set_dao_info(self, dao_slug: str, dao_name: str = ""):
        """Set DAO information for URLs and display"""
        self.dao_slug = dao_slug.lower()
        self.dao_name = dao_name or dao_slug.title()
        self.base_proposal_url = f"https://www.tally.xyz/gov/{self.dao_slug}/proposal/"
        self.base_delegate_url = f"https://www.tally.xyz/gov/{self.dao_slug}/delegate/"

    def validate_api_key(self) -> bool:
        """Test API key validity with minimal query"""
        # Validate API key

        query = """
        query TestConnection {
          organization(input: {slug: "arbitrum"}) {
            id
            name
          }
        }
        """

        data = self.api.make_request_optimized(query)
        if data and data.get("data"):
            print("API key validated successfully")
            return True
        else:
            print("API key validation failed")
            return False

    def search_daos_fast(self, search_term: str = "") -> List[Dict]:
        """Fast DAO search with parallel requests"""
        print("Searching for DAOs...")

        # Known popular DAOs for quick lookup
        known_daos = [
            "arbitrum", "uniswap", "compound", "aave", "makerdao",
            "ens", "gitcoin", "1inch-network", "optimism", "polygon",
            "nounsdao", "nouns", "frax", "olympusdao", "rari-capital"
        ]

        organizations = []

        def check_dao(slug):
            dao_info = self.get_dao_by_slug(slug)
            if dao_info and dao_info.get("governorIds"):
                return dao_info
            return None

        if search_term:
            # Try exact matches first
            test_slugs = [search_term.lower(), search_term.lower().replace(" ", "-")]
            for slug in test_slugs:
                dao_info = check_dao(slug)
                if dao_info:
                    organizations.append(dao_info)

            # Search in known DAOs
            matching_daos = [slug for slug in known_daos if search_term.lower() in slug.lower()]
            for slug in matching_daos:
                dao_info = check_dao(slug)
                if dao_info and not any(org['slug'] == dao_info['slug'] for org in organizations):
                    organizations.append(dao_info)
        else:
            # Get popular DAOs with limited parallel requests
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_slug = {executor.submit(check_dao, slug): slug for slug in known_daos[:10]}

                for future in as_completed(future_to_slug):
                    dao_info = future.result()
                    if dao_info:
                        organizations.append(dao_info)

        return organizations

    def get_dao_by_slug(self, slug: str) -> Optional[Dict]:
        """Get DAO info by slug with enhanced error handling"""
        query = """
        query GetDAOBySlug($slug: String!) {
          organization(input: {slug: $slug}) {
            id
            name
            slug
            governorIds
            delegatesCount
          }
        }
        """

        variables = {"slug": slug}
        data = self.api.make_request_optimized(query, variables)

        if data and data.get("data"):
            return data["data"].get("organization")
        return None

    def get_governor_ids(self, dao_slug: str) -> List[str]:
        """Get all governor IDs for a specific DAO"""
        dao_info = self.get_dao_by_slug(dao_slug)

        if dao_info:
            print(f"DAO: {dao_info['name']} ({dao_info['slug']})")

            governor_ids = dao_info.get("governorIds", [])
            print(f"Found {len(governor_ids)} governor IDs:")
            for i, gov_id in enumerate(governor_ids, 1):
                print(f"  {i}. {gov_id}")
            return governor_ids
        else:
            print(f"DAO with slug '{dao_slug}' not found")
            return []

    def get_governance_stats_fast(self, governor_id: str) -> Dict:
        """Get governance statistics with optimized query"""
        print(f"Fetching governance statistics for: {governor_id}")

        query = """
        query GovernanceStats($governorId: AccountID!) {
          governor(input: {id: $governorId}) {
            id
            name
            slug
            kind
            type
            quorum
            delegatesCount
            delegatesVotesCount
            tokenOwnersCount
            proposalStats {
              total
              failed
              passed
              active
            }
            token {
              id
              name
              symbol
              decimals
              supply
            }
            timelockId
            organization {
              id
              name
              slug
            }
          }
        }
        """

        variables = {"governorId": governor_id}
        data = self.api.make_request_optimized(query, variables)

        if data and data.get("data"):
            return data["data"].get("governor", {})
        return {}

    def get_all_proposals_optimized(self, governor_id: str, max_proposals: int = None) -> List[Dict]:
        """Get proposals with smart pagination and optional limits"""
        print(f"Fetching proposals for governor: {governor_id}")

        all_proposals = []
        after_cursor = None
        page_count = 0

        query = """
        query Proposals($input: ProposalsInput!) {
          proposals(input: $input) {
            nodes {
              ... on Proposal {
                id
                status
                block {
                  timestamp
                  number
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
                start {
                  ... on Block {
                    timestamp
                    number
                  }
                  ... on BlocklessTimestamp {
                    timestamp
                  }
                }
                createdAt
                executableCalls {
                  calldata
                  target
                  value
                  signature
                }
                proposer {
                  address
                  ens
                  name
                }
                voteStats {
                  type
                  percent
                  votersCount
                  votesCount
                }
                quorum
                governor {
                  id
                  name
                  slug
                  quorum
                  timelockId
                  token {
                    id
                    name
                    symbol
                    decimals
                  }
                }
                organization {
                  id
                  name
                }
              }
            }
            pageInfo {
              lastCursor
              count
            }
          }
        }
        """

        start_time = time.time()

        while True:
            page_count += 1

            # Progress reporting
            if page_count % 5 == 0 or page_count == 1:
                elapsed = time.time() - start_time
                rate = len(all_proposals) / elapsed if elapsed > 0 else 0
                print(f"  Page {page_count}: {len(all_proposals)} proposals ({rate:.1f}/sec)")

            variables = {
                "input": {
                    "filters": {"governorId": governor_id},
                    "page": {"limit": 50}  # Increased page size for efficiency
                }
            }

            if after_cursor:
                variables["input"]["page"]["afterCursor"] = after_cursor

            data = self.api.make_request_optimized(query, variables)

            if not data or not data.get("data"):
                print(f"Failed to get proposals page {page_count}")
                break

            proposals_data = data["data"].get("proposals", {})
            nodes = proposals_data.get("nodes", [])
            page_info = proposals_data.get("pageInfo", {})

            if not nodes:
                print(f"No more proposals found")
                break

            all_proposals.extend(nodes)

            # Check limits
            if max_proposals and len(all_proposals) >= max_proposals:
                print(f"Reached limit of {max_proposals} proposals")
                all_proposals = all_proposals[:max_proposals]
                break

            # Check for more pages
            last_cursor = page_info.get("lastCursor")
            if not last_cursor:
                print(f"Reached end of proposals")
                break

            after_cursor = last_cursor

        elapsed_time = time.time() - start_time
        print(f"Fetched {len(all_proposals)} proposals in {elapsed_time:.1f}s ({len(all_proposals)/elapsed_time:.1f} proposals/sec)")

        return all_proposals

    def get_all_delegates_optimized(self, governor_id: str, max_delegates: int = None) -> List[Dict]:
        """Get delegates with smart pagination and optional limits"""
        print(f"Fetching delegates for governor: {governor_id}")

        all_delegates = []
        after_cursor = None
        page_count = 0

        query = """
        query Delegates($input: DelegatesInput!) {
          delegates(input: $input) {
            nodes {
              ... on Delegate {
                id
                account {
                  address
                  ens
                  name
                  picture
                  bio
                  twitter
                  type
                }
                votesCount
                delegatorsCount
                statement {
                  id
                  statementSummary
                  statement
                  organizationID
                  discourseUsername
                  isSeekingDelegation
                }
              }
            }
            pageInfo {
              lastCursor
              count
            }
          }
        }
        """

        start_time = time.time()

        while True:
            page_count += 1

            # Progress reporting
            if page_count % 10 == 0 or page_count == 1:
                elapsed = time.time() - start_time
                rate = len(all_delegates) / elapsed if elapsed > 0 else 0
                print(f"  Page {page_count}: {len(all_delegates)} delegates ({rate:.1f}/sec)")

            variables = {
                "input": {
                    "filters": {"governorId": governor_id},
                    "page": {"limit": 50}  # Increased page size
                }
            }

            if after_cursor:
                variables["input"]["page"]["afterCursor"] = after_cursor

            data = self.api.make_request_optimized(query, variables)

            if not data or not data.get("data"):
                print(f"Failed to get delegates page {page_count}")
                break

            delegates_data = data["data"].get("delegates", {})
            nodes = delegates_data.get("nodes", [])
            page_info = delegates_data.get("pageInfo", {})

            if not nodes:
                print(f"No more delegates found")
                break

            all_delegates.extend([d for d in nodes if d is not None])

            # Check limits
            if max_delegates and len(all_delegates) >= max_delegates:
                print(f"Reached limit of {max_delegates} delegates")
                all_delegates = all_delegates[:max_delegates]
                break

            # Check for more pages
            last_cursor = page_info.get("lastCursor")
            if not last_cursor:
                print(f"Reached end of delegates")
                break

            after_cursor = last_cursor

        elapsed_time = time.time() - start_time
        print(f"Fetched {len(all_delegates)} delegates in {elapsed_time:.1f}s ({len(all_delegates)/elapsed_time:.1f} delegates/sec)")

        return all_delegates

    def format_votes_for_display(self, vote_stats: List[Dict]) -> Dict:
        """Format vote statistics for display"""
        formatted_votes = {
            "votes_for": "0",
            "votes_against": "0",
            "votes_abstain": "0",
            "total_votes": "0"
        }

        total_votes = 0

        for stat in vote_stats:
            vote_type = stat.get("type", "").upper()
            votes_count = int(stat.get("votesCount", 0))
            total_votes += votes_count

            # Format large numbers
            if votes_count >= 1_000_000:
                formatted = f"{votes_count/1_000_000:.2f}M"
            elif votes_count >= 1_000:
                formatted = f"{votes_count/1_000:.2f}K"
            else:
                formatted = str(votes_count)

            if vote_type in ["FOR", "YAE"]:
                formatted_votes["votes_for"] = formatted
            elif vote_type in ["AGAINST", "NAY"]:
                formatted_votes["votes_against"] = formatted
            elif vote_type == "ABSTAIN":
                formatted_votes["votes_abstain"] = formatted

        # Format total
        if total_votes >= 1_000_000:
            formatted_votes["total_votes"] = f"{total_votes/1_000_000:.2f}M"
        elif total_votes >= 1_000:
            formatted_votes["total_votes"] = f"{total_votes/1_000:.2f}K"
        else:
            formatted_votes["total_votes"] = str(total_votes)

        return formatted_votes

    def prepare_dao_basic_info(self, stats: Dict) -> Dict:
        """Prepare DAO basic information"""
        token = stats.get("token", {})
        org = stats.get("organization", {})
        proposal_stats = stats.get("proposalStats", {})

        return {
            "DAO_Name": stats.get("name", ""),
            "DAO_ID": stats.get("id", ""),
            "DAO_Slug": stats.get("slug", ""),
            "DAO_Type": stats.get("type", ""),
            "DAO_Kind": stats.get("kind", ""),
            "Token_Name": token.get("name", ""),
            "Token_Symbol": token.get("symbol", ""),
            "Token_ID": token.get("id", ""),
            "Token_Decimals": token.get("decimals", 0),
            "Token_Supply": token.get("supply", "0"),
            "Organization_Name": org.get("name", ""),
            "Organization_ID": org.get("id", ""),
            "Organization_Slug": org.get("slug", ""),
            "Quorum": stats.get("quorum", 0),
            "Delegates_Count": stats.get("delegatesCount", 0),
            "Delegates_Votes_Count": stats.get("delegatesVotesCount", 0),
            "Token_Owners_Count": stats.get("tokenOwnersCount", 0),
            "Timelock_ID": stats.get("timelockId", ""),
            "Total_Proposals": proposal_stats.get("total", 0),
            "Passed_Proposals": proposal_stats.get("passed", 0),
            "Failed_Proposals": proposal_stats.get("failed", 0),
            "Active_Proposals": proposal_stats.get("active", 0),
            "Analysis_Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def prepare_proposal_info(self, proposals: List[Dict]) -> List[Dict]:
        """Prepare proposal information with robust timestamp handling"""
        proposal_list = []

        for i, proposal in enumerate(proposals, 1):
            vote_stats = proposal.get("voteStats", [])
            formatted_votes = self.format_votes_for_display(vote_stats)

            proposer = proposal.get("proposer", {})
            proposer_display = proposer.get("name") or proposer.get("ens") or proposer.get("address", "Unknown")

            # Handle timestamps robustly
            block = proposal.get("block", {})
            created_date = ""
            block_timestamp_for_sorting = "0"

            if block.get("timestamp"):
                timestamp_str = str(block["timestamp"])
                try:
                    if 'T' in timestamp_str and 'Z' in timestamp_str:
                        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        timestamp_val = int(dt.timestamp())
                        created_date = dt.strftime("%b %d, %Y")
                        block_timestamp_for_sorting = str(timestamp_val)
                    elif timestamp_str.isdigit():
                        timestamp_val = int(timestamp_str)
                        created_date = datetime.fromtimestamp(timestamp_val).strftime("%b %d, %Y")
                        block_timestamp_for_sorting = timestamp_str
                    else:
                        timestamp_val = int(float(timestamp_str))
                        created_date = datetime.fromtimestamp(timestamp_val).strftime("%b %d, %Y")
                        block_timestamp_for_sorting = str(timestamp_val)
                except (ValueError, TypeError, OSError):
                    created_date = timestamp_str
                    block_timestamp_for_sorting = "0"

            status = proposal.get("status", "UNKNOWN").upper()

            # Calculate treasury operations
            executable_calls = proposal.get("executableCalls", [])
            total_value_wei = 0
            for call in executable_calls:
                value = call.get("value", "0")
                if value and value != "0":
                    try:
                        total_value_wei += int(value)
                    except:
                        pass

            proposal_data = {
                "Proposal": f"#{i}",
                "Status": status,
                "Date": created_date,
                "Votes_For": formatted_votes["votes_for"],
                "Votes_Against": formatted_votes["votes_against"],
                "Votes_Abstain": formatted_votes["votes_abstain"],
                "Total_Votes": formatted_votes["total_votes"],
                "Proposer": proposer_display,
                "Proposal_ID": proposal.get("id", ""),
                "Proposal_URL": f"{self.base_proposal_url}{proposal.get('id', '')}",
                "Quorum": proposal.get("quorum", 0),
                "Has_Treasury_Operations": total_value_wei > 0,
                "Treasury_Value_Wei": total_value_wei,
                "Treasury_Value_ETH": total_value_wei / 1e18 if total_value_wei > 0 else 0,
                "Executable_Calls_Count": len(executable_calls),
                "Block_Number": block.get("number", 0),
                "Block_Timestamp": block.get("timestamp", ""),
                "Block_Timestamp_Unix": block_timestamp_for_sorting,
                "Raw_Status": status
            }

            # Add vote percentages and voter counts
            for stat in vote_stats:
                vote_type = stat.get("type", "").lower()
                if vote_type:
                    proposal_data[f"{vote_type}_percent"] = stat.get("percent", 0)
                    proposal_data[f"{vote_type}_voters_count"] = int(stat.get("votersCount", 0))

            proposal_list.append(proposal_data)

        # Sort by timestamp (newest first)
        proposal_list.sort(key=lambda x: int(x.get("Block_Timestamp_Unix", "0")), reverse=True)
        return proposal_list

    def prepare_delegates_info(self, delegates: List[Dict]) -> List[Dict]:
        """Prepare delegate information"""
        delegates_list = []

        for delegate in delegates:
            account = delegate.get("account", {})
            statement = delegate.get("statement") or {}

            delegate_data = {
                "Delegate_Name": account.get("name") or account.get("ens") or "Unknown",
                "Delegate_Address": account.get("address", ""),
                "Delegate_ENS": account.get("ens", ""),
                "Delegate_URL": f"{self.base_delegate_url}{account.get('address', '')}",
                "Profile_Picture": account.get("picture", ""),
                "Bio": account.get("bio", ""),
                "Twitter": account.get("twitter", ""),
                "Account_Type": account.get("type", ""),
                "Votes_Count": delegate.get("votesCount", 0),
                "Delegators_Count": delegate.get("delegatorsCount", 0),
                "Statement_ID": statement.get("id", "") if statement else "",
                "Statement_Summary": statement.get("statementSummary", "") if statement else "",
                "Full_Statement": statement.get("statement", "") if statement else "",
                "Organization_ID": statement.get("organizationID", "") if statement else "",
                "Discourse_Username": statement.get("discourseUsername", "") if statement else "",
                "Is_Seeking_Delegation": statement.get("isSeekingDelegation", False) if statement else False,
                "Has_Statement": bool(statement.get("statement", "")),
                "Delegate_ID": delegate.get("id", "")
            }

            delegates_list.append(delegate_data)

        # Sort by voting power
        delegates_list.sort(key=lambda x: x.get("Votes_Count", 0), reverse=True)
        return delegates_list

    def save_comprehensive_data(self, governance_stats: Dict, proposals: List[Dict],
                              delegates: List[Dict], filename: str) -> str:
        """Save all data with multiple fallback options for Excel"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            print(f"Preparing comprehensive governance data...")

            # Prepare all data
            dao_info = self.prepare_dao_basic_info(governance_stats)
            proposal_info = self.prepare_proposal_info(proposals)
            delegates_info = self.prepare_delegates_info(delegates)

            # Try multiple Excel engines in order of preference
            excel_engines = ['xlsxwriter', 'openpyxl']
            excel_saved = False
            excel_filename = ""

            for engine in excel_engines:
                try:
                    excel_filename = f"{filename}_{timestamp}.xlsx"
                    print(f"Attempting to create Excel file using {engine} engine...")

                    with pd.ExcelWriter(excel_filename, engine=engine) as writer:
                        # DAO info sheet
                        dao_df = pd.DataFrame([dao_info])
                        dao_df.to_excel(writer, sheet_name='DAO_Basic_Info', index=False)

                        # Proposals sheet
                        if proposal_info:
                            proposals_df = pd.DataFrame(proposal_info)
                            proposals_df.to_excel(writer, sheet_name='Proposals_Details', index=False)
                            print(f"Saved {len(proposal_info)} proposals")

                        # Delegates sheet
                        if delegates_info:
                            delegates_df = pd.DataFrame(delegates_info)
                            delegates_df.to_excel(writer, sheet_name='Delegates_Complete_Info', index=False)
                            print(f"Saved {len(delegates_info)} delegates")

                    print(f"Excel file created: {excel_filename}")
                    excel_saved = True
                    break

                except ImportError:
                    print(f"{engine} not available, trying next option...")
                    continue
                except Exception as e:
                    print(f"Failed with {engine}: {e}")
                    continue

            # If Excel fails completely, save as CSV files
            if not excel_saved:
                print("Excel libraries not available. Saving as CSV files...")

                csv_files = []

                # Save DAO basic info
                dao_csv = f"{filename}_dao_info_{timestamp}.csv"
                dao_df = pd.DataFrame([dao_info])
                dao_df.to_csv(dao_csv, index=False)
                csv_files.append(dao_csv)
                print(f"DAO info saved to: {dao_csv}")

                # Save proposals
                if proposal_info:
                    proposals_csv = f"{filename}_proposals_{timestamp}.csv"
                    proposals_df = pd.DataFrame(proposal_info)
                    proposals_df.to_csv(proposals_csv, index=False)
                    csv_files.append(proposals_csv)
                    print(f"{len(proposal_info)} proposals saved to: {proposals_csv}")

                # Save delegates
                if delegates_info:
                    delegates_csv = f"{filename}_delegates_{timestamp}.csv"
                    delegates_df = pd.DataFrame(delegates_info)
                    delegates_df.to_csv(delegates_csv, index=False)
                    csv_files.append(delegates_csv)
                    print(f"{len(delegates_info)} delegates saved to: {delegates_csv}")

                return f"CSV files: {', '.join(csv_files)}"

            return excel_filename

        except Exception as e:
            print(f"Error saving data: {e}")
            print("\n🔧 TROUBLESHOOTING EXCEL SAVE ERROR:")
            print("1. Install required libraries:")
            print("   pip install xlsxwriter openpyxl")
            print("2. Or run the script anyway - data will be saved as CSV files")
            return ""

    def export_summary_report(self, governance_stats: Dict, proposals: List[Dict],
                            delegates: List[Dict], filename: str) -> str:
        """Export summary report in markdown format"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            report_filename = f"{filename}_summary_report.md"

            # Calculate statistics
            total_proposals = len(proposals)
            executed_proposals = len([p for p in proposals if p.get("Status", "").upper() == "EXECUTED"])
            failed_proposals = len([p for p in proposals if p.get("Status", "").upper() in ["DEFEATED", "FAILED", "CANCELED"]])

            total_delegates = len(delegates)
            active_delegates = len([d for d in delegates if d.get("Votes_Count", 0) > 0])

            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(f"# {self.dao_name} Governance Analysis Report\n\n")
                f.write(f"**Generated:** {timestamp}\n\n")

                # DAO Overview
                token = governance_stats.get("token", {})
                org = governance_stats.get("organization", {})

                f.write(f"## DAO Overview\n\n")
                f.write(f"- **DAO Name:** {governance_stats.get('name', 'N/A')}\n")
                f.write(f"- **Organization:** {org.get('name', 'N/A')}\n")
                f.write(f"- **Token:** {token.get('name', 'N/A')} ({token.get('symbol', 'N/A')})\n")
                f.write(f"- **Governance Type:** {governance_stats.get('type', 'N/A')}\n")
                f.write(f"- **Quorum Required:** {governance_stats.get('quorum', 'N/A')}\n\n")

                # Proposal Statistics
                f.write(f"## Proposal Statistics\n\n")
                f.write(f"- **Total Proposals:** {total_proposals}\n")
                f.write(f"- **Executed:** {executed_proposals}\n")
                f.write(f"- **Failed/Defeated:** {failed_proposals}\n")

                if total_proposals > 0:
                    success_rate = (executed_proposals / total_proposals) * 100
                    f.write(f"- **Success Rate:** {success_rate:.1f}%\n\n")
                else:
                    f.write(f"- **Success Rate:** N/A\n\n")

                # Delegate Statistics
                f.write(f"## Delegate Statistics\n\n")
                f.write(f"- **Total Delegates:** {total_delegates}\n")
                f.write(f"- **Active Delegates:** {active_delegates}\n")
                f.write(f"- **Total Delegated Votes:** {governance_stats.get('delegatesVotesCount', 'N/A')}\n")
                f.write(f"- **Token Holders:** {governance_stats.get('tokenOwnersCount', 'N/A')}\n\n")

                # Recent Proposals (last 5)
                if proposals:
                    f.write(f"## Recent Proposals\n\n")
                    recent_proposals = proposals[:5]  # Already sorted by date
                    for i, proposal in enumerate(recent_proposals, 1):
                        f.write(f"{i}. **{proposal.get('Proposal', 'N/A')}**\n")
                        f.write(f"   - Status: {proposal.get('Status', 'N/A')}\n")
                        f.write(f"   - Date: {proposal.get('Date', 'N/A')}\n")
                        f.write(f"   - Votes For: {proposal.get('Votes_For', 'N/A')}\n")
                        f.write(f"   - Votes Against: {proposal.get('Votes_Against', 'N/A')}\n")
                        f.write(f"   - Proposer: {proposal.get('Proposer', 'N/A')}\n\n")

                # Top Delegates (by voting power)
                if delegates:
                    f.write(f"## Top Delegates by Voting Power\n\n")
                    top_delegates = delegates[:10]  # Already sorted by votes count
                    for i, delegate in enumerate(top_delegates, 1):
                        name = delegate.get('Delegate_Name', 'Unknown')
                        votes = delegate.get('Votes_Count', 0)
                        delegators = delegate.get('Delegators_Count', 0)
                        f.write(f"{i}. **{name}**\n")
                        f.write(f"   - Voting Power: {votes:,}\n")
                        f.write(f"   - Delegators: {delegators:,}\n")
                        if delegate.get('Bio'):
                            bio = delegate.get('Bio', '')[:100]  # Truncate long bios
                            f.write(f"   - Bio: {bio}{'...' if len(delegate.get('Bio', '')) > 100 else ''}\n")
                        f.write(f"\n")

                f.write(f"---\n")
                f.write(f"*Report generated by Universal DAO Governance Analyzer*\n")

            print(f"Summary report saved to {report_filename}")
            return report_filename

        except Exception as e:
            print(f"Error creating summary report: {e}")
            return ""

    def install_excel_dependencies(self):
        """Helper function to install Excel dependencies"""
        print("\n📦 EXCEL DEPENDENCY INSTALLER")
        print("To save Excel files, you need one of these libraries:")
        print("1. xlsxwriter (recommended for performance)")
        print("2. openpyxl (widely used)")

        print("\nInstallation commands:")
        print("pip install xlsxwriter")
        print("pip install openpyxl")
        print("\nOr install both:")
        print("pip install xlsxwriter openpyxl")

        user_input = input("\nWould you like the script to continue anyway and save as CSV? (y/n): ").strip().lower()
        return user_input in ['y', 'yes']


def display_popular_daos():
    """Display some popular DAO examples for reference"""
    popular_daos = [
        {"name": "Arbitrum DAO", "slug": "arbitrum"},
        {"name": "Uniswap", "slug": "uniswap"},
        {"name": "Compound", "slug": "compound"},
        {"name": "Aave", "slug": "aave"},
        {"name": "MakerDAO", "slug": "makerdao"},
        {"name": "ENS", "slug": "ens"},
        {"name": "Gitcoin", "slug": "gitcoin"},
        {"name": "1inch", "slug": "1inch-network"},
        {"name": "Optimism", "slug": "optimism"},
        {"name": "Polygon", "slug": "polygon"}
    ]

    print("\n📋 Popular DAOs (for reference):")
    for i, dao in enumerate(popular_daos, 1):
        print(f"{i:2d}. {dao['name']:15} (slug: {dao['slug']})")
    print()


def check_dependencies():
    """Check if required libraries are available"""
    missing_libs = []
    available_engines = []

    try:
        import xlsxwriter
        available_engines.append('xlsxwriter')
        print("xlsxwriter is available")
    except ImportError:
        missing_libs.append('xlsxwriter')

    try:
        import openpyxl
        available_engines.append('openpyxl')
        print("openpyxl is available")
    except ImportError:
        missing_libs.append('openpyxl')

    if not available_engines:
        print("WARNING: No Excel libraries found!")
        print("Data will be saved as CSV files instead.")
        print("\nTo enable Excel support, install:")
        print("pip install xlsxwriter openpyxl")
        return False
    else:
        print(f"Excel support available via: {', '.join(available_engines)}")
        return True


def get_dao_choice():
    """Get DAO choice from user with multiple options"""
    print("=== Universal DAO Governance Data Analyzer ===")
    print("This program can analyze ANY DAO on Tally.xyz")
    print()

    # Check dependencies first
    print("Checking dependencies...")
    check_dependencies()
    print()

    display_popular_daos()

    print("Choose how you want to select a DAO:")
    print("1. Enter DAO slug directly (e.g., 'arbitrum', 'uniswap')")
    print("2. Search for DAOs by name")
    print("3. Browse popular DAOs")

    choice = input("\nEnter your choice (1-3): ").strip()
    return choice


def main():
    # Get API key from environment variable
    API_KEY = os.getenv("TALLY_API_KEY", "")

    if not API_KEY or API_KEY == "your_api_key_here":
        print("ERROR: Please set your Tally API key!")
        print("Set environment variable: set TALLY_API_KEY=your_key_here")
        print("Get your API key from: https://docs.tally.xyz/tally-api/authentication")
        return

    analyzer = UniversalDAOAnalyzer(API_KEY)

    try:
        # Validate API key first
        if not analyzer.validate_api_key():
            print("Invalid API key. Please check and try again.")
            return

        # Get DAO choice from user
        choice = get_dao_choice()

        dao_slug = ""
        dao_info = None

        if choice == "1":
            # Direct slug entry
            dao_slug = input("\nEnter the DAO slug (e.g., 'arbitrum', 'uniswap'): ").strip().lower()
            if dao_slug:
                dao_info = analyzer.get_dao_by_slug(dao_slug)
                if not dao_info:
                    print(f"DAO '{dao_slug}' not found.")
                    return

        elif choice == "2":
            # Search for DAOs
            search_term = input("\nEnter search term for DAO name: ").strip()
            print(f"\nSearching for DAOs matching '{search_term}'...")

            search_results = analyzer.search_daos_fast(search_term)
            if not search_results:
                print("No DAOs found matching your search.")
                return

            print(f"\nFound {len(search_results)} DAOs:")
            for i, dao in enumerate(search_results[:20], 1):  # Limit to first 20 results
                gov_count = len(dao.get("governorIds", []))
                print(f"{i:2d}. {dao['name']:25} (slug: {dao['slug']:15}) - {gov_count} governors")

            try:
                dao_choice = int(input(f"\nSelect DAO (1-{min(len(search_results), 20)}): ").strip())
                if 1 <= dao_choice <= len(search_results):
                    dao_info = search_results[dao_choice - 1]
                    dao_slug = dao_info['slug']
                else:
                    print("Invalid selection.")
                    return
            except ValueError:
                print("Invalid input. Please enter a number.")
                return

        elif choice == "3":
            # Browse popular DAOs
            print("\nFetching popular DAOs...")
            popular_daos = analyzer.search_daos_fast()  # Get all DAOs
            if not popular_daos:
                print("Failed to fetch DAOs.")
                return

            # Show first 20 popular DAOs
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
                    return
            except ValueError:
                print("Invalid input. Please enter a number.")
                return

        else:
            print("Invalid choice.")
            return

        if not dao_slug or not dao_info:
            print("Failed to select a DAO.")
            return

        # Set DAO information in analyzer
        analyzer.set_dao_info(dao_slug, dao_info['name'])

        print(f"\nSelected DAO: {dao_info['name']} ({dao_slug})")

        # Get governor IDs for the selected DAO
        governor_ids = analyzer.get_governor_ids(dao_slug)

        if not governor_ids:
            print(f"No governors found for DAO '{dao_slug}'.")
            return

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
            print("Invalid input. Please enter a number.")
            return

        if 1 <= choice_num <= len(governor_ids):
            selected_gov = governor_ids[choice_num - 1]
            print(f"\nAnalyzing governance data for: {selected_gov}")
            print("This may take several minutes...")

            # Get governance statistics
            print("\n" + "="*60)
            governance_stats = analyzer.get_governance_stats_fast(selected_gov)

            # Get all proposals
            print("\n" + "="*60)
            proposals = analyzer.get_all_proposals_optimized(selected_gov)

            # Get all delegates
            print("\n" + "="*60)
            delegates = analyzer.get_all_delegates_optimized(selected_gov)

            # Save comprehensive data
            print("\n" + "="*60)
            print("Saving governance data...")

            clean_dao_slug = dao_slug.replace(":", "_").replace("/", "_").replace("-", "_")
            clean_gov_id = selected_gov.replace(":", "_").replace("/", "_")

            # Save Excel file (or CSV as fallback)
            saved_file = analyzer.save_comprehensive_data(
                governance_stats,
                proposals,
                delegates,
                f"{clean_dao_slug}_governance_analysis_{clean_gov_id}"
            )

            # Save summary report
            summary_file = analyzer.export_summary_report(
                governance_stats,
                proposals,
                delegates,
                f"{clean_dao_slug}_governance_analysis_{clean_gov_id}"
            )

            # Final summary
            print(f"\n" + "="*60)
            print(f"ANALYSIS COMPLETE FOR {dao_info['name'].upper()}!")

            if saved_file:
                print(f"Governance data saved to: {saved_file}")
            if summary_file:
                print(f"Summary report saved to: {summary_file}")

            print(f"\nData Summary:")
            print(f"- Found {len(proposals)} proposals")
            print(f"- Found {len(delegates)} delegates")

            if proposals:
                executed = len([p for p in proposals if p.get("Status", "").upper() == "EXECUTED"])
                success_rate = (executed/len(proposals)*100) if len(proposals) > 0 else 0
                print(f"- {executed} executed proposals ({success_rate:.1f}% success rate)")

            if delegates:
                active_delegates = len([d for d in delegates if d.get("Votes_Count", 0) > 0])
                print(f"- {active_delegates} active delegates with voting power")

            if len(proposals) == 0 and len(delegates) == 0:
                print(f"\nNote: Limited data found for {dao_info['name']}")
                print(f"Check: https://www.tally.xyz/gov/{dao_slug}")

        else:
            print("Invalid choice.")

    except ValueError:
        print("Invalid input.")
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()