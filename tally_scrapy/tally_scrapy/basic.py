"""Basic DAO Governance Analyzer - Fetches governance stats, proposals, and delegates from Tally API."""

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


class EnhancedTallyAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.endpoint = "https://api.tally.xyz/query"
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        self.rate_limit_count = 0
        self.min_delay = 1.05
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
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay:
                time.sleep(self.min_delay - elapsed)
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
                    self.success_count += 1
                    return data
                elif response.status_code == 429:
                    self.rate_limit_count += 1
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
        return None

    def get_stats(self):
        total = max(self.request_count, 1)
        return {
            'total_requests': self.request_count,
            'success_rate': f"{(self.success_count / total) * 100:.1f}%",
            'current_delay': f"{self.min_delay:.2f}s",
            'efficiency': f"{self.success_count / total:.2%}"
        }


class UniversalDAOAnalyzer:
    def __init__(self, api_key: str):
        self.api = EnhancedTallyAPI(api_key)
        self.dao_slug = ""
        self.dao_name = ""
        self.base_proposal_url = ""
        self.base_delegate_url = ""

    def set_dao_info(self, dao_slug: str, dao_name: str = ""):
        self.dao_slug = dao_slug.lower()
        self.dao_name = dao_name or dao_slug.title()
        self.base_proposal_url = f"https://www.tally.xyz/gov/{self.dao_slug}/proposal/"
        self.base_delegate_url = f"https://www.tally.xyz/gov/{self.dao_slug}/delegate/"

    def validate_api_key(self) -> bool:
        query = 'query { organization(input: {slug: "arbitrum"}) { id name } }'
        data = self.api.make_request_optimized(query)
        return data and data.get("data") is not None

    def search_daos_fast(self, search_term: str = "") -> List[Dict]:
        known_daos = ["arbitrum", "uniswap", "compound", "aave", "makerdao", "ens", "gitcoin", "optimism", "polygon"]
        organizations = []

        def check_dao(slug):
            dao_info = self.get_dao_by_slug(slug)
            return dao_info if dao_info and dao_info.get("governorIds") else None

        if search_term:
            for slug in [search_term.lower(), search_term.lower().replace(" ", "-")]:
                dao_info = check_dao(slug)
                if dao_info:
                    organizations.append(dao_info)
            for slug in known_daos:
                if search_term.lower() in slug.lower():
                    dao_info = check_dao(slug)
                    if dao_info and not any(org['slug'] == dao_info['slug'] for org in organizations):
                        organizations.append(dao_info)
        else:
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(check_dao, slug): slug for slug in known_daos[:10]}
                for future in as_completed(futures):
                    if dao_info := future.result():
                        organizations.append(dao_info)
        return organizations

    def get_dao_by_slug(self, slug: str) -> Optional[Dict]:
        query = """
        query GetDAOBySlug($slug: String!) {
          organization(input: {slug: $slug}) { id name slug governorIds delegatesCount }
        }
        """
        data = self.api.make_request_optimized(query, {"slug": slug})
        return data["data"].get("organization") if data and data.get("data") else None

    def get_governor_ids(self, dao_slug: str) -> List[str]:
        dao_info = self.get_dao_by_slug(dao_slug)
        return dao_info.get("governorIds", []) if dao_info else []

    def get_governance_stats_fast(self, governor_id: str) -> Dict:
        query = """
        query GovernanceStats($governorId: AccountID!) {
          governor(input: {id: $governorId}) {
            id name slug kind type quorum delegatesCount delegatesVotesCount tokenOwnersCount
            proposalStats { total failed passed active }
            token { id name symbol decimals supply }
            timelockId
            organization { id name slug }
          }
        }
        """
        data = self.api.make_request_optimized(query, {"governorId": governor_id})
        return data["data"].get("governor", {}) if data and data.get("data") else {}

    def get_all_proposals_optimized(self, governor_id: str, max_proposals: int = None) -> List[Dict]:
        all_proposals = []
        after_cursor = None

        query = """
        query Proposals($input: ProposalsInput!) {
          proposals(input: $input) {
            nodes {
              ... on Proposal {
                id status
                block { timestamp number }
                end { ... on Block { timestamp number } ... on BlocklessTimestamp { timestamp } }
                start { ... on Block { timestamp number } ... on BlocklessTimestamp { timestamp } }
                createdAt
                executableCalls { calldata target value signature }
                proposer { address ens name }
                voteStats { type percent votersCount votesCount }
                quorum
                governor { id name slug quorum timelockId token { id name symbol decimals } }
                organization { id name }
              }
            }
            pageInfo { lastCursor count }
          }
        }
        """

        while True:
            variables = {"input": {"filters": {"governorId": governor_id}, "page": {"limit": 50}}}
            if after_cursor:
                variables["input"]["page"]["afterCursor"] = after_cursor

            data = self.api.make_request_optimized(query, variables)
            if not data or not data.get("data"):
                break

            nodes = data["data"].get("proposals", {}).get("nodes", [])
            if not nodes:
                break

            all_proposals.extend(nodes)
            if max_proposals and len(all_proposals) >= max_proposals:
                all_proposals = all_proposals[:max_proposals]
                break

            last_cursor = data["data"].get("proposals", {}).get("pageInfo", {}).get("lastCursor")
            if not last_cursor:
                break
            after_cursor = last_cursor

        return all_proposals

    def get_all_delegates_optimized(self, governor_id: str, max_delegates: int = None) -> List[Dict]:
        all_delegates = []
        after_cursor = None

        query = """
        query Delegates($input: DelegatesInput!) {
          delegates(input: $input) {
            nodes {
              ... on Delegate {
                id
                account { address ens name picture bio twitter type }
                votesCount delegatorsCount
                statement { id statementSummary statement organizationID discourseUsername isSeekingDelegation }
              }
            }
            pageInfo { lastCursor count }
          }
        }
        """

        while True:
            variables = {"input": {"filters": {"governorId": governor_id}, "page": {"limit": 50}}}
            if after_cursor:
                variables["input"]["page"]["afterCursor"] = after_cursor

            data = self.api.make_request_optimized(query, variables)
            if not data or not data.get("data"):
                break

            nodes = data["data"].get("delegates", {}).get("nodes", [])
            if not nodes:
                break

            all_delegates.extend([d for d in nodes if d is not None])
            if max_delegates and len(all_delegates) >= max_delegates:
                all_delegates = all_delegates[:max_delegates]
                break

            last_cursor = data["data"].get("delegates", {}).get("pageInfo", {}).get("lastCursor")
            if not last_cursor:
                break
            after_cursor = last_cursor

        return all_delegates

    def format_votes_for_display(self, vote_stats: List[Dict]) -> Dict:
        formatted = {"votes_for": "0", "votes_against": "0", "votes_abstain": "0", "total_votes": "0"}
        total = 0
        for stat in vote_stats:
            vtype = stat.get("type", "").upper()
            count = int(stat.get("votesCount", 0))
            total += count
            fmt = f"{count/1_000_000:.2f}M" if count >= 1_000_000 else f"{count/1_000:.2f}K" if count >= 1_000 else str(count)
            if vtype in ["FOR", "YAE"]:
                formatted["votes_for"] = fmt
            elif vtype in ["AGAINST", "NAY"]:
                formatted["votes_against"] = fmt
            elif vtype == "ABSTAIN":
                formatted["votes_abstain"] = fmt
        formatted["total_votes"] = f"{total/1_000_000:.2f}M" if total >= 1_000_000 else f"{total/1_000:.2f}K" if total >= 1_000 else str(total)
        return formatted

    def prepare_dao_basic_info(self, stats: Dict) -> Dict:
        token = stats.get("token", {})
        org = stats.get("organization", {})
        ps = stats.get("proposalStats", {})
        return {
            "DAO_Name": stats.get("name", ""), "DAO_ID": stats.get("id", ""), "DAO_Slug": stats.get("slug", ""),
            "Token_Name": token.get("name", ""), "Token_Symbol": token.get("symbol", ""),
            "Organization_Name": org.get("name", ""), "Quorum": stats.get("quorum", 0),
            "Delegates_Count": stats.get("delegatesCount", 0), "Total_Proposals": ps.get("total", 0),
            "Passed_Proposals": ps.get("passed", 0), "Failed_Proposals": ps.get("failed", 0),
            "Analysis_Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def prepare_proposal_info(self, proposals: List[Dict]) -> List[Dict]:
        result = []
        for i, p in enumerate(proposals, 1):
            votes = self.format_votes_for_display(p.get("voteStats", []))
            proposer = p.get("proposer", {})
            block = p.get("block", {})
            created = ""
            try:
                ts = block.get("timestamp", "")
                if ts and 'T' in str(ts):
                    created = datetime.fromisoformat(str(ts).replace('Z', '+00:00')).strftime("%b %d, %Y")
                elif ts and str(ts).isdigit():
                    created = datetime.fromtimestamp(int(ts)).strftime("%b %d, %Y")
            except:
                pass

            result.append({
                "Proposal": f"#{i}", "Status": p.get("status", "").upper(), "Date": created,
                "Votes_For": votes["votes_for"], "Votes_Against": votes["votes_against"],
                "Vote_Abstain": votes["votes_abstain"], "Total_Votes": votes["total_votes"],
                "Proposer": proposer.get("name") or proposer.get("ens") or proposer.get("address", ""),
                "Proposal_ID": p.get("id", ""), "Proposal_URL": f"{self.base_proposal_url}{p.get('id', '')}",
                "Quorum": p.get("quorum", 0)
            })
        return result

    def prepare_delegates_info(self, delegates: List[Dict]) -> List[Dict]:
        result = []
        for d in delegates:
            account = d.get("account", {})
            stmt = d.get("statement") or {}
            result.append({
                "Delegate_Name": account.get("name") or account.get("ens") or "Unknown",
                "Delegate_Address": account.get("address", ""),
                "Delegate_ENS": account.get("ens", ""),
                "Delegate_URL": f"{self.base_delegate_url}{account.get('address', '')}",
                "Votes_Count": d.get("votesCount", 0),
                "Delegators_Count": d.get("delegatorsCount", 0),
                "Has_Statement": bool(stmt.get("statement", "")),
                "Is_Seeking_Delegation": stmt.get("isSeekingDelegation", False)
            })
        result.sort(key=lambda x: x.get("Votes_Count", 0), reverse=True)
        return result

    def save_comprehensive_data(self, governance_stats: Dict, proposals: List[Dict], delegates: List[Dict], filename: str) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dao_info = self.prepare_dao_basic_info(governance_stats)
        proposal_info = self.prepare_proposal_info(proposals)
        delegates_info = self.prepare_delegates_info(delegates)

        try:
            excel_file = f"{filename}_{timestamp}.xlsx"
            with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
                pd.DataFrame([dao_info]).to_excel(writer, sheet_name='DAO_Info', index=False)
                if proposal_info:
                    pd.DataFrame(proposal_info).to_excel(writer, sheet_name='Proposals', index=False)
                if delegates_info:
                    pd.DataFrame(delegates_info).to_excel(writer, sheet_name='Delegates', index=False)
            print(f"Saved: {excel_file}")
            return excel_file
        except:
            csv_file = f"{filename}_{timestamp}.csv"
            if proposal_info:
                pd.DataFrame(proposal_info).to_csv(csv_file, index=False)
            print(f"Saved (CSV fallback): {csv_file}")
            return csv_file

    def export_summary_report(self, governance_stats: Dict, proposals: List[Dict], delegates: List[Dict], filename: str) -> str:
        report_file = f"{filename}_summary.md"
        token = governance_stats.get("token", {})
        org = governance_stats.get("organization", {})

        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"# {self.dao_name} Governance Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"## Overview\n- **DAO:** {governance_stats.get('name', 'N/A')}\n")
            f.write(f"- **Token:** {token.get('name', 'N/A')} ({token.get('symbol', 'N/A')})\n")
            f.write(f"- **Proposals:** {len(proposals)}\n- **Delegates:** {len(delegates)}\n")
        print(f"Saved: {report_file}")
        return report_file


def display_popular_daos():
    daos = ["arbitrum", "uniswap", "compound", "aave", "ens", "gitcoin", "optimism", "polygon"]
    print("\nPopular DAOs:", ", ".join(daos))


def check_dependencies():
    try:
        import xlsxwriter
        return True
    except:
        try:
            import openpyxl
            return True
        except:
            print("Note: Install xlsxwriter/openpyxl for Excel output")
            return False


def get_dao_choice():
    print("=== DAO Governance Analyzer ===")
    check_dependencies()
    display_popular_daos()
    print("\n1. Enter slug  2. Search  3. Browse")
    return input("Choice (1-3): ").strip()


def main():
    api_key = os.getenv("TALLY_API_KEY", "")
    if not api_key:
        print("Error: TALLY_API_KEY not set")
        return

    analyzer = UniversalDAOAnalyzer(api_key)
    if not analyzer.validate_api_key():
        print("Invalid API key")
        return

    choice = get_dao_choice()
    dao_slug, dao_info = "", None

    if choice == "1":
        dao_slug = input("DAO slug: ").strip().lower()
        dao_info = analyzer.get_dao_by_slug(dao_slug)
    elif choice in ["2", "3"]:
        term = input("Search term: ").strip() if choice == "2" else ""
        results = analyzer.search_daos_fast(term)
        if results:
            for i, d in enumerate(results[:10], 1):
                print(f"{i}. {d['name']} ({d['slug']})")
            try:
                idx = int(input("Select: ").strip()) - 1
                if 0 <= idx < len(results):
                    dao_info = results[idx]
                    dao_slug = dao_info['slug']
            except:
                pass

    if not dao_slug or not dao_info:
        print("No DAO selected")
        return

    analyzer.set_dao_info(dao_slug, dao_info['name'])
    governor_ids = analyzer.get_governor_ids(dao_slug)
    if not governor_ids:
        print("No governors found")
        return

    selected_gov = governor_ids[0]
    print(f"\nAnalyzing {dao_info['name']}...")

    stats = analyzer.get_governance_stats_fast(selected_gov)
    proposals = analyzer.get_all_proposals_optimized(selected_gov)
    delegates = analyzer.get_all_delegates_optimized(selected_gov)

    clean_slug = dao_slug.replace(":", "_").replace("/", "_").replace("-", "_")
    clean_gov = selected_gov.replace(":", "_").replace("/", "_")

    analyzer.save_comprehensive_data(stats, proposals, delegates, f"{clean_slug}_governance_{clean_gov}")
    analyzer.export_summary_report(stats, proposals, delegates, f"{clean_slug}_governance_{clean_gov}")

    print(f"\nComplete! {len(proposals)} proposals, {len(delegates)} delegates")


if __name__ == "__main__":
    main()