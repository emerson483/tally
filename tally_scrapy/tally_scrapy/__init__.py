"""
Tally DAO Scrapy - A Python library for fetching DAO governance data from Tally.xyz

Modules:
- basic: DAO governance analyzer (stats, proposals, delegates)
- prop: DAO proposals fetcher
- statement: Delegation statements fetcher
- voting_matrix: Voting matrix builder
"""

from .basic import UniversalDAOAnalyzer, EnhancedTallyAPI
from .prop import DAOProposalsFetcher
from .statement import DAODelegationStatementsFetcher
from .voting_matrix import FastDAOFetcher, DAOConfig

__version__ = "1.0.0"
__all__ = [
    "UniversalDAOAnalyzer",
    "EnhancedTallyAPI", 
    "DAOProposalsFetcher",
    "DAODelegationStatementsFetcher",
    "FastDAOFetcher",
    "DAOConfig"
]
