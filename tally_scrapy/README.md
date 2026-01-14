# Tally DAO Scraper

A robust Python toolkit for fetching and analyzing DAO governance data from [Tally.xyz](https://www.tally.xyz/).

## 🚀 Features

- **Unified Runner**: Single entry point (`main.py`) to run all data fetching modules.
- **Comprehensive Analysis**:
  - **Governance Stats**: General DAO stats, quorum, token info.
  - **Proposals**: Detailed proposal data including voting results.
  - **Delegates**: Complete list of delegates with voting power.
  - **Voting Matrix**: Detailed analysis of who voted on what and how.
- **Optimized Performance**: Smart rate limiting, caching, and concurrent processing.
- **Output Formats**: Excel (xlsx), CSV, and Markdown reports.

## 📦 Installation

1.  **Clone or Download** the repository.
2.  **Install Dependencies**:

    ```bash
    pip install requests pandas aiohttp xlsxwriter openpyxl
    ```

## 🔑 Configuration

You need a Tally API key relative to your environment to fetch data.

1.  Get your key from the [Tally API Dashboard](https://docs.tally.xyz/tally-api/authentication).
2.  Set the `TALLY_API_KEY` environment variable:

    **Windows (Command Prompt):**
    ```cmd
    set TALLY_API_KEY=your_api_key_here
    ```

    **Windows (PowerShell):**
    ```powershell
    $env:TALLY_API_KEY="your_api_key_here"
    ```

    **Mac/Linux:**
    ```bash
    export TALLY_API_KEY=your_api_key_here
    ```

## 🛠️ Usage

Run the unified runner script:

```bash
python main.py
```

Follow the interactive prompts:
1.  **DAO Name**: Enter the name (e.g., `Arbitrum`, `Compound`).
2.  **DAO Slug**: Enter the slug (e.g., `arbitrum`, `compound`).
3.  **Alternative Slugs**: (Optional) press Enter to skip.
4.  **Expected Delegates**: (Optional) press Enter for default.

The script executes 4 sequential stages. Here is what happens in each stage:

1.  **Stage 1: Delegation Statements**
    - **Module**: `statement.py`
    - **Action**: Fetches delegate statements, social links, and contact info.
    - **Output**: `{slug}_delegation_statements.csv` (and .json)

2.  **Stage 2: Basic Governance Analysis**
    - **Module**: `basic.py`
    - **Action**: Analyzes governors, quorum, token stats, and high-level metrics.
    - **Output**: `{slug}_governance_analysis_{id}.xlsx` (or .csv) and a summary markdown report.

3.  **Stage 3: Proposals Deep Dive**
    - **Module**: `prop.py`
    - **Action**: Fetches detailed proposal data, vote counts, timestamps, and execution status.
    - **Output**: `{slug}_proposals.csv`

4.  **Stage 4: Voting Matrix Construction**
    - **Module**: `voting_matrix.py`
    - **Action**: Builds a comprehensive matrix mapping every delegate's vote on every proposal to analyze participation.
    - **Output**: 
        - `{slug}_voting_matrix.csv` (Detailed vote log)
        - `{slug}_delegate_summary.csv` (Delegate participation stats)
        - `{slug}_proposal_analysis.csv` (Proposal-level stats)

All output files are saved in the `{DAO_Name}/` directory created automatically.

## 📂 Project Structure

```
tally_scrapy/
├── main.py                 # Unified runner script (Entry Point)
├── tally_scrapy/           # Package source code
│   ├── __init__.py         # Package exports
│   ├── basic.py            # Basic governance analyzer
│   ├── prop.py             # Proposal fetching module
│   ├── statement.py        # Delegation statement fetcher
│   └── voting_matrix.py    # Voting matrix builder
└── README.md               # Documentation
```

## 📊 Modules Description

- **`basic.py`**: Fetches high-level governance stats, proposals list, and delegates overview.
- **`prop.py`**: deep-dives into proposals, extracting vote counts, execution data, and timelines.
- **`statement.py`**: Retrieves delegate platform statements and contact info.
- **`voting_matrix.py`**: The heavy lifter—analyzes exactly how every delegate voted on every proposal to build a voting behavior matrix.
