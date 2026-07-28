# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
KQL Script Runner
-----------------
Executes KQL commands against Fabric Eventhouse from the command line.

Usage:
    # Run a single query
    python scripts/run_kql.py --query "raw_events | count"

    # Run a .kql file (executes each query separated by blank lines)
    python scripts/run_kql.py --file kql_queries/materialized_views.kql

    # Run a management command
    python scripts/run_kql.py --query ".show materialized-views"

Requires:
    1. az login (one-time Azure CLI authentication)
    2. KUSTO_URI and KUSTO_DATABASE in .env file
"""

import argparse
import os
import sys

from dotenv import load_dotenv
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table

load_dotenv()

KUSTO_URI = os.getenv("KUSTO_URI", "")
KUSTO_DATABASE = os.getenv("KUSTO_DATABASE", "")


def get_client() -> KustoClient:
    """Create a KustoClient authenticated via Azure CLI."""
    if not KUSTO_URI:
        print("ERROR: KUSTO_URI not set in .env")
        print("  Go to Fabric → Eventhouse → copy the Query URI")
        sys.exit(1)

    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(KUSTO_URI)
    return KustoClient(kcsb)


def execute_query(client: KustoClient, query: str, is_management: bool = False):
    """Execute a single KQL query and print results."""
    query = query.strip()
    if not query or query.startswith("//"):
        return

    print(f"\n{'='*60}")
    print(f"Query: {query[:100]}{'...' if len(query) > 100 else ''}")
    print(f"{'='*60}")

    try:
        if is_management or query.startswith("."):
            response = client.execute_mgmt(KUSTO_DATABASE, query)
        else:
            response = client.execute(KUSTO_DATABASE, query)

        for table in response.primary_results:
            df = dataframe_from_result_table(table)
            if len(df) == 0:
                print("  (no results)")
            else:
                print(df.to_string(index=False))
            print(f"\n  ({len(df)} rows)")

    except KustoServiceError as e:
        print(f"  ❌ KQL Error: {e}")
    except Exception as e:
        print(f"  ❌ Error: {e}")


def run_file(client: KustoClient, filepath: str):
    """Execute all queries in a .kql file, separated by blank lines."""
    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}")
        sys.exit(1)

    with open(filepath, "r") as f:
        content = f.read()

    # Split on double newlines (blank line separates queries)
    # Also handle queries separated by semicolons
    queries = []
    current = []
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped == "" and current:
            queries.append("\n".join(current))
            current = []
        elif not stripped.startswith("//"):
            current.append(line)
    if current:
        queries.append("\n".join(current))

    print(f"Found {len(queries)} queries in {filepath}")

    for i, query in enumerate(queries, 1):
        query = query.strip()
        if query:
            print(f"\n--- Query {i}/{len(queries)} ---")
            execute_query(client, query)


def main():
    parser = argparse.ArgumentParser(description="Run KQL against Fabric Eventhouse")
    parser.add_argument("--query", "-q", help="Single KQL query to execute")
    parser.add_argument("--file", "-f", help="Path to .kql file to execute")
    args = parser.parse_args()

    if not args.query and not args.file:
        print("Provide --query or --file")
        sys.exit(1)

    client = get_client()

    if args.query:
        execute_query(client, args.query)
    elif args.file:
        run_file(client, args.file)


if __name__ == "__main__":
    main()
