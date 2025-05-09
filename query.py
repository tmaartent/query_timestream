#!/usr/bin/env python3
"""
Simple script to run Amazon Timestream Query using boto3.
Usage:
  python timestream_query.py --query "SELECT * FROM \"db\".\"table\" LIMIT 10" [--region us-east-1] [--profile default] [--output results.json]

This script also includes a section to track manual steps you've taken during development.
"""

import argparse
import boto3
import os
import sys
import json


def parse_args():
    parser = argparse.ArgumentParser(description="Run a Timestream Query")
    parser.add_argument(
        '--query', '-q', required=True,
        help='Timestream SQL query string'
    )
    parser.add_argument(
        '--region', '-r', default=os.getenv('AWS_REGION', 'us-east-1'),
        help='AWS region'
    )
    parser.add_argument(
        '--profile', '-p', default=None,
        help='AWS CLI profile to use (optional)'
    )
    parser.add_argument(
        '--output', '-o', default=None,
        help='Path to output JSON file (optional)'
    )
    return parser.parse_args()


def get_client(region, profile=None):
    """
    Create a boto3 TimestreamQuery client.
    """
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
    else:
        session = boto3.Session(region_name=region)
    return session.client('timestream-query')


def parse_row(row, column_info):
    """
    Convert a Timestream Row into a dict of column_name: value.
    """
    result = {}
    for info, data in zip(column_info, row.get('Data', [])):
        col_name = info.get('Name')
        result[col_name] = data.get('ScalarValue')
    return result


def run_query(client, query_string):
    """
    Execute the query, handling pagination, and return list of row dicts.
    """
    next_token = None
    all_rows = []

    while True:
        params = {'QueryString': query_string}
        if next_token:
            params['NextToken'] = next_token
        response = client.query(**params)

        column_info = response.get('ColumnInfo', [])
        for row in response.get('Rows', []):
            all_rows.append(parse_row(row, column_info))

        next_token = response.get('NextToken')
        if not next_token:
            break

    return all_rows


def main():
    args = parse_args()
    client = get_client(args.region, args.profile)

    try:
        rows = run_query(client, args.query)
    except Exception as e:
        print(f"Error running query: {e}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(rows, f, indent=2)
        print(f"Results written to {args.output}")
    else:
        print(json.dumps(rows, indent=2))


if __name__ == '__main__':
    main()

# Steps Record:
# 1. Using AWS CloudShell to test queries against the aggregate table
