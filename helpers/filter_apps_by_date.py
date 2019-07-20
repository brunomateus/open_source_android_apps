#!/usr/bin/env python3

import argparse
import csv
import logging
import json
from util.parse import parse_package_details, parse_package_to_repos_file
from datetime import datetime

logging.basicConfig(level=logging.INFO,
        format='%(asctime)s | [%(levelname)s] : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

parser = argparse.ArgumentParser()
parser.add_argument("--start_date",
        type=lambda d: datetime.strptime(d, '%Y-%m-%d'),
        help="Upload date. Only apps upload after this date will be retrived. Format required: YYYY-MM-DD",
        required=True)

parser.add_argument("--all_repos",
        type=argparse.FileType('r'),
        help="A CSV file containg with following format: package_name,repository_name",
        required=True)

parser.add_argument("--details_dir",
        type=str,
        help="Folder containing json files that store google play metadata",
        required=True)


parser.add_argument(
        '--output', default=open('filtered_pkgs', 'w'),
        type=argparse.FileType('w'),
        help='Output file. Default: filtered_pkgs')

args = parser.parse_args()


details_dir = args.details_dir

all_files =  parse_package_details(details_dir)
analyzed = 0
pkgs = []
spinner = "/-\|"

logging.debug("Retriving applicatios released after: {}".format(args.start_date))

for package_name, package_details in all_files:
    logging.debug(package_name)

    analyzed = analyzed + 1
    print("\r {} {} apps analyzed".format(spinner[analyzed % 4], analyzed), end='', flush=True)
   
    if package_details:
        app_details = package_details.get('details').get('appDetails', None)
  
        upload_date = datetime.strptime(app_details.get('uploadDate'), "%b %d, %Y")

        if upload_date > args.start_date:
            pkgs.append(package_name)


result = []


all_repos_file = args.all_repos
all_repos_reader = csv.DictReader(all_repos_file, delimiter=',', fieldnames=['package','repo_name'])
all_repos = list(all_repos_reader)

print("\nRetriving all possible repos")

total = len(all_repos)
i = 0
for row in all_repos:
    i = i + 1

    workdone = i/total
    print("\rProgress: [{0:50s}] {1:.1f}% {2}/{3}".format('#' * int(workdone * 50), workdone*100, i, total), end='', flush=True)

    if row['package'] in pkgs:
        result.append("{},{}\n".format(row['package'], row['repo_name']))

args.output.write(''.join(result))
