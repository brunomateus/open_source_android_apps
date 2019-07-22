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
parser.add_argument("--package_list",
        type=argparse.FileType('r'),
        help="The csv file that contins output the match process",
        required=True)

parser.add_argument("--details_dir",
        type=str,
        help="Folder containing json files that store google play metadata",
        required=True)


parser.add_argument(
        '--output', default=open('new_apps.json', 'w'),
        type=argparse.FileType('w'),
        help='Output file. Default: new_apps.json.')

args = parser.parse_args()
csv_reader = csv.reader(args.package_list, delimiter=',')

matched_pkgs = []
matched_dict = {}

for row in csv_reader:
    matched_pkgs.append(row[0].strip())
    matched_dict[row[0]] = "https://github.com/{}".format(row[1].strip())

args.package_list.close()

n_matched = len(matched_pkgs)
found = 0

details_dir = args.details_dir


result = []
for package_name, package_details in parse_package_details(details_dir):
    if package_name in matched_pkgs:
        logging.debug(package_name)

        found = found + 1

        workdone = found/n_matched
        print("\rProgress: [{0:50s}] {1:.1f}% {2}/{3}".format('#' * int(workdone * 50), workdone*100, found, n_matched), end='', flush=True)
       
        app_details = package_details.get('details').get('appDetails', None)
        
        relevant_info = {}

        if app_details:
            relevant_info['package'] = package_name
            relevant_info['name'] =  package_details.get('title')
            relevant_info['summary'] =  package_details.get('promotionalDescription')
            relevant_info['last_added_on'] =  str(datetime.strptime(app_details.get('uploadDate'), "%b %d, %Y").date())
            relevant_info['last_version_number'] =  app_details.get('versionCode')
            relevant_info['last_version_name'] =  app_details.get('versionString')
            relevant_info['source_repo'] =  matched_dict.get(package_name)
            result.append(relevant_info)
        else:
            logging.warning('Impossible to retrive details from package {}'.format(package_name)) 

        if n_matched == found:
            break


print(json.dumps(result, indent=4, sort_keys=False), file=args.output)
