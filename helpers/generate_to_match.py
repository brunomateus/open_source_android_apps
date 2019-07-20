import csv
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--all_repos", help="Path to the file that contains a list of packages extracted from AndroidManifest at Github", required=True)
parser.add_argument("--repos_at_play", help="Path to the file that contains a list of packages found at Google Play Store", required=True)
parser.add_argument(
        '--output', default=open('to_match.csv', 'w'),
        type=argparse.FileType('w'),
        help='Output. Default: to_match.csv')

args = parser.parse_args()

to_match_file = args.output

all_pkgs_file = open(args.all_repos, mode='r')
all_reader = csv.DictReader(all_pkgs_file, delimiter=',', fieldnames=['package','repo_name'])
all_repos = list(all_reader)
all_pkgs_file.close()

pkgs_on_play_file = open(args.repos_at_play, mode='r')
pkgs_reader = csv.DictReader(pkgs_on_play_file, delimiter=',', fieldnames=['package','repo_name'])
pkgs_on_play = list(pkgs_reader)
pkgs_on_play_file.close()


to_match = {}

i = 0

n_repos = len(pkgs_on_play)

pkgs = set()

for item in pkgs_on_play:
    i = i + 1
    pkg = item['package'].strip()

    if pkg not in pkgs:
        pkgs.add(pkg)
        for row in all_repos:
            if pkg == row['package'].strip():
                to_match.setdefault(pkg,[]).append(row['repo_name'].strip())
        workdone = i/n_repos
        print("\rProgress: [{0:50s}] {1:.1f}% {2}/{3}".format('#' * int(workdone * 50), workdone*100, i, n_repos), end='', flush=True)


to_match_file.write("package,all_repos\n")

for pkg, repos in to_match.items():
    formated_repos = ';'.join(repos)
    to_match_file.write("{},{}\n".format(pkg.strip(),formated_repos))


