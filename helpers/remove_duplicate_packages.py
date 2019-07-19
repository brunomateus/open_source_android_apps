import argparse
import csv
import logging

logging.basicConfig(level=logging.INFO,
        format='%(asctime)s | [%(levelname)s] : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

parser = argparse.ArgumentParser()
parser.add_argument("package_list",
        type=argparse.FileType('r'),
        help="Path to the file that contains a list of packages extracted from AndroidManifest at Github")

parser.add_argument(
        '--output', default=open('pkgs_one_manifest_repo', 'w'),
        type=argparse.FileType('w'),
        help='Log file. Default: pkgs_one_manifest_repo.')

args = parser.parse_args()
csv_reader = csv.reader(args.package_list, delimiter=',')
next(csv_reader, None)

lines = []
for row in csv_reader:
    lines.append("{}\n".format(row[0]))

n_lines = len(lines)

args.package_list.close()

logging.info("Extracting packages names")
logging.info("{} packages found.".format(n_lines))
logging.info("Removing duplicated packages")

uniq_lines = set(lines)
n_uniq = len(uniq_lines)
logging.info("{} packages remaining. {} packages duplicated removed".format(n_uniq, n_lines - n_uniq))

args.output.write(''.join(sorted(uniq_lines)))


