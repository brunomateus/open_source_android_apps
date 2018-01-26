"""Parse intermediary files for further processing."""

import csv
from datetime import datetime
import glob
import json
import logging
import os
from typing import \
    Dict, \
    Generator, \
    IO, \
    List, \
    Mapping, \
    Sequence, \
    Set, \
    Text, \
    Tuple, \
    Union


__log__ = logging.getLogger(__name__)

ParsedJSON = Union[  # pylint: disable=C0103
    Mapping[Text, 'ParsedJSON'], Sequence['ParsedJSON'], Text, int, float,
    bool, None]


def parse_package_to_repos_file(input_file: IO[str]) -> Dict[str, List[str]]:
    """Parse CSV file mapping package names to repositories.

    :param IO[str] input_file: CSV file to parse.
        The file needs to contain a column `package` and a column
        `all_repos`. `all_repos` contains a comma separated string of
        Github repositories that include an AndroidManifest.xml file for
        package name in column `package`.
    :returns Dict[str, List[str]]: A mapping from package name to
        list of repository names.
    """
    return {
        row['package']: row['all_repos'].split(',')
        for row in csv.DictReader(input_file)
        }


def parse_package_details(details_dir: str) -> Generator[
        Tuple[str, ParsedJSON], None, None]:
    """Parse all JSON files in details_dir.

    Filenames need to have .json extension. Filename without extension is
    assumed to be package name for details contained in file.

    :param str details_dir: Directory to include JSON files from.
    :returns Generator[Tuple[str, ParsedJSON]]: Generator over tuples of
        package name and parsed JSON.
    """
    for path in glob.iglob('{}/*.json'.format(details_dir)):
        if os.path.isfile(path):
            with open(path, 'r') as details_file:
                filename = os.path.basename(path)
                package_name = os.path.splitext(filename)[0]
                package_details = json.load(details_file)
                yield package_name, package_details


def invert_mapping(packages: Mapping[str, Sequence[str]]) -> Dict[
        str, Set[str]]:
    """Create mapping from repositories to package names.

    :param Mapping[str, Sequence[str]] packages: Mapping of package names to
        a list of repositories.
    :returns Dict[str, Set[str]]: Mapping of repositories to set of package
        names.
    """
    result = {}
    for package, repos in packages.items():
        for repo in repos:
            result.setdefault(repo, set()).add(package)
    return result


def parse_repo_to_package_file(input_file: IO[str]) -> Dict[str, Set[str]]:
    """Parse CSV file mapping a repository name to a package name.

    :param IO[str] input_file:
        CSV file to parse. First column of the file needs to contain package
        names. The second column contains the corresponding repository name.
    :returns Dict[str, Set[str]]:
        A mapping from repository name to set of package names in that
        repository.
    """
    result = {}
    for row in csv.reader(input_file):
        result.setdefault(row[1], set()).add(row[0])
    return result


def describe_in_app_purchases(meta_data: ParsedJSON) -> str:
    """Find description of in-app purchases.

    :param dict meta_data:
        Meta data of Google Play Store page parses from JSON.
    :returns str:
        Description of in-app purchases if it exists, otherwise None.
    """
    product_details_sections = meta_data.get('productDetails', {}).get(
        'section', [])
    for section in product_details_sections:
        if section['title'] == 'In-app purchases':
            return section['description'][0]['description']
    return None


def parse_upload_date(app_details: ParsedJSON) -> float:
    """Parse upload date to POSIX timestamp

    :param dict app_details:
        App details section of meta data of Google Play Store page parses
        from JSON.
    :returns float:
        POSIX timestampt of upload date.
    """
    upload_date_string = app_details.get('uploadDate')
    if upload_date_string:
        return datetime.strptime(
            upload_date_string, '%b %d, %Y').timestamp()
    return None


def parse_google_play_info(package_name: str, play_details_dir: str) -> dict:
    """Select and format data from json_file to store in node.

    :param str package_name:
        Package name.
    :param str play_details_dir:
        Name of directory to include JSON files from. Filenames in this
        directory need to have .json extension. Filename without extension is
        assumed to be package name for details contained in file.
    :returns dict:
        Properties of a node represinting the Google Play page of an app.
    """
    def _parse_json_file(prefix: str) -> Tuple[dict, float]:
        """Return parsed JSON and mdate

        Uses prefix and package_name (from outer scope) to build path.
        """
        json_file_name = '{}.json'.format(package_name)
        json_file_path = os.path.join(prefix, json_file_name)
        if not os.path.exists(json_file_path):
            __log__.warning('Cannot read file: %s.', json_file_path)
            return {}, None
        with open(json_file_path) as json_file:
            return json.load(json_file), os.stat(json_file_path).st_mtime

    meta_data, mtime = _parse_json_file(play_details_dir)
    category_data, category_mtime = _parse_json_file(os.path.join(
        play_details_dir, 'categories'))
    if not meta_data and not category_data:
        return None
    if not meta_data:
        meta_data = {'docId': package_name}
        mtime = category_mtime
    offer = meta_data.get('offer', [])
    if offer:
        formatted_amount = offer[0].get('formattedAmount')
        currency_code = offer[0].get('currencyCode')
    else:
        formatted_amount = None
        currency_code = None
    details = meta_data.get('details', {})
    app_details = details.get('appDetails', {})
    if category_data:
        categories = app_details.setdefault('appCategory', [])
        categories.append(category_data['appCategory'])
    aggregate_rating = meta_data.get('aggregateRating')
    if not aggregate_rating:
        aggregate_rating = {}

    return {
        'docId': meta_data.get('docId'),
        'uri': meta_data.get('shareUrl'),
        'snapshotTimestamp': mtime,
        'title': meta_data.get('title'),
        'appCategory': app_details.get('appCategory'),
        'promotionalDescription': meta_data['promotionalDescription'],
        'descriptionHtml': meta_data['descriptionHtml'],
        'translatedDescriptionHtml': meta_data['translatedDescriptionHtml'],
        'versionCode': app_details.get('versionCode'),
        'versionString': app_details.get('versionString'),
        'uploadDate': parse_upload_date(app_details),
        'formattedAmount': formatted_amount,
        'currencyCode': currency_code,
        'in-app purchases': describe_in_app_purchases(meta_data),
        'installNotes': app_details.get('installNotes'),
        'starRating': aggregate_rating.get('starRating'),
        'numDownloads': app_details.get('numDownloads'),
        'developerName': app_details.get('developerName'),
        'developerEmail': app_details.get('developerEmail'),
        'developerWebsite': app_details.get('developerWebsite'),
        'targetSdkVersion': app_details.get('targetSdkVersion'),
        'permissions':  app_details.get('permission')
        }


def get_latest_repo_name(meta_data: dict) -> Tuple[str, str]:
    """Determine the most recently used repository name.

    :param dict meta_data:
        Dictionary containing repository meta data. Needs to include
        `full_name`, `renamed_to` and `not_found`.
    :returns Tuple[str, str]:
        Tuple of original repository name and latest known repository
        name if available, otherwise None.
    """
    original_repo = meta_data['full_name']
    renamed_to = meta_data['renamed_to']
    not_found = meta_data['not_found'] == 'TRUE'
    if renamed_to:
        return original_repo, renamed_to
    elif not not_found:
        return original_repo, original_repo
    return original_repo, None
