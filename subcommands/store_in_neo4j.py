"""Store information in Neo4j graph database.

Use -h or --help for more information.
"""
import argparse
import csv
from datetime import datetime
import itertools
import json
import logging
import os
from typing import IO, Set, Tuple

from gitlab import Gitlab
from gitlab.v4.objects import Project

from util.bare_git import BareGit
from util.neo4j import Neo4j, Node
from util.parse import ParsedJSON
from util.parse import parse_repo_to_package_file


__log__ = logging.getLogger(__name__)


NEO4J_HOST = 'bolt://localhost'
NEO4J_PORT = 7687
GITLAB_HOST = 'http://145.108.225.21'
GITLAB_REPOSITORY_PATH = '/var/opt/gitlab/git-data/repositories/gitlab'


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


def format_google_play_info(json_file: IO[str]) -> dict:
    """Select and format data from json_file to store in node.

    :param IO[str] json_file:
        JSON file to read meta data from.
    :returns dict:
        Properties of a node represinting the Google Play page of an app.
    """
    meta_data = json.load(json_file)
    offer = meta_data.get('offer', {})
    details = meta_data.get('details', {})
    app_details = details.get('appDetails', {})

    return {
        'docId': meta_data.get('docId'),
        'uri': meta_data.get('shareUrl'),
        'snapshotTimestamp': os.stat(json_file).st_mtime,
        'title': meta_data.get('title'),
        'appCategory': app_details.get('appCategory'),
        'promotionalDescription': meta_data['promotionalDescription'],
        'descriptionHtml': meta_data['descriptionHtml'],
        'translatedDescriptionHtml': meta_data['translatedDescriptionHtml'],
        'versionCode': app_details.get('versionCode'),
        'versionString': app_details.get('versionString'),
        'uploadDate': parse_upload_date(app_details),
        'formattedAmount': offer.get('formattedAmount'),
        'currencyCode': offer.get('currencyCode'),
        'in-app purchases': describe_in_app_purchases(meta_data),
        'installNotes': app_details.get('installNotes'),
        'starRating': meta_data.get('aggregateRating', {}).get('starRating'),
        'numDownloads': app_details.get('numDownloads'),
        'developerName': app_details.get('developerName'),
        'developerEmail': app_details.get('developerEmail'),
        'developerWebsite': app_details.get('developerWebsite'),
        'targetSdkVersion': app_details.get('targetSdkVersion'),
        'permissions':  app_details.get('permission')
        }


def add_google_play_page_node(
        package_name: str, neo4j: Neo4j, play_details_dir: str) -> Node:
    """Create a node for an Google Play page.

    Meta data of Google Play page is loaded from JSON file at
    <play_details_dir>/<package_name>.json

    :param str package_name:
        Package name.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    :param str play_details_dir:
        Name of directory to include JSON files from. Filenames in this
        directory need to have .json extension. Filename without extension is
        assumed to be package name for details contained in file.
    :return Node:
        Node created for Google Play page if JSON file exists, otherwise None.
    """
    json_file_name = '{}.json'.format(package_name)
    json_file_path = os.path.join(play_details_dir, json_file_name)
    if not os.path.exists(json_file_path):
        __log__.warning(
            'Cannot create GooglePlayPage node: %s does not exist.',
            json_file_path)
        return None
    with open(json_file_path, 'r') as json_file:
        google_play_info = format_google_play_info(json_file)
    __log__.info('Create GooglePlayPage node from %s', json_file_path)
    return neo4j.create_node('GooglePlayPage', **google_play_info)


def format_repository_data(meta_data: dict, snapshot: str) -> dict:
    """Format repository data for insertion into Neo4j.

    :param dict meta_data:
        Meta data of Google Play Store page parses from JSON.
    :param str snapshot:
        String representation of snapshot time of repository mirror.
    :returns dict:
        A dictionary of properties of the node to create.
    """
    snapshot_time = datetime.strptime(snapshot, '%Y-%m-%dT%H:%M:%S.%fZ')
    return {
        'owner': meta_data['owner_login'],
        'name': meta_data['renamed_to'] or meta_data['name'],
        'snapshot': meta_data['clone_project_url'],
        'snapshotTimestamp': snapshot_time.timestamp(),
        'description': meta_data['description'],
        'createdAt': meta_data['created_at'],
        'forksCount': meta_data['forks_count'],
        'stargazersCount': meta_data['stargazers_count'],
        'subscribersCount': meta_data['subscribers_count'],
        'watchersCount': meta_data['watchers_count'],
        'networkCount': meta_data['network_count'],
        'ownerType': meta_data['owner_type'],
        'parentId': meta_data['parent_id'],
        'sourceId': meta_data['source_id']
        }


def add_repository_node(
        meta_data: dict, package_names: Set[str],
        snapshot: str, neo4j: Neo4j) -> Node:
    """Add a repository and link it to all apps imnplemented by it.

    Does not do anything if packages_names is empty or no :App node exists
    with a matching package name.

    :param dict meta_data:
        Meta data of Google Play Store page parses from JSON.
    :param Set[str] package_names:
        a set of package names implemented by this repository.
    :param str snapshot:
        String representation of snapshot time of repository mirror.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    :returns Node:
        The node created for the repository.
    """
    repo_data = format_repository_data(meta_data, snapshot)
    query = '''
        MATCH (app:App)
        WHERE app.id in {package_names}
        CREATE
            (app)
            -[:IMPLEMENTED_BY]->
            (repo:GitHubRepository {repo_properties}
        RETURN repo
        '''
    result = neo4j.run(
        query, package_name=package_names, repo_properties=repo_data)
    return result.single()[0]


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


def find_package_names(meta_data: dict, packages: dict) -> Set[str]:
    """Find package names implemented by repository.

    :param dict meta_data:
        Dictionary containing repository meta data. Needs to include
        `full_name`, `renamed_to` and `not_found`.
    :param Dict[str, Set[str]] packages:
        A mapping from repository name to set of package names in that
        repository.
    :returns Set[str]:
        a set of package names implemented by this repository.
    """
    original_repo_name, latest_repo_name = get_latest_repo_name(meta_data)
    __log__.info(
        'Original repo name: %s. Latest known repo name: %s',
        original_repo_name, latest_repo_name)
    return packages.get(latest_repo_name, packages.get(original_repo_name))


def add_tag_nodes(gitlab_project: Project, repo_node_id: int, neo4j: Neo4j):
    """Create nodes representing GIT tags of a repository.

    Creates a node for each tag and links it with the repository identified
    by repo_node_id and the commit the tag points to.

    :param gitlab.v4.object.Project gitlab_project:
        Gitlab project to retrieve tags from.
    :param int repo_node_id:
        ID of node the tags should be linked to.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    """
    for tag in gitlab_project.tags.list(all=True, as_list=False):
        parameters = {
            'commit_hash': tag.commit.id,
            'repo_id': repo_node_id,
            'tag_details': {
                'name': tag.name,
                'message': tag.message,
                },
            }

        neo4j.run(
            '''
            MERGE (commit:Commit {hash: {commit_hash}
            MATCH (repo:GitHubRepository)
            WHERE id(repo) = {repo_id}
            CREATE
                (:Tag {tag_details})-[:BELONGS_TO]->(repo),
            ''', **parameters)


def add_branche_nodes(
        gitlab_project: Project, repo_node_id: int, neo4j: Neo4j):
    """Create nodes representing GIT branches of a repository.

    Creates a node for each branch and links it with the repository identified
    by repo_node_id and the commit the branch points to.

    :param gitlab.v4.object.Project gitlab_project:
        Gitlab project to retrieve branches from.
    :param int repo_node_id:
        ID of node the branches should be linked to.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    """
    for branch in gitlab_project.branches.list(all=True, as_list=False):
        parameters = {
            'commit_hash': branch.commit.id,
            'repo_id': repo_node_id,
            'branch_details': {
                'name': branch.name,
                },
            }

        neo4j.run(
            '''
            MERGE (commit:Commit {hash: {commit_hash}
            MATCH (repo:GitHubRepository)
            WHERE id(repo) = {repo_id}
            CREATE
                (:Branch {branch_details})-[:BELONGS_TO]->(repo),
            ''', **parameters)


def add_commit_nodes(gitlab_project: Project, repo_node_id: int, neo4j: Neo4j):
    """Create nodes representing GIT commits of a repository.

    Creates a node for each commit and links it with  the repository identified
    by repo_node_id.

    Also creates relationships to author, committer and parent commits. Creates
    each of these in turn unless they exist already.

    :param gitlab.v4.object.Project gitlab_project:
        Gitlab project to retrieve commits from.
    :param int repo_node_id:
        ID of node the commits should be linked to.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    """
    for commit in gitlab_project.commits.list(all=True, as_list=False):
        parameters = {
            'commit_hash': commit.id,
            'author_email': commit.author_email,
            'committer_email': commit.committer_email,
            'repo_id': repo_node_id,
            'commit_details': {
                'id': commit.id,
                'short_id': commit.short_id,
                'title': commit.title,
                'message': commit.message,
                },
            'author_details': {
                'email': commit.author_email,
                'name': commit.author_name,
                },
            'committer_details': {
                'email': commit.committer_email,
                'name': commit.committer_name,
                },
            'authored_date': commit.authored_date,
            'committed_date': commit.committed_date,
            }

        neo4j.run(
            '''
            MERGE (commit:Commit {hash: {commit_hash}
            MERGE (author:Contributor {email: {author_email}})
            MERGE (committer:Contributor {email: {committer_email}})
            MATCH (repo:GitHubRepository)
            WHERE id(repo) = {repo_id}
            CREATE
                (commit {commit_details}),
                (author {author_details}),
                (committer {committer_details}),
                (commit)-[:BELONGS_TO]->(repo),
                (author)-[:AUTHORS {date: {authored_date}}]->(commit),
                (committer)-[:COMMITS {date: {committed_date}}]->(commit)
            ''', **parameters)

        for parent in commit.parent_ids:
            neo4j.run('MERGE (:Commit {hash: {parent}})', parent=parent)


def add_paths_property(
        properties: dict, repo_node_id: int, package_name: str, neo4j: Neo4j):
    """Add path names as properties based on search.

    Search a git repository and add file names which contain matches to an
    :IMPLEMENTED_BY relationship matched agains package_name and repoe_node_id.

    :param dict properties:
        Mapping of property name to propertie values to be added to
        relationship.
    :param int repo_node_id:
        Identifier for :GitHubRepository node which the :IMPLEMENTED_BY
        relationship points to.
    :param str package_name:
        Package name of :App node.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    """
    parameters = {
        'package': package_name,
        'repo_id': repo_node_id,
        'rel_properties': properties,
        }
    query = '''
        MATCH
            (:App {id: {package}})-[r:IMPLEMENTED_BY]->(repo:GitHubRepository)
        WHERE id(repo) = {repo_id}
        CREATE (r {rel_properties})
        '''
    neo4j.run(query, **parameters)


def find_paths(pattern: str, file_pattern: str, branch: str, git: BareGit):
    """Find files in GIT repository.

    :param str pattern:
        Search pattern.
    :param str file_pattern:
        Pathspec to restrict files matched in GIT repository.
    :param str branch:
        Refspec to base search in GIT repository on.
    :param BareGit git:
        GIT repository to search.
    :returns List[str]:
        list of path names.
    """
    search_results = git.grep(pattern, branch, file_pattern)
    paths = sorted(map(lambda m: m[1], search_results))
    groups = itertools.groupby(paths)
    return [group[0] for group in groups]


def add_manifest_path(
        repo_node_id: int, package_name: str, branch: str, git: BareGit,
        neo4j: Neo4j):
    """Add paths of AndroidManifest.xml files to :IMPLEMENTED_BY relationship.

    :param int repo_node_id:
        Identifier for :GitHubRepository node which the :IMPLEMENTED_BY
        relationship points to.
    :param str package_name:
        Package name of :App node.
    :param str branch:
        Refspec to base search in GIT repository on.
    :param BareGit git:
        GIT repository to search.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    """
    pattern = 'package="{}"'.format(package_name)
    paths = find_paths(pattern, '*AndroidManifest.xml', branch, git)
    if paths:
        __log__.info('Found manifests: %s', paths)
        add_paths_property(
            {'manifestPaths': paths}, repo_node_id, package_name, neo4j)


def add_gradle_config_path(
        repo_node_id: int, package_name: str, branch: str, git: BareGit,
        neo4j: Neo4j):
    """Add paths of gradle configuration files to :IMPLEMENTED_BY relationship.

    :param int repo_node_id:
        Identifier for :GitHubRepository node which the :IMPLEMENTED_BY
        relationship points to.
    :param str package_name:
        Package name of :App node.
    :param str branch:
        Refspec to base search in GIT repository on.
    :param BareGit git:
        GIT repository to search.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    """
    pattern = 'applicationId *.{}.'.format(package_name)
    paths = find_paths(pattern, '*build.gradle', branch, git)
    if paths:
        __log__.info('Found gradle files: %s', paths)
        add_paths_property(
            {'gradleConfigPaths': paths}, repo_node_id, package_name, neo4j)


def add_maven_config_path(
        repo_node_id: int, package_name: str, branch: str, git: BareGit,
        neo4j: Neo4j):
    """Add paths of Maven configuration files to :IMPLEMENTED_BY relationship.

    :param int repo_node_id:
        Identifier for :GitHubRepository node which the :IMPLEMENTED_BY
        relationship points to.
    :param str package_name:
        Package name of :App node.
    :param str branch:
        Refspec to base search in GIT repository on.
    :param BareGit git:
        GIT repository to search.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    """
    pattern = r'<groupId>{}<\/groupId>'.format(package_name)
    paths = find_paths(pattern, '*pom.xml', branch, git)
    if paths:
        __log__.info('Found maven files: %s', paths)
        add_paths_property(
            {'mavenConfigPaths': paths}, repo_node_id, package_name, neo4j)


def add_implementation_properties(
        project: Project, repo_node_id: int, meta_data: dict, packages: dict,
        neo4j: Neo4j):
    """Add properties to IMPLEMENTED_BY relationship.

    Find Android manifest files and build system files for app in the
    repository and add their paths as properties to the IMPLEMENTED_BY
    relationship.

    :param gitlab.v4.object.Project gitlab_project:
        Gitlab project to search.
    :param int repo_node_id:
        ID of node representing the repository.
    :param dict meta_data:
        Dictionary containing repository meta data.
    :param Dict[str, Set[str]] packages:
        A mapping from repository name to set of package names in that
        repository.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    """
    repository_path = os.path.join(
        GITLAB_REPOSITORY_PATH, meta_data['clone_project_name'])
    __log__.info('Use local git repository at %s', repository_path)
    git = BareGit(repository_path)
    for package in packages:
        add_manifest_path(
            repo_node_id, package, project.default_branch, git, neo4j)
        add_gradle_config_path(
            repo_node_id, package, project.default_branch, git, neo4j)
        add_maven_config_path(
            repo_node_id, package, project.default_branch, git, neo4j)


def add_repository_info(
        csv_file: IO[str], packages_by_repo: dict, neo4j: Neo4j,
        gitlab: Gitlab):
    """Add data of GIT repositories to Neo4j.

    :param IO[str] csv_file:
        CSV file containing meta data of repositories.
    :param dict packages_by_repo:
        A mapping from repository name to set of package names in that
        repository.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    :param Gitlab gitlab:
        Gitlab instance to query repository data from.
    """
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        if row['clone_status'] != 'Success':
            __log__.warning(
                'Project %s does not exist. Clone status: %s',
                row['full_name'], row['clone_status'])
            continue
        __log__.info('Create repo info')
        packages = find_package_names(row, packages_by_repo)
        __log__.info('Found packages: %s', packages)
        project = gitlab.projects.get(int(row['clone_project_id']))
        node = add_repository_node(row, packages, project.created_at, neo4j)
        __log__.info('Created :GitHubRepository node with id %d', node.id)
        add_commit_nodes(project, node.id, neo4j)
        __log__.info('Created :Commit nodes')
        add_branche_nodes(project, node.id, neo4j)
        __log__.info('Created :Branch nodes')
        add_tag_nodes(project, node.id, neo4j)
        __log__.info('Created :Tag nodes')
        add_implementation_properties(project, node.id, row, packages, neo4j)


def add_app_data(packages_by_repo: dict, play_details_dir: str, neo4j: Neo4j):
    """Create nodes and relationships for Android apps.

    :param dict packages_by_repo:
        A mapping from repository name to set of package names in that
        repository.
    :param str play_details_dir:
        Name of directory to include JSON files from. Filenames in this
        directory need to have .json extension. Filename without extension is
        assumed to be package name for details contained in file.
    :param Neo4j neo4j:
        Neo4j instance to add nodes to.
    """
    for packages in packages_by_repo.values():
        __log__.info(
            'Add :GooglePlayPage and :App nodes for packages: %s', packages)
        for package in packages:
            add_google_play_page_node(package, neo4j, play_details_dir)
            neo4j.run(
                '''MERGE (g:GooglePlayPage {docId: {package}})
                CREATE (a:App {id: {package}})-[:PUBLISHED_AT]->(g)''',
                package=package)


def define_cmdline_arguments(parser: argparse.ArgumentParser):
    """Add arguments to parser."""
    parser.add_argument(
        'PLAY_STORE_DETAILS_DIR', type=str,
        help='Directory containing JSON files with details from Google Play.')
    parser.add_argument(
        'PACKAGE_LIST', type=argparse.FileType('r'),
        help='''CSV file that lists package name and repository name in
            a column each. The file should not have a header.''')
    parser.add_argument(
        'REPOSITORY_LIST', type=argparse.FileType('r'),
        help='''CSV file that lists meta data for repositories and their
        snapshots on Gitlab.''')
    parser.add_argument(
        '--gitlab-repos-dir', type=str, default=GITLAB_REPOSITORY_PATH,
        help='''Local path to repositories of Gitlab user `gitlab`. Default:
        {}'''.format(GITLAB_REPOSITORY_PATH))
    parser.add_argument(
        '--gitlab-host', type=str, default=GITLAB_HOST,
        help='''Hostname Gitlab instance is running on. Default:
        {}'''.format(GITLAB_HOST))
    parser.add_argument(
        '--neo4j-host', type=str, default=NEO4J_HOST,
        help='''Hostname Neo4j instance is running on. Default:
        {}'''.format(NEO4J_HOST))
    parser.add_argument(
        '--neo4j-port', type=int, default=NEO4J_PORT,
        help='Port number of Neo4j instance. Default: {}'.format(NEO4J_PORT))
    parser.set_defaults(func=_main)


def _main(args: argparse.Namespace):
    """Pass arguments to respective function."""
    __log__.info('------- Arguments: -------')
    __log__.info('PLAY_STORE_DETAILS_DIR: %s', args.PLAY_STORE_DETAILS_DIR)
    __log__.info('PACKAGE_LIST: %s', args.PACKAGE_LIST.name)
    __log__.info('REPOSITORY_LIST: %s', args.REPOSITORY_LIST.name)
    __log__.info('--gitlab-repos-dir: %s', args.gitlab_repos_dir)
    __log__.info('--gitlab-host: %s', args.gitlab_host)
    __log__.info('--neo4j-host: %s', args.neo4j_host)
    __log__.info('--neo4j-port: %d', args.neo4j_port)
    __log__.info('------- Arguments end -------')

    neo4j_user = os.getenv('NEO4J_USER')
    __log__.info('Use `%s` to login to Neo4j', neo4j_user)
    neo4j_password = os.getenv('NEO4J_PASSWORD')
    __log__.info('Read Neo4j password from environment')

    gitlab = Gitlab(args.gitlab_host, api_version=4)

    with Neo4j(NEO4J_HOST, neo4j_user, neo4j_password, NEO4J_PORT) as neo4j:
        packages_by_repo = parse_repo_to_package_file(args.PACKAGE_LIST)
        __log__.info(
            'Read packages in %d repos from %s', len(packages_by_repo),
            args.PACKAGE_LIST)
        add_app_data(packages_by_repo, args.PLAY_STORE_DETAILS_DIR, neo4j)
        add_repository_info(
            args.REPOSITORY_LIST, packages_by_repo, neo4j, gitlab)
