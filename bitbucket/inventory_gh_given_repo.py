import os
import csv
import urllib.request
import requests
import json
from datetime import datetime
#from dateutil.parser import parse
import argparse
from urllib.parse import urlparse, parse_qs
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import threading
import concurrent.futures
import random
import sys

# Global variables
repo_data = []  # List to store the data for each repository
progress_emojis = ["⏳", "⌛", "⏳", "⌛"]
progress_asci = ["/", "/", "\\", "\\"]
api_url = "https://api.github.com" # can be overridden by the --server argument
api_call_counter = 0
previous_msg_length = 0
counter_lock = threading.Lock()
msg_lock = threading.Lock()
rate_limit_lock = threading.Lock()
repo_data_lock = threading.Lock()

def supports_emojis():
    try:
        print("⏳", end='', flush=True)
        return True
    except:
        return False

emoji_support = supports_emojis()

def store_repo_data(repo_name, data):
    global repo_data
    with repo_data_lock:
        # add data to dictionary if it does not exist
        # otherwise update the existing data
        repo_data.append(data)
        # if repo_name not in repo_data:
        #     repo_data[repo_name] = data
        # else:
        #     repo_data[repo_name].update(data)

def set_previous_msg_length(length):
    global previous_msg_length
    with msg_lock:
        previous_msg_length = max(length + 5,previous_msg_length)

def increment_api_counter(func):
    def wrapper(*args, **kwargs):
        global api_call_counter
        try:
            result = func(*args, **kwargs)
            with counter_lock:
                api_call_counter += 1
            return result
        except Exception as e:
            raise
    return wrapper

def print_msg(msg):
    global previous_msg_length
    print(f"\r{' ' * previous_msg_length}\r{msg}", end="")
    set_previous_msg_length(len(msg))

def get_last_page(response):
    try:
        # Get the URL for the last page of hooks
        last_page_url = response.links['last']['url']
    except Exception:
        return len(response.json())

    # Parse the URL and extract the page number
    parsed_url = urlparse(last_page_url)
    page_number = parse_qs(parsed_url.query)['page'][0]

    return int(page_number)

def get_pat():
    if not hasattr(get_pat, "index"):
        # first time this function is called, initialize the variables
        get_pat.index = -1
        get_pat.pats = os.getenv('GH_SOURCE_PAT').split(',')
    get_pat.index = (get_pat.index + 1) % len(get_pat.pats)
    #print (f"Using PAT {get_pat.index} of {len(get_pat.pats)} - {get_pat.pats[get_pat.index]}")
    return get_pat.pats[get_pat.index]

def get_remaining_api_calls(org_name):
    global api_url

    if not api_url.endswith('/api/v3'):
        # we are using github.com APIs
        url = f"{api_url}/rate_limit"
        headers = {"Authorization": f"token {get_pat()}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return int(response.json()['rate']['remaining']), int(response.json()['rate']['reset'])
    else:
        return 0, 0

def wait_for_rate_limit(org_name):
    with rate_limit_lock:
        while get_remaining_api_calls(org_name)[0] < 10:
            reset_time = datetime.fromtimestamp(get_remaining_api_calls(org_name)[1])
            time_to_reset = reset_time - datetime.now()
            print(f"Waiting for rate limit to reset. Time to reset: {time_to_reset}")
            time.sleep(time_to_reset.total_seconds())

def get_remaining_calls(header):
    try:
        return int(header['X-RateLimit-Remaining'])
    except Exception:
        return 0

def parse_link_header(header):
    links = {}
    if header:
        link_parts = header.split(',')
        for part in link_parts:
            url, rel = part.split('; ')
            url = url.strip(' ')
            url = url.strip('<>')
            rel = rel.split('=')[1].strip('"')
            links[rel] = url
    return links

def get_current_datetime():
    return datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

def format_size(size):
    return f"{size:.1f}" if size > 1 else f"{int(size)}"

def csv_to_md_code_section(file_path):
    with open(file_path, 'r') as file:
        csv_content = file.read().replace(',','\t')
    return f"```\n{csv_content}\n```"

def csv_to_xlsx(csv_file, xlsx_file):
    # Read the tab-delimited file
    df = pd.read_csv(csv_file, delimiter=',')

    # Write the DataFrame to an Excel file
    df.to_excel(xlsx_file, index=False)

def csv_to_md_table(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)
        data = list(reader)

    md_table = '| ' + ' | '.join(headers) + ' |\n'
    md_table += '| ' + ' | '.join(['---'] * len(headers)) + ' |\n'

    for row in data:
        md_table += '| ' + ' | '.join(row) + ' |\n'

    return md_table

def comment_on_issue(issue_repo, issue_number, code_body, intro_text=None):
    global api_url
    url = f"{api_url}/repos/{issue_repo}/issues/{issue_number}/comments"
    headers = {
        "Authorization": f"token {os.getenv('GITHUB_TOKEN')}",
        "Accept": "application/vnd.github.v3+json",
    }
    data = {
        "body": f"{intro_text}\n\n{code_body}" if intro_text else code_body
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()

# def get_latest_workflow_run(org_name, repo_name):
#     url = f"https://api.github.com/repos/{org_name}/{repo_name}/actions/runs"
#     headers = {
#         "Authorization": f"token {get_pat()}",
#         "Accept": "application/vnd.github.v3+json",
#     }
#     params = {
#         "per_page": 1
#     }
#     response = requests.get(url, headers=headers, params=params)
#     response.raise_for_status()
#     #print(f"Remaining calls: {get_remaining_calls(response.headers)}")
#     runs = response.json()['workflow_runs']

#     if not runs:
#         return None
#     else:
#         return runs[0]['created_at']

#     #latest_run = max(runs, key=lambda run: parse(run['created_at']))
#     #return latest_run['created_at']

@increment_api_counter
def count_webhooks(org_name, repo_name):
    global api_url
    url = f"{api_url}/repos/{org_name}/{repo_name}/hooks"

    headers = {
        "Authorization": f"token {get_pat()}",
        "Accept": "application/vnd.github.v3+json",
    }
    params = {
        "per_page": 1
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
    except Exception:
        return 0

    return int(get_last_page(response))

# def count_packages(org_name, repo_name):
#     url = f"https://api.github.com/repos/{org_name}/{repo_name}/packages"
#     headers = {
#         "Authorization": f"token {get_pat()}",
#         "Accept": "application/vnd.github.v3+json",
#     }
#     params = {
#         "per_page": 1
#     }
#     try:
#         response = requests.get(url, headers=headers, params=params)
#         response.raise_for_status()
#     except Exception:
#         return 0

#     return int(get_last_page(response))


# def count_branches(org_name, repo_name):
#     url = f"https://api.github.com/repos/{org_name}/{repo_name}/branches"
#     headers = {
#         "Authorization": f"token {get_pat()}",
#         "Accept": "application/vnd.github.v3+json",
#     }
#     params = {
#         "per_page": 1
#     }
#     try:
#         response = requests.get(url, headers=headers, params=params)
#         response.raise_for_status()
#     except Exception:
#         return 0

#     return int(get_last_page(response))

# def count_prs(org_name, repo_name):
#     url = f"https://api.github.com/repos/{org_name}/{repo_name}/pulls"
#     headers = {
#         "Authorization": f"token {get_pat()}",
#         "Accept": "application/vnd.github.v3+json",
#     }
#     params = {
#         "state": "all",
#         "per_page": 1
#     }
#     try:
#         response = requests.get(url, headers=headers, params=params)
#         response.raise_for_status()
#     except Exception:
#         return 0

#     return int(get_last_page(response))

# def count_commits(org_name, repo_name):
#     url = f"https://api.github.com/repos/{org_name}/{repo_name}/commits"
#     headers = {
#         "Authorization": f"token {get_pat()}",
#         "Accept": "application/vnd.github.v3+json",
#     }
#     params = {
#         "per_page": 1
#     }
#     try:
#         response = requests.get(url, headers=headers, params=params)
#         response.raise_for_status()
#     except Exception:
#         return 0

#     return int(get_last_page(response))

@increment_api_counter
def fetch_repo_data(org_name, repo_name):
    global api_url
    url = f"{api_url}/graphql"
    headers = {
        "Authorization": f"Bearer {get_pat()}",
        "Accept": "application/vnd.github.v3+json",
    }
    query = """
    query($org_name: String!, $repo_name: String!) {
        repository(owner: $org_name, name: $repo_name) {
            defaultBranchRef {
                name
            }
            workflowRuns: object(expression: "HEAD") {
                ... on Commit {
                    history(first: 1) {
                        nodes {
                            committedDate
                        }
                    }
                }
            }

            packages: packages(first: 1) {
                totalCount
            }
            branches: refs(refPrefix: "refs/heads/", first: 1) {
                totalCount
            }
            pullRequests: pullRequests(states: [OPEN, CLOSED, MERGED], first: 1) {
                totalCount
            }
            commits: object(expression: "HEAD") {
                ... on Commit {
                    history {
                        totalCount
                    }
                }
            }

            commitcomments: commitComments(first: 1) {
                totalCount
            }

            environments: environments(first: 1) {
                totalCount
            }

            issues: issues(first: 1) {
                totalCount
            }
        }
    }
    """
    variables = {
        "org_name": org_name,
        "repo_name": repo_name
    }

    try:
        response = requests.post(url, json={'query': query, 'variables': variables}, headers=headers)
        response.raise_for_status()
        #print (response.json())
        return response.json()
    except Exception as e:
        print (f"Error: {e}")
        return None

def parse_graphql_data(data):
    if (data is None):
        return {
            "default_branch": "Error",
            "latest_workflow_run": "Error",
            "packages_count": "Error",
            "branches_count": "Error",
            "pull_requests_count": "Error",
            "commits_count": "Error",
            "coommitcomments_count": "Error",
            "environments_count": "Error",
            "issues_count": "Error"
        }

    # assign default values if value does not exist in json
    repo_data = data['data']['repository']
    default_branch = repo_data['defaultBranchRef']['name'] if repo_data.get('defaultBranchRef') else None
    latest_workflow_run = repo_data['workflowRuns']['history']['nodes'][0]['committedDate'] if repo_data['workflowRuns'] else None
    #webhooks_count = repo_data['webhooks']['totalCount']
    packages_count = repo_data['packages']['totalCount']
    branches_count = repo_data['branches']['totalCount']
    pull_requests_count = repo_data['pullRequests']['totalCount']
    commits_count = repo_data['commits']['history']['totalCount'] if repo_data["commits"] else None

    return {
        "default_branch": default_branch,
        "latest_workflow_run": latest_workflow_run,
        #"webhooks_count": webhooks_count,
        "packages_count": packages_count,
        "branches_count": branches_count,
        "pull_requests_count": pull_requests_count,
        "commits_count": commits_count,
        "coommitcomments_count": repo_data['commitcomments']['totalCount'],
        "environments_count": repo_data['environments']['totalCount'],
        "issues_count": repo_data['issues']['totalCount']
    }

@increment_api_counter
def openurl(req):
    return urllib.request.urlopen(req)

def scan_repo(repo: dict, deep_scan: bool):
    repo_data = {
        "owner_login": repo['owner']['login'],
        "repo_name": repo['name'],
        "full_name": repo['full_name'],
        "size_kb": repo['size'],
        "size_mb": repo['size'] / 1024,
        "size_gb": (repo['size'] / 1024) / 1024,
        "updated_at": repo['updated_at'],
        "updated_at_days": (datetime.now() - datetime.strptime(repo['updated_at'], '%Y-%m-%dT%H:%M:%SZ')).days if repo['updated_at'] else None,
        "pushed_at": repo['pushed_at'],
        "pushed_at_days": (datetime.now() - datetime.strptime(repo['pushed_at'], '%Y-%m-%dT%H:%M:%SZ')).days if repo['pushed_at'] else None,
        "privacy": 'Private' if repo['private'] else 'Public',
        "archived": repo['archived'],
        "language": repo['language'],
        "fork": repo['fork'],
        "forks_count": repo['forks_count'],
        "has_issues": repo['has_issues'],
        "has_projects": repo['has_projects'],
        "has_wiki": repo['has_wiki'],
        "has_pages": repo['has_pages']
    }

    if deep_scan:
        #data = fetch_repo_data(source_org, repo_name)
        #parsed_data = parse_repo_data(data)

        with ThreadPoolExecutor() as repo_executor:
            graphql_data = repo_executor.submit(fetch_repo_data, repo_data['owner_login'], repo_data['repo_name'])
            webhooks = repo_executor.submit(count_webhooks, repo_data['owner_login'], repo_data['repo_name'])

        try:
            repo_graphql_data = graphql_data.result()
        except Exception as e:
            parsed_data = parse_graphql_data(None)

        parsed_data = parse_graphql_data(repo_graphql_data)

        repo_data.update(parsed_data)
        repo_data['webhooks'] = webhooks.result()

        """
        with ThreadPoolExecutor() as executor:
            last_run = executor.submit(get_latest_workflow_run, source_org, repo_name)
            commits = executor.submit(count_commits, source_org, repo_name)
            pull_requests = executor.submit(count_prs, source_org, repo_name)
            webhooks = executor.submit(count_webhooks, source_org, repo_name)
            branches = executor.submit(count_branches, source_org, repo_name)
            packages = executor.submit(count_packages, source_org, repo_name)

        # Get the results of the futures
        last_run = last_run.result()
        commits = commits.result()
        pull_requests = pull_requests.result()
        webhooks = webhooks.result()
        branches = branches.result()
        packages = packages.result()
        """
    #     last_run = parsed_data['latest_workflow_run']
    #     commits = parsed_data['commits_count']
    #     pull_requests = parsed_data['pull_requests_count']
    #     #webhooks = parsed_data['webhooks_count']
    #     webhooks = webhooks.result()
    #     branches = parsed_data['branches_count']
    #     packages = parsed_data['packages_count']
    #     commitcomments = parsed_data['coommitcomments_count']
    #     environments = parsed_data['environments_count']
    #     issues = parsed_data['issues_count']
    # else:
    #     last_run = None
    #     commits = None
    #     pull_requests = None
    #     webhooks = None
    #     branches = None
    #     packages = None
    #     commitcomments = None
    #     environments = None
    #     issues = None

    return repo_data

def process_repo(repo : dict, deep_scan : bool):
    global repo_data
    global progress_emojis

    if emoji_support:
        emoji = random.choice(progress_emojis)
    else:
        emoji = random.choice(progress_asci)

    print_msg(f"{emoji} Scanning repo #{len(repo_data) + 1}: {repo['name']}")
    store_repo_data(repo['name'], scan_repo(repo, deep_scan))

def main():
    # Create the parser
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument('--source_org', help='The source organization')
    parser.add_argument('--output_file', help='The output file name')
    parser.add_argument('--issue_number', help='The issue number')
    parser.add_argument('--issue_repo', help='The issue repository')
    parser.add_argument('--deep_scan', action='store_true', help='Perform a deep scan')
    parser.add_argument('--server' , help='The GitHub server to connect to')
    parser.add_argument('--workers', type=int, help='The number of workers to use')


# python .\inventory_gh_given_repo.py --source_org "upwork-corp" --output_file "GH-Repo-Inventory-Repo-post.xlsx" --issue_number "" --issue_repo "" --deep_scan "" --server "" --workers ""

    # Parse the arguments
    args = parser.parse_args()

    # Check if both or neither of issue_number and issue_repo are specified
    if (args.issue_number is None) != (args.issue_repo is None):
        parser.error("--issue_number and --issue_repo should be both present or both absent")

    if (args.server is not None):
        global api_url
        api_url = f"https://{args.server}/api/v3"

    workers = args.workers if args.workers else 5

    # Get the source organization and output file name from command line arguments
    source_org, output_file = args.source_org, args.output_file
    issue_number, issue_repo = args.issue_number, args.issue_repo
    deep_scan = args.deep_scan
    # Set the API endpoint
    url = f"{api_url}/orgs/{source_org}/repos?per_page=100&page=1"

    # Set the headers for API request
    headers = {
        'Authorization': f'token {get_pat()}',
        'Accept': 'application/vnd.github.v3+json',
    }
    #print(headers)
    repo_count = 0
    scan_date = get_current_datetime()
    starting_api_calls = get_remaining_api_calls(source_org)[0]
    starting_time = datetime.now()

    link_header = None
    while url:
        # Send the API request
        req = urllib.request.Request(url, headers=headers)
        with openurl(req) as response:
        #with urllib.request.urlopen(req) as response:
            #print(f"Remaining calls: {get_remaining_calls(response.headers)}")
            repos = json.loads(response.read().decode())
            # Get the Link header
            link_header = response.getheader('Link')
            # Parse the Link header
            links = parse_link_header(link_header)

        # itterate through the repos and extract the data

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_repo, repo, deep_scan) for repo in repos}
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"An error occurred: {e}")


        # for repo in repos:
        #     #wait_for_rate_limit(source_org)
        #     # overwrite the previous line with the current repo name
        #     # make sure the entire line is overwritten by padding with spaces


        #     print_msg(f"Exporting repo: {repo['name']}")
        #     #print(f"\rExporting repo: {repo['name']}", end="")
        #     #print ("exporting repo: ", repo['name'])

        #     store_repo_data(repo['name'], scan_repo(repo, deep_scan))


        #     #last_run = get_latest_workflow_run(source_org, repo_name) if deep_scan else None
        #     #commits = count_commits(source_org, repo_name) if deep_scan else None
        #     #pull_requests = count_prs(source_org, repo_name) if deep_scan else None
        #     #webhooks = count_webhooks(source_org, repo_name) if deep_scan else None
        #     #branches = count_branches(source_org, repo_name) if deep_scan else None

        #     # Write the repository details to the CSV file

        #     repo_count += 1
        #     #print (get_latest_workflow_run(source_org, repo_name))
        #     #writer.writerow([repo['owner']['login'], repo['name'], repo['updated_at'], 'Private' if repo['private'] else 'Public', repo['archived']])

        # Get the next page URL
        url = links['next'] if 'next' in links else None

    # output the repos to a csv file
    # Open the output file in write mode
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        # Write the header row
        column_headers = (['Org', 'Repo', 'FullName', 'SizeGB', 'SizeMB', 'SizeKB', 'LastUpdated','LastUpdatedDays', 'LastPushed', 'LastPushedDays', 'Visibility', 'Archived','Language', 'IsFork', 'ForkCount', 'IssuesOn', 'ProjectsOn', 'WikiOn', 'PagesOn'])
        column_headers.extend(['Branches', 'Commits', 'PullRequests', 'LastWFRun', 'Webhooks', 'Packages', 'CommitComments', 'Environments', 'Issues']) if deep_scan else None
        column_headers.append('ScanDate')
        writer.writerow(column_headers)

        for data in repo_data:
            row = [
                data.get("owner_login"),
                data.get("repo_name"),
                data.get("full_name"),
                format_size(data.get("size_gb", 0)),
                format_size(data.get("size_mb", 0)),
                data.get("size_kb", 0),
                data.get("updated_at"),
                data.get("updated_at_days"),
                data.get("pushed_at"),
                data.get("pushed_at_days"),
                data.get("privacy"),
                data.get("archived"),
                data.get("language"),
                data.get("fork"),
                data.get("forks_count"),
                data.get("has_issues"),
                data.get("has_projects"),
                data.get("has_wiki"),
                data.get("has_pages")
            ]
            if deep_scan:
                row.extend([
                    data.get("branches"),
                    data.get("commits"),
                    data.get("pull_requests"),
                    data.get("last_run"),
                    data.get("webhooks"),
                    data.get("packages"),
                    data.get("commitcomments"),
                    data.get("environments"),
                    data.get("issues")
                ])
            row.append(scan_date)
            writer.writerow(row)


    # csv_to_xlsx(output_file, output_file.replace('.csv', '.xlsx'))
    ending_time = datetime.now()
    ending_api_calls = get_remaining_api_calls(source_org)[0]
    #starting_api_calls = get_remaining_api_calls(source_org)[0]

    repo_count = len(repo_data)

    intro_text = f"""
| Attribute             | Value |
|-----------------|-------|
| Scan Date       | {scan_date} |
| Organization    | {source_org} |
| Number of repos | {repo_count} |
| Execution Time  | {ending_time - starting_time} |
| API calls start | {starting_api_calls} |
| API calls Used  | {api_call_counter} |
| API calls Left  | {ending_api_calls} |
"""

    intro_text += f"""

**TAB delimited inventory:**
"""
    comment_on_issue(issue_repo, issue_number, "", intro_text) if issue_number != None else None

    print (intro_text)

if __name__ == "__main__":
    main()