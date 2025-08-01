"""
Azure DevOps Project Inventory Script

This script collects project and repository information from Azure DevOps organizations
and exports the data to a CSV file.

Usage:
    python collect-ado-inventory.py <organizations_file> [options]

Arguments:
    organizations_file    Path to file containing organization names (one per line)

Options:
    --max-org-workers     Maximum parallel organization workers (default: 5)
    --max-project-workers Maximum parallel project workers (default: 3)

Environment Variables:
    ADO_PAT              Azure DevOps Personal Access Token (required)

Example:
    python collect-ado-inventory.py FMT_orgs.list
    python collect-ado-inventory.py organizations.txt --max-org-workers 3 --max-project-workers 2
"""

import requests
import base64
import csv
import json
import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math

# Thread lock for thread-safe printing and logging
print_lock = threading.Lock()

def setup_logging():
    """Setup logging configuration with timestamp in filename"""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_filename = f"ADO-nventory-{timestamp}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
        ]
    )
    
    return log_filename

def thread_safe_print(message):
    """Thread-safe print function that also logs to file"""
    with print_lock:
        print(message)
        logging.info(message)

def load_organizations_from_file(filename: str) -> List[str]:
    """Load organization names from a file"""
    organizations = []
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                org_name = line.strip()
                if org_name and not org_name.startswith('#'):  # Skip empty lines and comments
                    organizations.append(org_name)
        return organizations
    except FileNotFoundError:
        thread_safe_print(f"Error: File '{filename}' not found.")
        thread_safe_print(f"Please make sure the organizations file exists.")
        thread_safe_print(f"Usage: python collect-ado-inventory.py <organizations_file>")
        sys.exit(1)
    except Exception as e:
        thread_safe_print(f"Error reading file '{filename}': {e}")
        sys.exit(1)

def get_ado_token() -> str:
    """Get Azure DevOps Personal Access Token from environment variable"""
    token = os.getenv('ADO_PAT')
    if not token:
        thread_safe_print("Error: ADO_PAT environment variable not set.")
        thread_safe_print("Please set it using: export ADO_PAT=\"your_token_here\"")
        sys.exit(1)
    return token

def get_auth_header(token: str) -> Dict[str, str]:
    """Create authentication header for Azure DevOps API"""
    auth_string = f":{token}"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    return {
        "Authorization": f"Basic {encoded_auth}",
        "Content-Type": "application/json"
    }

def get_projects_within_org(organization: str, token: str) -> List[str]:
    """Get all projects within an organization"""
    headers = get_auth_header(token)
    url = f"https://dev.azure.com/{organization}/_apis/projects?api-version=7.1"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return [project['name'] for project in data.get('value', [])]
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"Error getting projects for {organization}: {e}")
        return []

def get_repos_within_project(organization: str, project: str, token: str) -> List[Dict[str, Any]]:
    """Get all repositories within a project"""
    headers = get_auth_header(token)
    url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories?api-version=7.1"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get('value', [])
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"Error getting repos for {organization}/{project}: {e}")
        return []

def get_project_version_control_type(organization: str, project: str, token: str) -> str:
    """
    Improved function to determine if a project uses Git, TFVC, or file storage
    This version checks both systems and determines which is the primary/active one
    """
    headers = get_auth_header(token)
    
    # Check both TFVC and Git simultaneously
    tfvc_info = check_tfvc_system_inline(organization, project, headers)
    git_info = check_git_system_inline(organization, project, headers)
    
    # Decision logic based on findings
    if tfvc_info['has_content'] and git_info['has_content']:
        # Both systems have content - determine which is primary
        thread_safe_print(f"    Project has BOTH TFVC and Git content - analyzing which is primary...")
        return determine_primary_version_control_inline(tfvc_info, git_info)
    elif git_info['has_content']:
        thread_safe_print(f"    Project uses Git version control ({git_info['repo_count']} repositories with content)")
        return "GIT"
    elif tfvc_info['has_content']:
        thread_safe_print(f"    Project uses TFVC version control ({tfvc_info['item_count']} items found)")
        return "TFVC"
    else:
        # Check for other storage types
        return check_other_storage_types_inline(organization, project, headers)

def check_tfvc_system_inline(organization: str, project: str, headers) -> dict:
    """Check TFVC system and return detailed information including size data"""
    tfvc_info = {
        'has_content': False,
        'item_count': 0,
        'total_size': 0,  # Add total size tracking
        'changesets': 0,
        'last_activity': None,
        'error': None
    }
    
    try:
        # Check TFVC items with size information
        tfvc_url = f"https://dev.azure.com/{organization}/{project}/_apis/tfvc/items?api-version=7.1&recursionLevel=Full&includeContentMetadata=true"
        tfvc_response = requests.get(tfvc_url, headers=headers)
        if tfvc_response.status_code == 200:
            tfvc_data = tfvc_response.json()
            tfvc_items = tfvc_data.get('value', [])
            if tfvc_items:
                tfvc_info['has_content'] = True
                # Count files and calculate total size
                file_count = 0
                total_size = 0
                for item in tfvc_items:
                    if not item.get('isFolder', False):  # This is a file
                        file_count += 1
                        file_size = item.get('size', 0)
                        total_size += file_size
                
                tfvc_info['item_count'] = file_count
                tfvc_info['total_size'] = total_size
        
        # Check TFVC changesets for activity (fallback if items API doesn't work)
        if not tfvc_info['has_content']:
            changesets_url = f"https://dev.azure.com/{organization}/{project}/_apis/tfvc/changesets?api-version=7.1&$top=1"
            changesets_response = requests.get(changesets_url, headers=headers)
            if changesets_response.status_code == 200:
                changesets_data = changesets_response.json()
                changesets = changesets_data.get('value', [])
                if changesets:
                    tfvc_info['has_content'] = True
                    tfvc_info['changesets'] = len(changesets)
                    
                    # Get last activity date
                    if changesets:
                        last_changeset = changesets[0]
                        created_date = last_changeset.get('createdDate')
                        if created_date:
                            try:
                                tfvc_info['last_activity'] = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
                            except:
                                pass
                            
    except requests.exceptions.RequestException as e:
        tfvc_info['error'] = str(e)
    
    return tfvc_info

def check_git_system_inline(organization: str, project: str, headers) -> dict:
    """Check Git system and return detailed information including size data"""
    git_info = {
        'has_content': False,
        'repo_count': 0,
        'total_files': 0,
        'total_size': 0,  # Add total size tracking
        'last_activity': None,
        'repos': [],
        'error': None
    }
    
    try:
        git_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories?api-version=7.1"
        git_response = requests.get(git_url, headers=headers)
        if git_response.status_code == 200:
            git_data = git_response.json()
            git_repos = git_data.get('value', [])
            
            if git_repos:
                repos_with_content = []
                total_files = 0
                total_size = 0
                latest_activity = None
                
                for repo in git_repos[:3]:  # Check first 3 repos
                    repo_info = analyze_git_repo_inline(organization, project, repo, headers)
                    if repo_info['has_content']:
                        repos_with_content.append(repo_info)
                        total_files += repo_info['file_count']
                        total_size += repo_info.get('size_bytes', 0)  # Add size from repo
                        
                        # Track latest activity
                        if repo_info['last_commit'] and (not latest_activity or repo_info['last_commit'] > latest_activity):
                            latest_activity = repo_info['last_commit']
                
                if repos_with_content:
                    git_info['has_content'] = True
                    git_info['repo_count'] = len(repos_with_content)
                    git_info['total_files'] = total_files
                    git_info['total_size'] = total_size
                    git_info['last_activity'] = latest_activity
                    git_info['repos'] = repos_with_content
                    
    except requests.exceptions.RequestException as e:
        git_info['error'] = str(e)
    
    return git_info

def analyze_git_repo_inline(organization: str, project: str, repo: dict, headers) -> dict:
    """Analyze a single Git repository for content, activity, and size"""
    repo_info = {
        'name': repo.get('name', 'Unknown'),
        'id': repo['id'],
        'has_content': False,
        'file_count': 0,
        'size_bytes': 0,  # Add size tracking
        'last_commit': None
    }
    
    try:
        # Get repository size from API first
        repo_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo['id']}?api-version=7.1"
        repo_response = requests.get(repo_url, headers=headers)
        if repo_response.status_code == 200:
            repo_data = repo_response.json()
            if 'size' in repo_data:
                repo_info['size_bytes'] = repo_data['size']
        
        # Check repository content
        items_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo['id']}/items?api-version=7.1&recursionLevel=Full"
        items_response = requests.get(items_url, headers=headers)
        if items_response.status_code == 200:
            items_data = items_response.json()
            items = items_data.get('value', [])
            # Count actual files (blobs)
            file_count = sum(1 for item in items if item.get('gitObjectType') == 'blob')
            if file_count > 0:
                repo_info['has_content'] = True
                repo_info['file_count'] = file_count
            elif len(items) > 1:  # Has structure
                repo_info['has_content'] = True
                repo_info['file_count'] = len(items)
        
        # Get last commit for activity
        commits_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo['id']}/commits?searchCriteria.$top=1&api-version=7.1"
        commits_response = requests.get(commits_url, headers=headers)
        if commits_response.status_code == 200:
            commits_data = commits_response.json()
            commits = commits_data.get('value', [])
            if commits:
                commit_date = commits[0].get('author', {}).get('date')
                if commit_date:
                    try:
                        repo_info['last_commit'] = datetime.fromisoformat(commit_date.replace('Z', '+00:00'))
                    except:
                        pass
                        
    except requests.exceptions.RequestException:
        pass
    
    return repo_info

def determine_primary_version_control_inline(tfvc_info: dict, git_info: dict) -> str:
    """
    Determine which version control system is primary when both exist
    Primary decision factor: Repository size (whichever has maximum size)
    Fallback factors:
    1. Recent activity (commits/changesets in last 2 years)
    2. Content volume (file/item count)
    3. Default to Git if similar (modern standard)
    """
    from datetime import timezone
    
    # First, try to get repository sizes for comparison
    tfvc_size = get_system_size_estimate(tfvc_info, 'tfvc')
    git_size = get_system_size_estimate(git_info, 'git')
    
    # If we have size information for both, use size as the primary decision factor
    if tfvc_size > 0 and git_size > 0:
        if git_size > tfvc_size:
            thread_safe_print(f"    Git is primary (larger repository: {format_size_in_kb(git_size)} KB vs TFVC {format_size_in_kb(tfvc_size)} KB)")
            return "GIT"
        elif tfvc_size > git_size:
            thread_safe_print(f"    TFVC is primary (larger repository: {format_size_in_kb(tfvc_size)} KB vs Git {format_size_in_kb(git_size)} KB)")
            return "TFVC"
        else:
            thread_safe_print(f"    Both systems have similar size ({format_size_in_kb(git_size)} KB) - checking other factors...")
    elif git_size > 0:
        thread_safe_print(f"    Git is primary (only Git has size data: {format_size_in_kb(git_size)} KB)")
        return "GIT"
    elif tfvc_size > 0:
        thread_safe_print(f"    TFVC is primary (only TFVC has size data: {format_size_in_kb(tfvc_size)} KB)")
        return "TFVC"
    else:
        thread_safe_print(f"    No size data available for either system - checking activity and content...")
    
    # Fallback to activity-based decision if size comparison is inconclusive
    now = datetime.now(timezone.utc)
    two_years_ago = now.replace(year=now.year - 2)
    
    # Check recent activity
    tfvc_recent = tfvc_info.get('last_activity') and tfvc_info['last_activity'] > two_years_ago
    git_recent = git_info.get('last_activity') and git_info['last_activity'] > two_years_ago
    
    if git_recent and not tfvc_recent:
        thread_safe_print(f"    Git is primary (recent activity: {git_info['last_activity']}, TFVC last: {tfvc_info.get('last_activity', 'unknown')})")
        return "GIT"
    elif tfvc_recent and not git_recent:
        thread_safe_print(f"    TFVC is primary (recent activity: {tfvc_info['last_activity']}, Git last: {git_info.get('last_activity', 'unknown')})")
        return "TFVC"
    elif git_recent and tfvc_recent:
        # Both have recent activity - compare which is more recent
        if git_info['last_activity'] > tfvc_info['last_activity']:
            thread_safe_print(f"    Git is primary (more recent: {git_info['last_activity']} vs {tfvc_info['last_activity']})")
            return "GIT"
        else:
            thread_safe_print(f"    TFVC is primary (more recent: {tfvc_info['last_activity']} vs {git_info['last_activity']})")
            return "TFVC"
    else:
        # No recent activity in either - compare content volume and default to Git
        if git_info['total_files'] > tfvc_info['item_count']:
            thread_safe_print(f"    Git is primary (more content: {git_info['total_files']} files vs {tfvc_info['item_count']} TFVC items)")
            return "GIT"
        elif tfvc_info['item_count'] > git_info['total_files'] * 2:  # TFVC significantly more
            thread_safe_print(f"    TFVC is primary (significantly more content: {tfvc_info['item_count']} items vs {git_info['total_files']} Git files)")
            return "TFVC"
        else:
            thread_safe_print(f"    Defaulting to Git (modern standard) - similar content levels")
            return "GIT"

def get_system_size_estimate(system_info: dict, system_type: str) -> int:
    """
    Get size estimate for a version control system
    Returns size in bytes, or 0 if no size information available
    """
    if system_type == 'git':
        # Use actual total size if available
        if system_info.get('total_size', 0) > 0:
            return system_info['total_size']
        # Fallback: estimate based on individual repository sizes
        elif system_info.get('repos'):
            total_size = 0
            for repo in system_info['repos']:
                repo_size = repo.get('size_bytes', 0)
                if repo_size > 0:
                    total_size += repo_size
                else:
                    # Estimate if no size data: 5KB per file
                    file_count = repo.get('file_count', 0)
                    total_size += file_count * 5 * 1024
            return total_size
        elif system_info.get('total_files', 0) > 0:
            # Last resort estimate based on total files
            return system_info['total_files'] * 5 * 1024  # 5KB per file
    elif system_type == 'tfvc':
        # Use actual total size if available
        if system_info.get('total_size', 0) > 0:
            return system_info['total_size']
        # Fallback estimate based on item count
        elif system_info.get('item_count', 0) > 0:
            return system_info['item_count'] * 10 * 1024  # 10KB per item
    
    return 0

def check_other_storage_types_inline(organization: str, project: str, headers) -> str:
    """Check for other storage types when neither TFVC nor Git have content"""
    try:
        # Check for artifacts/feeds
        artifacts_url = f"https://feeds.dev.azure.com/{organization}/{project}/_apis/packaging/feeds?api-version=7.1-preview.1"
        artifacts_response = requests.get(artifacts_url, headers=headers)
        if artifacts_response.status_code == 200:
            artifacts_data = artifacts_response.json()
            feeds = artifacts_data.get('value', [])
            if feeds:
                thread_safe_print(f"    Project has artifact feeds ({len(feeds)} feeds)")
                return "ARTIFACTS"
    except requests.exceptions.RequestException:
        pass
    
    try:
        # Check for wiki content
        wiki_url = f"https://dev.azure.com/{organization}/{project}/_apis/wiki/wikis?api-version=7.1"
        wiki_response = requests.get(wiki_url, headers=headers)
        if wiki_response.status_code == 200:
            wiki_data = wiki_response.json()
            wikis = wiki_data.get('value', [])
            if wikis:
                thread_safe_print(f"    Project has wiki content ({len(wikis)} wikis)")
                return "WIKI"
    except requests.exceptions.RequestException:
        pass
    
    thread_safe_print(f"    Project exists but version control type unclear - checking for file storage")
    return "FILE_STORAGE"

def get_repo_size_mb(organization: str, project: str, repo_id: str, token: str) -> str:
    """Get repository size in MB"""
    headers = get_auth_header(token)
    
    try:
        # Get repository information for size
        repo_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}?api-version=7.1"
        repo_response = requests.get(repo_url, headers=headers)
        if repo_response.status_code == 200:
            repo_data = repo_response.json()
            if 'size' in repo_data and repo_data['size'] > 0:
                size_mb = repo_data['size'] / (1024 * 1024)  # Convert bytes to MB
                return f"{size_mb:.2f}"
            else:
                return "0.00"
        else:
            return "Unknown"
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"    Error getting repo size: {e}")
        return "Error"

def get_repo_branch_count(organization: str, project: str, repo_id: str, token: str) -> str:
    """Get number of branches in repository"""
    headers = get_auth_header(token)
    
    try:
        # Get all branches for the repository
        branches_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}/refs?filter=heads/&api-version=7.1"
        branches_response = requests.get(branches_url, headers=headers)
        if branches_response.status_code == 200:
            branches_data = branches_response.json()
            branches = branches_data.get('value', [])
            return str(len(branches))
        else:
            return "Unknown"
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"    Error getting branch count: {e}")
        return "Error"

def get_repo_last_modified(organization: str, project: str, repo_id: str, token: str) -> str:
    """Get last modified timestamp from repository commit history"""
    headers = get_auth_header(token)
    
    try:
        # Get the most recent commit
        commits_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}/commits?searchCriteria.$top=1&api-version=7.1"
        commits_response = requests.get(commits_url, headers=headers)
        if commits_response.status_code == 200:
            commits_data = commits_response.json()
            commits = commits_data.get('value', [])
            if commits:
                commit = commits[0]
                commit_date = commit.get('author', {}).get('date', 'Unknown')
                if commit_date != 'Unknown':
                    try:
                        # Parse and format the timestamp
                        parsed_date = datetime.fromisoformat(commit_date.replace('Z', '+00:00'))
                        return parsed_date.strftime('%Y-%m-%d %H:%M:%S UTC')
                    except:
                        return commit_date
                else:
                    return "Unknown"
            else:
                return "No commits"
        else:
            return "Unknown"
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"    Error getting last modified time: {e}")
        return "Error"

def get_tfvc_last_changeset(organization: str, project: str, token: str) -> str:
    """Get the last changeset timestamp for a TFVC project"""
    headers = get_auth_header(token)
    
    try:
        changesets_url = f"https://dev.azure.com/{organization}/{project}/_apis/tfvc/changesets?api-version=7.1&$top=1&$orderby=id desc"
        changesets_response = requests.get(changesets_url, headers=headers)
        if changesets_response.status_code == 200:
            changesets_data = changesets_response.json()
            changesets = changesets_data.get('value', [])
            if changesets:
                changeset = changesets[0]
                changeset_date = changeset.get('createdDate', 'Unknown')
                if changeset_date != 'Unknown':
                    try:
                        # Parse and format the timestamp
                        parsed_date = datetime.fromisoformat(changeset_date.replace('Z', '+00:00'))
                        return parsed_date.strftime('%Y-%m-%d %H:%M:%S UTC')
                    except:
                        return changeset_date
                else:
                    return "Unknown"
            else:
                return "No changesets"
        else:
            return "Unknown"
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"    Error getting TFVC last changeset: {e}")
        return "Error"

def get_file_storage_last_modified(organization: str, project: str, token: str) -> str:
    """Get last modified time for file storage projects"""
    headers = get_auth_header(token)
    
    try:
        # Try to get work items as they might indicate activity
        work_items_url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitems?api-version=7.1&$top=1&$orderby=System.ChangedDate desc"
        work_items_response = requests.get(work_items_url, headers=headers)
        if work_items_response.status_code == 200:
            work_items_data = work_items_response.json()
            work_items = work_items_data.get('value', [])
            if work_items:
                work_item = work_items[0]
                changed_date = work_item.get('fields', {}).get('System.ChangedDate', 'Unknown')
                if changed_date != 'Unknown':
                    try:
                        # Parse and format the timestamp
                        parsed_date = datetime.fromisoformat(changed_date.replace('Z', '+00:00'))
                        return parsed_date.strftime('%Y-%m-%d %H:%M:%S UTC')
                    except:
                        return changed_date
                else:
                    return "Unknown"
            else:
                return "No activity"
        else:
            return "Unknown"
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"    Error getting file storage last modified: {e}")
        return "Unknown"

def get_repo_statistics(organization: str, project: str, repo_id: str, token: str) -> Dict[str, Any]:
    """Get repository statistics including size, file count, and largest file size"""
    headers = get_auth_header(token)
    
    # Initialize default values
    stats = {
        'total_size': 'Unknown',
        'file_count': 'Unknown',
        'largest_file_size': 'Unknown'
    }
    
    # Initialize variables for file analysis
    file_count = 0
    calculated_total_size = 0
    largest_size = 0
    repo_api_size = None
    
    try:
        # Get repository information for size (as a fallback)
        repo_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}?api-version=7.1"
        repo_response = requests.get(repo_url, headers=headers)
        if repo_response.status_code == 200:
            repo_data = repo_response.json()
            # Store repository size as fallback, but don't use it yet
            if 'size' in repo_data:
                repo_api_size = repo_data['size']
        
        # Get items in the repository to count files and find largest file
        items_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}/items?api-version=7.1&recursionLevel=Full&includeContentMetadata=true"
        items_response = requests.get(items_url, headers=headers)
        
        if items_response.status_code == 200:
            items_data = items_response.json()
            items = items_data.get('value', [])
            
            # Count files and calculate sizes from individual files
            files = [item for item in items if item.get('gitObjectType') == 'blob']
            file_count = len(files)
            
            if file_count > 0:
                thread_safe_print(f"    Analyzing {file_count} files for accurate sizes...")
                
                # Method 1: Try to get sizes from item metadata first
                sizes_from_metadata = 0
                for item in files:
                    file_size = item.get('size', 0)
                    if file_size and file_size > 0:
                        calculated_total_size += file_size
                        if file_size > largest_size:
                            largest_size = file_size
                        sizes_from_metadata += 1
                
                # Method 2: If metadata doesn't have sizes, use Content-Length method for a sample
                if sizes_from_metadata == 0 and file_count <= 50:  # Only for small repos to avoid too many requests
                    thread_safe_print(f"    Metadata missing size info, using Content-Length method for accurate measurement...")
                    successful_measurements = 0
                    
                    for item in files:
                        file_path = item.get('path', '')
                        if file_path:
                            try:
                                # Use HEAD request to get Content-Length without downloading content
                                content_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}/items?path={file_path}&api-version=7.1&includeContent=true"
                                head_response = requests.head(content_url, headers=headers, timeout=5)
                                content_length = head_response.headers.get('Content-Length')
                                
                                if content_length:
                                    file_size = int(content_length)
                                    calculated_total_size += file_size
                                    if file_size > largest_size:
                                        largest_size = file_size
                                    successful_measurements += 1
                                elif successful_measurements < 5:  # Try GET for first few files if HEAD fails
                                    get_response = requests.get(content_url, headers=headers, timeout=5, stream=True)
                                    if get_response.status_code == 200:
                                        content_length = get_response.headers.get('Content-Length')
                                        if content_length:
                                            file_size = int(content_length)
                                            calculated_total_size += file_size
                                            if file_size > largest_size:
                                                largest_size = file_size
                                            successful_measurements += 1
                                        get_response.close()  # Close the stream
                            except Exception as e:
                                # Skip files that can't be measured
                                continue
                    
                    thread_safe_print(f"    Successfully measured {successful_measurements}/{file_count} files using Content-Length method")
                
                # Method 3: If we still don't have accurate sizes, use estimation with better logic
                if calculated_total_size == 0:
                    if repo_api_size is not None and repo_api_size > 0:
                        # Use repository API size as fallback
                        calculated_total_size = repo_api_size
                        largest_size = max(1024, repo_api_size // file_count)  # Estimate largest file
                        thread_safe_print(f"    Using repository API size: {repo_api_size} bytes")
                    else:
                        # Provide conservative estimates based on file count and type
                        avg_file_size = 1024  # 1KB default
                        calculated_total_size = file_count * avg_file_size
                        largest_size = avg_file_size * 2  # Assume largest is 2x average
                        thread_safe_print(f"    No size information available - using conservative estimates")
            
            # Set final statistics
            stats['file_count'] = str(file_count)
            stats['largest_file_size'] = format_size_in_kb(largest_size)
            
            # For total repository size, use the most reliable available method
            if repo_api_size is not None and repo_api_size > 0:
                # Repository API has accurate size including Git metadata
                stats['total_size'] = format_size_in_kb(repo_api_size)
                thread_safe_print(f"    Using repository API size: {repo_api_size} bytes (includes Git metadata)")
            elif calculated_total_size > 0:
                # Use measured content size but add realistic Git overhead estimate
                # Note: Actual download sizes can be much larger due to Git working tree overhead
                # For very small repos with minimal content, overhead can be 50-100x due to
                # minimum .git folder size, file system allocation, and Git object database
                if calculated_total_size < 10000:  # Less than 10KB content
                    # Small repos have disproportionate overhead
                    min_git_size = 40 * 1024  # Minimum ~40KB for basic Git repo structure
                    estimated_repo_size = max(calculated_total_size * 50, min_git_size)
                else:
                    # Larger repos have more proportional overhead
                    estimated_repo_size = calculated_total_size * 8
                
                stats['total_size'] = format_size_in_kb(estimated_repo_size)
                thread_safe_print(f"    Content size: {calculated_total_size} bytes → Estimated download: {estimated_repo_size} bytes (includes Git working tree overhead)")
            else:
                # Conservative estimate based on file count
                if file_count > 0:
                    # For Git repos, estimate higher than just content due to Git overhead
                    estimated_total = file_count * 3072  # 3KB per file (content + Git overhead)
                    stats['total_size'] = format_size_in_kb(estimated_total)
                    thread_safe_print(f"    Using Git repository estimate: {estimated_total} bytes for {file_count} files")
                else:
                    stats['total_size'] = '0'
                
        else:
            # If we can't get items, this might be an empty repository or access issue
            file_count = 0
            stats['file_count'] = '0'
            stats['largest_file_size'] = '0'
            
            # For total size, if repo API gave us a size but we can't access files, 
            # it's likely an empty repo or access restriction
            if repo_api_size is not None and repo_api_size > 0:
                thread_safe_print(f"    Repository API shows {repo_api_size} bytes but items API failed (HTTP {items_response.status_code}) - likely access restricted")
                stats['total_size'] = format_size_in_kb(repo_api_size)
            else:
                stats['total_size'] = '0'
                thread_safe_print(f"    Could not retrieve repository items (HTTP {items_response.status_code}) - treating as empty repository")
            
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"    Error getting repository statistics: {e}")
        # Set all to 0 if we can't get any data
        stats = {
            'total_size': '0',
            'file_count': '0',
            'largest_file_size': '0'
        }
    
    return stats

def get_tfvc_statistics(organization: str, project: str, token: str) -> Dict[str, Any]:
    """Get TFVC project statistics including file count and sizes"""
    headers = get_auth_header(token)
    
    # Initialize default values
    stats = {
        'total_size': 'Unknown',
        'file_count': 'Unknown',
        'largest_file_size': 'Unknown'
    }
    
    try:
        # Method 1: Try to get TFVC items with full recursion
        items_url = f"https://dev.azure.com/{organization}/{project}/_apis/tfvc/items?api-version=7.1&recursionLevel=Full&includeContentMetadata=true"
        items_response = requests.get(items_url, headers=headers)
        
        file_count = 0
        largest_size = 0
        total_size = 0
        
        if items_response.status_code == 200:
            items_data = items_response.json()
            items = items_data.get('value', [])
            thread_safe_print(f"    Retrieved {len(items)} TFVC items")
            
            # Count files and find largest file
            for item in items:
                if not item.get('isFolder', False):  # This is a file
                    file_count += 1
                    file_size = item.get('size', 0)
                    total_size += file_size
                    if file_size > largest_size:
                        largest_size = file_size
            
            thread_safe_print(f"    TFVC analysis: {file_count} files, {total_size} bytes total, largest file {largest_size} bytes")
            
            if file_count > 0:
                stats['file_count'] = str(file_count)
                stats['largest_file_size'] = format_size_in_kb(largest_size)
                stats['total_size'] = format_size_in_kb(total_size)
            else:
                # No files found, but items API worked - this is a legitimate empty repository
                thread_safe_print(f"    TFVC repository is empty (contains only folders, no files)")
                stats['file_count'] = '0'
                stats['total_size'] = '0'
                stats['largest_file_size'] = '0'
        else:
            thread_safe_print(f"    TFVC items API failed (HTTP {items_response.status_code})")
            # Log the response details for debugging
            try:
                error_details = items_response.json()
                thread_safe_print(f"    TFVC API Error details: {error_details}")
            except:
                thread_safe_print(f"    TFVC API Error content: {items_response.text[:200]}")
            
            # Try to get at least basic project info to confirm TFVC presence
            changesets_url = f"https://dev.azure.com/{organization}/{project}/_apis/tfvc/changesets?api-version=7.1&$top=1"
            changesets_response = requests.get(changesets_url, headers=headers)
            if changesets_response.status_code == 200:
                changesets_data = changesets_response.json()
                changesets = changesets_data.get('value', [])
                if changesets:
                    thread_safe_print(f"    TFVC confirmed via changesets, but content access restricted - trying Git repository API for size")
                    # Fall back to Git repository API for size information
                    stats = try_git_fallback_for_tfvc(organization, project, token, stats)
                else:
                    stats['file_count'] = '0'
                    stats['total_size'] = '0'
                    stats['largest_file_size'] = '0'
            else:
                thread_safe_print(f"    No TFVC access at all")
                stats['file_count'] = 'No Access'
                stats['total_size'] = 'No Access'
                stats['largest_file_size'] = 'No Access'
                
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"    Error getting TFVC statistics: {e}")
        stats = {
            'total_size': 'Error',
            'file_count': 'Error',
            'largest_file_size': 'Error'
        }
    
    return stats

def try_git_fallback_for_tfvc(organization: str, project: str, token: str, current_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Try to get repository size from Git API when TFVC content is not accessible"""
    headers = get_auth_header(token)
    
    try:
        # Check if there are Git repositories (some TFVC projects also have Git repos for metadata)
        git_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories?api-version=7.1"
        git_response = requests.get(git_url, headers=headers)
        
        if git_response.status_code == 200:
            git_data = git_response.json()
            git_repos = git_data.get('value', [])
            
            if git_repos:
                # Try to get size from the first repository
                repo_id = git_repos[0]['id']
                repo_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}?api-version=7.1"
                repo_response = requests.get(repo_url, headers=headers)
                
                if repo_response.status_code == 200:
                    repo_data = repo_response.json()
                    if 'size' in repo_data and repo_data['size'] > 0:
                        thread_safe_print(f"    Found repository size via Git API: {repo_data['size']} bytes")
                        current_stats['total_size'] = format_size_in_kb(repo_data['size'])
                        current_stats['file_count'] = 'TFVC Access Restricted'
                        current_stats['largest_file_size'] = 'TFVC Access Restricted'
                        return current_stats
    
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"    Git fallback also failed: {e}")
    
    # If fallback fails, mark as access restricted
    current_stats['file_count'] = 'Access Restricted'
    current_stats['total_size'] = 'Access Restricted'
    current_stats['largest_file_size'] = 'Access Restricted'
    return current_stats

def format_size_in_kb(size_bytes) -> str:
    """Convert bytes to KB and format as integer (rounded up)"""
    if isinstance(size_bytes, (int, float)) and size_bytes > 0:
        size_kb = math.ceil(size_bytes / 1024)
        return str(size_kb)
    elif size_bytes == 0:
        return "0"
    else:
        return "Unknown"

def get_file_storage_info(organization: str, project: str, token: str) -> Dict[str, Any]:
    """Get basic information about projects with file storage but no traditional version control"""
    headers = get_auth_header(token)
    
    # Initialize default values
    info = {
        'repo_name': project,
        'total_size': 'Unknown',
        'file_count': 'Unknown',
        'largest_file_size': 'Unknown'
    }
    
    try:
        # Try to estimate file storage using alternative methods
        try:
            # Check for attachments in work items
            attachments_url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/attachments?api-version=7.1&$top=100"
            attachments_response = requests.get(attachments_url, headers=headers)
            if attachments_response.status_code == 200:
                attachments_data = attachments_response.json()
                attachments = attachments_data.get('value', [])
                if attachments:
                    total_attachment_size = 0
                    largest_attachment = 0
                    for attachment in attachments:
                        size = attachment.get('attributes', {}).get('resourceSize', 0)
                        total_attachment_size += size
                        if size > largest_attachment:
                            largest_attachment = size
                    
                    if total_attachment_size > 0:
                        info['total_size'] = format_size_in_kb(total_attachment_size)
                        info['file_count'] = str(len(attachments))
                        info['largest_file_size'] = format_size_in_kb(largest_attachment)
                        thread_safe_print(f"    Found {len(attachments)} attachments totaling {info['total_size']} KB")
        except requests.exceptions.RequestException:
            pass
            
        # Try to get shared documents or files via SharePoint if available
        try:
            # Some Azure DevOps projects use SharePoint for file storage
            sharepoint_url = f"https://dev.azure.com/{organization}/{project}/_apis/tfvc/items?api-version=7.1&recursionLevel=OneLevel&$top=50"
            sharepoint_response = requests.get(sharepoint_url, headers=headers)
            if sharepoint_response.status_code == 200:
                sharepoint_data = sharepoint_response.json()
                items = sharepoint_data.get('value', [])
                files = [item for item in items if not item.get('isFolder', False)]
                if files:
                    total_size = sum(item.get('size', 0) for item in files)
                    largest_size = max(item.get('size', 0) for item in files) if files else 0
                    
                    if total_size > 0:
                        info['total_size'] = format_size_in_kb(total_size)
                        info['file_count'] = str(len(files))
                        info['largest_file_size'] = format_size_in_kb(largest_size)
                        thread_safe_print(f"    Found {len(files)} files in alternative storage totaling {info['total_size']} KB")
        except requests.exceptions.RequestException:
            pass
        
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"    Error getting file storage info for {organization}/{project}: {e}")
    
    # For file storage projects, if no files/attachments were found, set values to 0 instead of Unknown
    if info['total_size'] == 'Unknown' and info['file_count'] == 'Unknown' and info['largest_file_size'] == 'Unknown':
        info['total_size'] = '0'
        info['file_count'] = '0'
        info['largest_file_size'] = '0'
        thread_safe_print(f"    No files found in file storage - setting values to 0")
    
    return info

def process_project(organization: str, project: str, token: str) -> List[Dict[str, Any]]:
    """Process a single project and return CSV data rows"""
    thread_safe_print(f"Processing project: {organization}/{project}")
    
    project_data = []
    
    # Get project version control type
    project_type = get_project_version_control_type(organization, project, token)
    
    # Get repositories for this project
    if project_type == "GIT":
        repos = get_repos_within_project(organization, project, token)
        
        if not repos:
            # If no repositories, still add the project to show it exists
            project_data.append({
                'Organization': organization,
                'Project': project,
                'Project Type': project_type,
                'Repo Name': 'No repositories found',
                'No of Branches of Repo': 'N/A',
                'Total Repo Size (KB)': 'N/A',
                'Total Repo Size (MB)': 'N/A',
                'No of Files in Repo': 'N/A',
                'Largest File Size in Repo (KB)': 'N/A',
                'Last Modified Time of Repo': 'N/A'
            })
        else:
            for repo in repos:
                # Get repository statistics
                repo_stats = get_repo_statistics(organization, project, repo['id'], token)
                
                # Get new metrics
                repo_size_mb = get_repo_size_mb(organization, project, repo['id'], token)
                branch_count = get_repo_branch_count(organization, project, repo['id'], token)
                last_modified = get_repo_last_modified(organization, project, repo['id'], token)
                
                project_data.append({
                    'Organization': organization,
                    'Project': project,
                    'Project Type': project_type,
                    'Repo Name': repo['name'],
                    'No of Branches of Repo': branch_count,
                    'Total Repo Size (KB)': repo_stats['total_size'],
                    'Total Repo Size (MB)': repo_size_mb,
                    'No of Files in Repo': repo_stats['file_count'],
                    'Largest File Size in Repo (KB)': repo_stats['largest_file_size'],
                    'Last Modified Time of Repo': last_modified
                })
    
    elif project_type == "TFVC":
        # Handle TFVC projects
        # Get TFVC statistics
        tfvc_stats = get_tfvc_statistics(organization, project, token)
        
        # Calculate MB from KB if available
        tfvc_size_mb = "N/A"
        if tfvc_stats['total_size'] != 'Unknown' and tfvc_stats['total_size'] != 'N/A':
            try:
                kb_value = float(tfvc_stats['total_size'])
                mb_value = kb_value / 1024  # Convert KB to MB
                tfvc_size_mb = f"{mb_value:.2f}"
            except (ValueError, TypeError):
                tfvc_size_mb = "N/A"
        
        # Get TFVC last modified time
        tfvc_last_modified = get_tfvc_last_changeset(organization, project, token)
        
        project_data.append({
            'Organization': organization,
            'Project': project,
            'Project Type': project_type,
            'Repo Name': project,  # For TFVC, use project name as repo name
            'No of Branches of Repo': 'N/A',
            'Total Repo Size (KB)': tfvc_stats['total_size'],
            'Total Repo Size (MB)': tfvc_size_mb,
            'No of Files in Repo': tfvc_stats['file_count'],
            'Largest File Size in Repo (KB)': tfvc_stats['largest_file_size'],
            'Last Modified Time of Repo': tfvc_last_modified
        })
    
    elif project_type in ["FILE_STORAGE", "ARTIFACTS", "WIKI"]:
        # Handle projects with file storage, artifacts, or wiki content
        file_storage_info = get_file_storage_info(organization, project, token)
        
        # Calculate MB from KB if available
        storage_size_mb = "N/A"
        if file_storage_info['total_size'] != 'Unknown' and file_storage_info['total_size'] != 'N/A':
            try:
                kb_value = float(file_storage_info['total_size'])
                mb_value = kb_value / 1024  # Convert KB to MB
                storage_size_mb = f"{mb_value:.2f}"
            except (ValueError, TypeError):
                storage_size_mb = "N/A"
        
        # Get file storage last modified time
        file_storage_last_modified = get_file_storage_last_modified(organization, project, token)
        
        project_data.append({
            'Organization': organization,
            'Project': project,
            'Project Type': project_type,
            'Repo Name': file_storage_info['repo_name'],
            'No of Branches of Repo': 'N/A',
            'Total Repo Size (KB)': file_storage_info['total_size'],
            'Total Repo Size (MB)': storage_size_mb,
            'No of Files in Repo': file_storage_info['file_count'],
            'Largest File Size in Repo (KB)': file_storage_info['largest_file_size'],
            'Last Modified Time of Repo': file_storage_last_modified
        })
    
    else:
        # Handle Unknown project types - try to get basic project info
        thread_safe_print(f"    Project type unknown, attempting to get basic project information")
        file_storage_info = get_file_storage_info(organization, project, token)
        
        # Calculate MB from KB if available
        unknown_size_mb = "N/A"
        if file_storage_info['total_size'] != 'Unknown' and file_storage_info['total_size'] != 'N/A':
            try:
                kb_value = float(file_storage_info['total_size'])
                mb_value = kb_value / 1024  # Convert KB to MB
                unknown_size_mb = f"{mb_value:.2f}"
            except (ValueError, TypeError):
                unknown_size_mb = "N/A"
        
        # Get unknown project type last modified time
        unknown_last_modified = get_file_storage_last_modified(organization, project, token)
        
        project_data.append({
            'Organization': organization,
            'Project': project,
            'Project Type': f"{project_type} (Basic Info)",
            'Repo Name': file_storage_info['repo_name'],
            'No of Branches of Repo': 'N/A',
            'Total Repo Size (KB)': file_storage_info['total_size'],
            'Total Repo Size (MB)': unknown_size_mb,
            'No of Files in Repo': file_storage_info['file_count'],
            'Largest File Size in Repo (KB)': file_storage_info['largest_file_size'],
            'Last Modified Time of Repo': unknown_last_modified
        })
    
    return project_data

def process_organization(organization: str, token: str, max_project_workers: int = 3) -> List[Dict[str, Any]]:
    """Process a single organization and return CSV data rows"""
    thread_safe_print(f"Processing organization: {organization}")
    thread_safe_print("=" * 50)
    
    org_data = []
    
    # Get projects for this organization
    projects = get_projects_within_org(organization, token)
    
    if not projects:
        thread_safe_print(f"No projects found for {organization}")
        return org_data
    
    # Process projects in parallel (with configurable thread pool size)
    with ThreadPoolExecutor(max_workers=max_project_workers) as project_executor:
        project_futures = {
            project_executor.submit(process_project, organization, project, token): project 
            for project in projects
        }
        
        for future in as_completed(project_futures):
            project = project_futures[future]
            try:
                project_data = future.result()
                org_data.extend(project_data)
            except Exception as e:
                thread_safe_print(f"Error processing project {organization}/{project}: {e}")
    
    return org_data

def main():
    """Main function to collect data and export to CSV"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Collect Azure DevOps project and repository information')
    parser.add_argument('orgs_file', help='Path to the file containing organization names (one per line)')
    parser.add_argument('--max-org-workers', type=int, default=5, help='Maximum number of parallel organization workers (default: 5)')
    parser.add_argument('--max-project-workers', type=int, default=3, help='Maximum number of parallel project workers (default: 3)')
    
    args = parser.parse_args()
    
    # Setup logging
    log_filename = setup_logging()
    print(f"Logging initialized. Log file: {log_filename}")
    
    start_time = time.time()
    
    # Load organizations from the specified file
    organizations = load_organizations_from_file(args.orgs_file)
    thread_safe_print(f"Loaded {len(organizations)} organizations from file: {args.orgs_file}")
    
    # Get token from environment variable
    token = get_ado_token()
    thread_safe_print("ADO_PAT token loaded successfully")
    thread_safe_print(f"Starting parallel processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    csv_data = []
    
    # Process organizations in parallel
    with ThreadPoolExecutor(max_workers=args.max_org_workers) as org_executor:
        org_futures = {
            org_executor.submit(process_organization, org, token, args.max_project_workers): org 
            for org in organizations
        }
        
        for future in as_completed(org_futures):
            org = org_futures[future]
            try:
                org_data = future.result()
                csv_data.extend(org_data)
                thread_safe_print(f"Completed processing organization: {org}")
            except Exception as e:
                thread_safe_print(f"Error processing organization {org}: {e}")
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Export to CSV
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    csv_filename = f"ADO-Inventory-{timestamp}.csv"
    
    if csv_data:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Organization', 'Project', 'Project Type', 'Repo Name', 'No of Branches of Repo', 'Total Repo Size (KB)', 'Total Repo Size (MB)', 'No of Files in Repo', 'Largest File Size in Repo (KB)', 'Last Modified Time of Repo']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in csv_data:
                writer.writerow(row)
        
        thread_safe_print(f"\n" + "="*60)
        thread_safe_print(f"EXECUTION SUMMARY")
        thread_safe_print(f"="*60)
        thread_safe_print(f"Data exported to: {csv_filename}")
        thread_safe_print(f"Log file: {log_filename}")
        thread_safe_print(f"Total records: {len(csv_data)}")
        thread_safe_print(f"Organizations processed: {len(organizations)}")
        thread_safe_print(f"Total execution time: {execution_time:.2f} seconds")
        thread_safe_print(f"Average time per organization: {execution_time/len(organizations):.2f} seconds")
        thread_safe_print(f"Parallel processing completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        thread_safe_print("No data to export.")

if __name__ == "__main__":
    main()
