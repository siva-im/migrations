import requests
from requests.auth import HTTPBasicAuth
import argparse
import pandas as pd
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_projects(server_url, username, password):
    """
    Fetches the list of projects from the Bitbucket server, handling pagination.
    
    Args:
        server_url (str): The URL of the Bitbucket server.
        username (str): The username for authentication.
        password (str): The password for authentication.
    
    Returns:
        list: A list of projects.
    """
    projects = []
    url = f"{server_url}/rest/api/1.0/projects"
    auth = HTTPBasicAuth(username, password)
    params = {'limit': 100}  # Adjust the limit as needed

    while url:
        logging.info(f"Fetching projects from {url}")
        response = requests.get(url, auth=auth, params=params, verify=False)
        
        if response.status_code == 200:
            data = response.json()
            projects.extend(data.get('values', []))
            logging.info(f"Fetched {len(data.get('values', []))} projects")
            url = data.get('nextPageStart', None)
            if url:
                url = f"{server_url}/rest/api/1.0/projects?start={url}"
        else:
            logging.error(f"Failed to fetch projects. Status code: {response.status_code}, Response: {response.text}")
            break

    logging.info(f"Total projects fetched: {len(projects)}")
    return projects

def get_repo_count(server_url, username, password, project_key):
    """
    Fetches the number of repositories for a given project.
    
    Args:
        server_url (str): The URL of the Bitbucket server.
        username (str): The username for authentication.
        password (str): The password for authentication.
        project_key (str): The key of the project.
    
    Returns:
        int: The number of repositories in the project.
    """
    url = f"{server_url}/rest/api/1.0/projects/{project_key}/repos"
    auth = HTTPBasicAuth(username, password)
    params = {'limit': 100}  # Adjust the limit as needed
    repo_count = 0

    while url:
        logging.info(f"Fetching repositories from {url}")
        response = requests.get(url, auth=auth, params=params, verify=False)
        
        if response.status_code == 200:
            data = response.json()
            repo_count += len(data.get('values', []))
            url = data.get('nextPageStart', None)
            if url:
                url = f"{server_url}/rest/api/1.0/projects/{project_key}/repos?start={url}"
        else:
            logging.error(f"Failed to fetch repositories. Status code: {response.status_code}, Response: {response.text}")
            break

    logging.info(f"Total repositories fetched for project {project_key}: {repo_count}")
    return repo_count

def save_projects_to_excel(projects, filename):
    """
    Saves the list of projects to an Excel file.
    
    Args:
        projects (list): The list of projects.
        filename (str): The name of the output Excel file.
    """
    scan_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for project in projects:
        project['scanDate'] = scan_date
    df = pd.DataFrame(projects)
    df.to_excel(filename, index=False)
    logging.info(f"Projects data saved to {filename}")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Fetch Bitbucket projects.')
    parser.add_argument('--bitbucket_url', required=True, help='Bitbucket server URL')
    parser.add_argument('--username', required=True, help='Username for authentication')
    parser.add_argument('--password', required=True, help='Password for authentication')
    parser.add_argument('--output', required=True, help='Output Excel file name')

    # Parse arguments
    args = parser.parse_args()

    # Fetch projects
    projects = get_projects(args.bitbucket_url, args.username, args.password)
    
    # Fetch repository count for each project
    for project in projects:
        project_key = project['key']
        repo_count = get_repo_count(args.bitbucket_url, args.username, args.password, project_key)
        project['repoCount'] = repo_count
    
    # Save projects to Excel if any projects were fetched
    if projects:
        save_projects_to_excel(projects, args.output)
    else:
        logging.warning("No projects to save")