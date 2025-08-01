# Azure DevOps Inventory Scripts

A collection of Python scripts to collect comprehensive inventory data from Azure DevOps organizations, including user information and project/repository details.

## Scripts Overview

### 1. collect-ado-users.py
Collects user information from Azure DevOps organizations including organizational users, project members, and project administrators.

### 2. collect-ado-inventory.py
Collects project and repository information including version control details, repository sizes, file counts, and activity metrics.

## Features

### User Collection Script
- **Organizational Users**: Extracts active users with proper entitlements
- **Project Members**: Identifies team members for each project
- **Project Administrators**: Detects project administrators using multiple methods
- **Multi-API Approach**: Uses VSAEX, Graph, and membership APIs for comprehensive user discovery
- **Service Account Filtering**: Automatically filters out build services and system accounts
- **Real User Focus**: Prioritizes actual human users over automated accounts

### Inventory Collection Script
- **Version Control Detection**: Automatically identifies Git, TFVC, or file storage projects
- **Repository Statistics**: Calculates accurate sizes, file counts, and largest file metrics
- **Branch Information**: Counts Git repository branches
- **Activity Tracking**: Records last modification times for repositories
- **Dual System Support**: Handles projects with both Git and TFVC
- **Size Optimization**: Uses multiple methods to get accurate repository sizes

## Prerequisites

- Python 3.7 or higher
- Required Python packages:
  ```bash
  pip install requests
  ```
- Azure DevOps Personal Access Token (PAT) with appropriate permissions

## Setup

### 1. Environment Variables
Set your Azure DevOps Personal Access Token:

```bash
# Linux/macOS
export ADO_PAT="your_personal_access_token_here"

# Windows PowerShell
$env:ADO_PAT="your_personal_access_token_here"

# Windows Command Prompt
set ADO_PAT=your_personal_access_token_here
```

### 2. Organization List File
Create a text file containing your Azure DevOps organization names (one per line):

```text
organization1
organization2
organization3
# Comments start with # and are ignored
```

## Usage

### Basic Usage

```bash
# Collect user information
python collect-ado-users.py organizations.txt

# Collect inventory information
python collect-ado-inventory.py organizations.txt
```

### Advanced Usage with Custom Threading

```bash
# Custom parallel processing settings
python collect-ado-users.py organizations.txt --max-org-workers 3 --max-project-workers 2
python collect-ado-inventory.py organizations.txt --max-org-workers 3 --max-project-workers 2
```

## Command Line Options

Both scripts support the following options:

| Option | Default | Description |
|--------|---------|-------------|
| `--max-org-workers` | 5 | Maximum parallel organization workers |
| `--max-project-workers` | 3 | Maximum parallel project workers |

## Output Files

### User Collection Output
- **CSV File**: `ADO-Users-YYYYMMDD-HHMMSS.csv`
- **Log File**: `ADO-Users-YYYYMMDD-HHMMSS.log`

#### CSV Columns:
- Organization
- Project
- Repo Name
- Organizational Users
- Project Members
- Project Admins
- Last User Modified Repo
- Last Modified Timestamp

### Inventory Collection Output
- **CSV File**: `ADO-Inventory-YYYYMMDD-HHMMSS.csv`
- **Log File**: `ADO-Inventory-YYYYMMDD-HHMMSS.log`

#### CSV Columns:
- Organization
- Project
- Project Type (GIT/TFVC/FILE_STORAGE/ARTIFACTS/WIKI)
- Repo Name
- No of Branches of Repo
- Total Repo Size (KB)
- Total Repo Size (MB)
- No of Files in Repo
- Largest File Size in Repo (KB)
- Last Modified Time of Repo

## Performance Optimization

### Threading Configuration
The scripts use parallel processing to improve performance:

- **Organization Level**: Process multiple organizations simultaneously
- **Project Level**: Process multiple projects within an organization simultaneously
- **Thread Safety**: All output and logging operations are thread-safe

### Recommended Settings
For optimal performance based on your Azure DevOps environment:

```bash
# Small organizations (1-10 projects each)
python script.py orgs.txt --max-org-workers 5 --max-project-workers 5

# Medium organizations (10-50 projects each)
python script.py orgs.txt --max-org-workers 3 --max-project-workers 3

# Large organizations (50+ projects each)
python script.py orgs.txt --max-org-workers 2 --max-project-workers 2
```

## Error Handling

### Common Issues and Solutions

#### 1. Authentication Errors
```
Error: ADO_PAT environment variable not set.
```
**Solution**: Set the ADO_PAT environment variable with your Personal Access Token.

#### 2. Permission Issues
```
API returned 403 - insufficient permissions
```
**Solution**: Ensure your PAT has the following scopes:
- Project and team (read)
- Identity (read)
- Code (read)
- User profile (read)

#### 3. Network Timeouts
The scripts include automatic retry logic and timeout handling for network issues.

### Logging
Detailed logs are created for each execution, containing:
- API call details
- Error messages and stack traces
- Processing statistics
- Performance metrics

## API Rate Limiting

The scripts are designed to respect Azure DevOps API rate limits:
- Built-in delays between API calls
- Parallel processing limits to prevent overwhelming the APIs
- Automatic retry logic for rate-limited requests

## Security Considerations

- **Token Security**: Never commit PAT tokens to version control
- **Scope Limitation**: Use minimal required permissions for PAT tokens
- **Log Privacy**: Review log files before sharing as they may contain user information

## Troubleshooting

### Debug Mode
For debugging issues, you can monitor the log files in real-time:

```bash
# Linux/macOS
tail -f ADO-Users-*.log

# Windows PowerShell
Get-Content ADO-Users-*.log -Wait
```

### Common Solutions

1. **Empty Results**: Check PAT permissions and organization access
2. **Slow Performance**: Reduce thread counts or check network connectivity
3. **Memory Issues**: Process fewer organizations at once or reduce thread counts

## Examples

### Sample Organization File (organizations.txt)
```text
contoso
fabrikam
northwind
# Test organizations
test-org-1
test-org-2
```

### Sample Execution
```bash
$ python collect-ado-users.py organizations.txt
Logging initialized. Log file: ADO-Users-20241201-143022.log
Loaded 3 organizations from file: organizations.txt
ADO_PAT token loaded successfully
Starting parallel processing at 2024-12-01 14:30:22

Processing organization: contoso
==================================================
  Getting organizational users for contoso...
    Found 25 user entitlements via VSAEX API
  Processing project: ContosoApp
  Getting project members for ContosoApp...
    Found 2 teams in project ContosoApp
      Team 'ContosoApp Team': 8 members

============================================================
EXECUTION SUMMARY
============================================================
Data exported to: ADO-Users-20241201-143045.csv
Log file: ADO-Users-20241201-143022.log
Total records: 45
Organizations processed: 3
Total execution time: 23.45 seconds
Average time per organization: 7.82 seconds
Parallel processing completed at 2024-12-01 14:30:45
```

## Contributing

When contributing to these scripts:

1. Maintain thread safety for all shared operations
2. Add appropriate error handling and logging
3. Test with various organization sizes and configurations
4. Update documentation for any new features or parameters

## License

These scripts are provided as-is for Azure DevOps inventory collection purposes.