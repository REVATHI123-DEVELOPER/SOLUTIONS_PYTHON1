import requests
from requests.auth import HTTPBasicAuth


# Get all the PR  from a given repo & branch
def get_all_prs_for_repo(
    organization: str,
    project: str,
    repo_id: str,
    branch: str,
    skip: int,
    top: int,
    pat: str,
) -> list:

    url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}/pullrequests?searchCriteria.targetRefName=refs/heads/{branch}&searchCriteria.status=completed&$top={top}&$skip={skip}&api-version=7.1"
    auth = HTTPBasicAuth("", pat)

    loop = 0
    while True:
        loop += 1
        response = requests.get(url, auth=auth, timeout=60)
        if response.status_code == 200:
            response = response.json()
            return response["value"]
        elif response.status_code == 503:  # Unavailable
            print("Service Unavailable")
        elif response.status_code not in [200, 503] or loop > 10:
            print(url)
            print(response.status_code)
            print(response.text)
            raise Exception("Failed to get PR details")


# This method gets the details for all the workitems for a given pull_request
def get_associated_workitems_for_pull_request(
    organization: str, project: str, repo_id: str, pull_request_id: int, pat: str
) -> list:
    url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}/pullRequests/{pull_request_id}/workitems?api-version=7.1"
    auth = HTTPBasicAuth("", pat)

    loop = 0
    while True:
        loop += 1
        response = requests.get(url, auth=auth, timeout=60)
        if response.status_code == 200:
            response = response.json()
            return response["value"]
        elif response.status_code == 503:  # Unavailable
            print("Service Unavailable")
        elif response.status_code not in [200, 503] or loop > 10:
            print(url)
            print(response.status_code)
            print(response.text)
            raise Exception("Failed to get workitem details")


# Gives the details of all given work items
def get_workitem_details(ids: list, organization: str, project: str, pat: str) -> list:

    url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitemsbatch?api-version=7.1"
    payload = {
        "ids": ids,
        "fields": [
            "System.Id",
            "System.Title",
            "System.WorkItemType",
            "Custom.Feature",
            "System.State",
            "Custom.ExistsInProd",
            "Custom.ExistsInSit2",
            "Custom.Recidivistic",
            "Custom.TargetRelease",
            "Microsoft.VSTS.Common.Priority",
            "Microsoft.VSTS.Common.Severity",
            "Custom.Reporter",
            "Custom.ReporterRole",
            "System.CreatedDate",
            "System.ChangedDate",
        ],
    }
    auth = HTTPBasicAuth("", pat)

    loop = 0
    while True:
        loop += 1
        response = requests.post(url, json=payload, auth=auth, timeout=60)
        if response.status_code == 200:
            response = response.json()
            return response["value"]
        elif response.status_code == 503:  # Unavailable
            print("Service Unavailable")
        elif response.status_code not in [200, 503] or loop > 10:
            print(url)
            print(response.status_code)
            print(response.text)
            raise Exception("Failed to get batch workitem detail")


# Get a specific PR details
def get_pr_details(pr_id, organization, project, repo_id, pat):
    url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}/pullRequests/{pr_id}/?api-version=7.1"
    auth = HTTPBasicAuth("", pat)

    loop = 0
    while True:
        loop += 1
        response = requests.get(url, auth=auth, timeout=60)
        if response.status_code == 200:
            response = response.json()
            return response
        elif response.status_code == 503:  # Unavailable
            print("Service Unavailable")
        elif response.status_code not in [200, 503] or loop > 10:
            print(url)
            print(response.status_code)
            raise Exception("Failed to get iteration details")


# Get all the iterations for a PR
def get_pr_iterations(pr_id, organization, project, repo_id, pat):
    url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}/pullRequests/{pr_id}/iterations?api-version=7.1"
    auth = HTTPBasicAuth("", pat)

    loop = 0
    while True:
        loop += 1
        response = requests.get(url, auth=auth, timeout=60)
        if response.status_code == 200:
            response = response.json()
            return response["value"]
        elif response.status_code == 503:  # Unavailable
            print("Service Unavailable")
        elif response.status_code not in [200, 503] or loop > 10:
            print(url)
            print(response.status_code)
            raise Exception("Failed to get iteration details")


# Get all changes from beginning to that specified iteration
def get_files_for_iteration(
    pr_id, iteration_id, organization, project, repo_id, pat, top=50
):
    url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_id}/pullRequests/{pr_id}/iterations/{iteration_id}/changes?api-version=7.1"
    auth = HTTPBasicAuth("", pat)

    changes = []
    skip = 0

    loop = 0
    while True:
        loop += 1
        paginated_url = f"{url}&$top={top}&$skip={skip}"
        response = requests.get(paginated_url, auth=auth, timeout=60)

        if response.status_code == 200:
            data = response.json()
            change_entries = data.get("changeEntries", [])
            changes.extend(change_entries)

            # If the number of returned items is less than the 'top', we have reached the last page
            if len(change_entries) < top:
                break
            else:
                # Otherwise, increment the skip value and keep paginating
                skip += top
        elif response.status_code == 503:  # Unavailable
            print("Service Unavailable")
        elif response.status_code not in [200, 503] or loop > 10:
            print(paginated_url)
            print(response.status_code)
            raise Exception("Failed to get change details")

    return changes


def add_comment_to_workitem(workitem_id, comment, organization, project, pat):
    url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workItems/{workitem_id}/comments?format=html&api-version=7.2-preview.4"
    auth = HTTPBasicAuth("", pat)
    payload = {"text": comment}

    loop = 0
    while True:
        loop += 1
        response = requests.post(url, json=payload, auth=auth, timeout=60)
        if response.status_code == 200:
            response = response.json()
            return response
        elif response.status_code == 503:  # Unavailable
            print("Service Unavailable")
        elif response.status_code not in [200, 503] or loop > 10:
            print(url)
            print(response.status_code)
            raise Exception("Failed to add comment")
