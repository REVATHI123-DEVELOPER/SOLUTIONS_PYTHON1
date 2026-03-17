import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from ado_apis import apis
from data import ado_data
from db import db_exec_pr_workitem_details

# Create a global lock object to ensure only one thread writes to db
db_lock = threading.Lock()


def run():
    """
    This wrapper function finds the mapping between the PRs and the workitems.
    To optimize run time, only the new PRs are processed, and multiple repositories are handled concurrently.
    """

    pat = ado_data.pat
    organization = ado_data.organization
    project = ado_data.project
    repo_details = ado_data.repo_details

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(__process_repo, repo_detail, organization, project, pat)
            for repo_detail in repo_details
        ]
        for future in as_completed(futures):
            future.result()  # propagate any exceptions

    # Identify all PRs in DB that dont have workitems and check if new workitems are associated
    # __process_prs_without_workitems(organization, project, pat)


def __process_repo(repo_detail, organization, project, pat):
    """
    Process a single repository:
      1. Get new PRs.
      2. For each new PR, get associated workitems.
      3. Populate the DB with the PR - Workitem mapping.
    """
    new_pr_details = {}
    repo_id = repo_detail["repo_id"]
    repo_name = repo_detail["repo_name"]
    branch = repo_detail["branch_name"]

    __get_new_pr_details(
        new_pr_details, organization, project, repo_id, repo_name, branch, pat
    )
    __get_work_items_for_new_prs(
        new_pr_details, organization, project, repo_id, repo_name, branch, pat
    )
    with db_lock:
        __insert_into_db(new_pr_details)


def __process_prs_without_workitems(organization, project, pat) -> None:
    """
    Process a single repository:
      1. Get all PRs in the DB that dont have associated workitems.
      2. For each PR without associated workitem, check if workitem is associated.
      3. Populate the DB with the PR - Workitem mapping.
    """
    pr_details = {}
    __check_new_workitems_for_prs_without_workitems(
        pr_details, organization, project, pat
    )
    __update_db(pr_details)


def __insert_into_db(pr_details: dict) -> None:
    for pr_id in pr_details.keys():
        repo_id = pr_details[pr_id]["repo_id"]
        workitems = pr_details[pr_id]["workitems"]
        # If workitems are not attached to the PR, then make it as None
        if len(workitems) == 0:
            db_exec_pr_workitem_details.insert_into_pr_workitem_details_table(
                pr_id, repo_id, None
            )
        else:
            for workitem in workitems:
                db_exec_pr_workitem_details.insert_into_pr_workitem_details_table(
                    pr_id, repo_id, workitem
                )


def __update_db(pr_details: dict) -> None:
    for pr_id in pr_details.keys():
        workitems = pr_details[pr_id]["workitems"]
        # If workitems are not attached to the PR, then dont do anythng
        if len(workitems) == 0:
            pass
        else:
            for workitem in workitems:
                db_exec_pr_workitem_details.update_pr_workitem_details_table(
                    pr_id, workitem
                )


def __get_new_pr_details(
    pr_details: dict,
    organization: str,
    project: str,
    repo_id: str,
    repo_name: str,
    branch: str,
    pat: str,
) -> None:

    print("[❕] Getting new PRs on Repo:{} Branch {}".format(repo_name, branch))
    start_time = time.time()

    # Keep track of already_processed_pr_ids so that we know when to stop processing
    already_processed_pr_ids = db_exec_pr_workitem_details.get_all_pr_ids()

    # ADO has a make page length for 101. So we implement pagination to get all the PRs.
    top = 100  # Number of items per page.
    skip = 0  # Number of items to skip (start at 0).

    while True:
        _pr_details: list = apis.get_all_prs_for_repo(
            organization, project, repo_id, branch, skip, top, pat
        )
        for _pr_detail in _pr_details:
            _pr_id = _pr_detail["pullRequestId"]
            # When even one PR is already processed, we stop assuming that all PRs after that will also be processed since we go from the recent PRs to the old
            if _pr_id not in already_processed_pr_ids:
                pr_details[_pr_id] = {"repo_id": repo_id}
            else:
                # set __pr_details = [] so that it will break the loop
                _pr_details = []
                break

        if not _pr_details:
            # No more pull requests returned; exit the loop.
            break

        skip += top
        print(
            "--- DEBUG: Collected {} PRs on Repo:{} Branch:{}".format(
                skip, repo_name, branch
            )
        )

    end_time = time.time()
    print(
        "[✅] Completed getting {} new PRs on Repo:{} Branch:{} in {} seconds".format(
            len(pr_details.keys()), repo_name, branch, end_time - start_time
        )
    )


def __get_work_items_for_new_prs(
    pr_details: dict,
    organization: str,
    project: str,
    repo_id: str,
    repo_name: str,
    branch: str,
    pat: str,
) -> None:
    print(
        "[❕] Getting related workitems for {} new PRs on Repo:{} Branch:{}".format(
            len(pr_details), repo_name, branch
        )
    )
    start_time = time.time()

    def fetch_workitems(pull_request_id: str):
        # Fetch associated workitems for a given PR ID.
        workitem_details = apis.get_associated_workitems_for_pull_request(
            organization, project, repo_id, pull_request_id, pat
        )
        # Remove duplicates using a set comprehension. This does not preserve order.
        workitem_ids = list({workitem["id"] for workitem in workitem_details})
        return pull_request_id, workitem_ids

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_workitems, pr_id): pr_id
            for pr_id in pr_details.keys()
        }
        count = 0
        for future in as_completed(futures):
            pr_id, workitem_ids = future.result()
            if pr_id not in pr_details:
                pr_details[pr_id] = {}
            pr_details[pr_id]["workitems"] = workitem_ids
            count += 1
            if count % 50 == 0:
                print(
                    "--- DEBUG: Collected related workitems for {} PRs on Repo:{} Branch:{}".format(
                        count, repo_name, branch
                    )
                )

    end_time = time.time()
    print(
        "[✅] Completed getting related workitems for {} new PRs on Repo:{} Branch:{} in {} seconds".format(
            len(pr_details), repo_name, branch, end_time - start_time
        )
    )


def __check_new_workitems_for_prs_without_workitems(
    pr_details: dict, organization: str, project: str, pat: str
) -> None:
    all_pr_without_workitems = (
        db_exec_pr_workitem_details.get_all_pr_ids_without_workitems_associated()
    )

    print(
        "[❕] Checking for related workitems for {} PRs without previously mapped workitems".format(
            len(all_pr_without_workitems)
        )
    )

    start_time = time.time()

    def fetch_workitems(pull_request_id: str):
        repo_id = db_exec_pr_workitem_details.get_repo_id_for_pr(pull_request_id)

        # Fetch associated workitems for a given PR ID.
        workitem_details = apis.get_associated_workitems_for_pull_request(
            organization, project, repo_id, pull_request_id, pat
        )
        # Remove duplicates using a set comprehension. This does not preserve order.
        workitem_ids = list({workitem["id"] for workitem in workitem_details})
        return pull_request_id, workitem_ids

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_workitems, pr_id): pr_id
            for pr_id in all_pr_without_workitems
        }
        count = 0
        for future in as_completed(futures):
            pr_id, workitem_ids = future.result()
            if pr_id not in pr_details:
                pr_details[pr_id] = {}
            pr_details[pr_id]["workitems"] = workitem_ids
            count += 1
            if count % 50 == 0:
                print(
                    "--- DEBUG: Checked for related workitems for {} PRs without workitem mapping".format(
                        count,
                    )
                )

    end_time = time.time()
    print(
        "[✅] Completed checking for related workitems for {} PRs without previously mapped workitems in {} seconds".format(
            len(pr_details), end_time - start_time
        )
    )
