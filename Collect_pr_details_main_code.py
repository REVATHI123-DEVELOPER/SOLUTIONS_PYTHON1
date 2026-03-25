import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Import modules for API calls, configuration data, and database operations.
from ado_apis import apis
from data import ado_data
from db import db_exec_pr_details, db_exec_pr_workitem_details

# Create a global lock object to ensure only one thread writes to db
db_lock = threading.Lock()


def run():
    """
    This wrapper function will find all the unprocessed PRs and get the details of the PRs
    """
    pat = ado_data.pat
    organization = ado_data.organization
    project = ado_data.project

    __get_pr_details(organization, project, pat)


def __populate_db(
    pull_request_id: str,
    closed_date: str,
    source_ref_commit_id: str,
    target_ref_commit_id: str,
    files: list,
):
    """
    Inserts details for a given PR into the database.

    For each file in the list of changed files, this function:
      - Extracts the file path and original file path (if available).
      - Determines the file types (extensions) based on the file paths.
      - Calls the database function to insert the details into the pr_details table.

    Parameters:
      pull_request_id (str): The ID of the pull request.
      closed_date (str): The date when the PR was closed.
      source_ref_commit_id (str): The commit ID of the source reference from the last iteration.
      target_ref_commit_id (str): The commit ID of the target (or common) reference from the last iteration.
      files (list): A list of dictionaries, each representing a file change. Each dictionary is expected
                    to contain at least "change_type" and optionally "file_path" and "original_file_path".
    """
    # Iterate over each file detected in the PR.
    for file in files:
        # Extract file paths if available. Use None as default if the key is missing.
        file_path = file.get("file_path", None)
        original_file_path = file.get("original_file_path", None)
        change_type = file["change_type"]

        # Determine the file extension (type) for the current file_path, if it exists.
        file_type = None
        if file_path:
            file_type = Path(file_path).suffix

        # Determine the file extension (type) for the original_file_path, if it exists.
        original_file_type = None
        if original_file_path:
            original_file_type = Path(original_file_path).suffix

        # Insert the PR file details into the pr_details table.
        db_exec_pr_details.insert_into_pr_details_table(
            pull_request_id,
            closed_date,
            source_ref_commit_id,
            target_ref_commit_id,
            file_path,
            original_file_path,
            file_type,
            original_file_type,
            change_type,
        )


def __get_pr_details(organization: str, project: str, pat: str) -> None:
    """
    Fetches and processes details for PRs that have not yet been processed

    The function performs the following steps:
      1. Retrieve all PR IDs from the pr_workitem_details table that has workitems associated.
      2. Retrieve all PR IDs from the pr_details table for which details have already been collected.
      3. Calculate the set difference to obtain PR IDs that still require detail processing.
      4. For each PR needing processing:
          - Retrieves the corresponding repository ID.
          - Fetches the PR details using an API call.
          - Fetches the file details from the last iteration of the PR.
          - Extracts necessary information (e.g., closed date, commit IDs) and then populates the database.

    Concurrency:
      - Uses a ThreadPoolExecutor to process multiple PRs concurrently to improve performance.
      - Debug messages are printed every 50 processed PRs.

    Parameters:
      organization (str): The Azure DevOps organization name.
      project (str): The project name.
      pat (str): The Personal Access Token used for authentication.
    """
    # Retrieve all PR IDs that have a workitem mapping.
    all_pull_request_ids = (
        db_exec_pr_workitem_details.get_all_pr_ids_with_workitems_associated()
    )

    # Retrieve all PR IDs for which the details have already been processed.
    processed_pull_request_ids = db_exec_pr_details.get_all_pr_ids()

    # Determine the PR IDs that still need to have details identified.
    unprocessed_pull_request_ids = list(
        set(all_pull_request_ids) - set(processed_pull_request_ids)
    )

    print("[❕] Getting details for {} PRs".format(len(unprocessed_pull_request_ids)))
    print(
        "--- Some PRs do not have any files and therefore will constantly keep showing up here ---"
    )
    start_time = time.time()

    def process_pr(pr_id: str):
        """
        Processes a single PR by performing the following:
          1. Retrieve the repository ID for the PR.
          2. Fetch the PR details using the API.
          3. Fetch the details of files (from the last iteration) for the PR.
          4. Extract key information such as closed date and commit IDs.
          5. Call __populate_db to insert the details into the database.

        Parameters:
          pr_id (str): The ID of the pull request to process.

        Returns:
          pr_id (str): The same PR ID after processing (used for progress tracking).
        """
        # Get the repository ID for the given PR from the workitem details table.
        repo_id = db_exec_pr_workitem_details.get_repo_id_for_pr(pr_id)

        # Retrieve PR details using the API.
        pr_details = apis.get_pr_details(pr_id, organization, project, repo_id, pat)

        # Retrieve details of files for the PR by fetching the last iteration details.
        last_iteration_detail, changed_files = __get_pr_file_details(
            pr_id, organization, project, repo_id, pat
        )

        # Extract the closed date from the PR details.
        closed_date = pr_details["closedDate"]

        # Extract commit IDs from the last iteration details.
        source_ref_commit_id = last_iteration_detail["sourceRefCommit"]["commitId"]
        target_ref_commit_id = last_iteration_detail["commonRefCommit"]["commitId"]

        # Populate the database with the collected change details for this PR.
        with db_lock:
            __populate_db(
                pr_id,
                closed_date,
                source_ref_commit_id,
                target_ref_commit_id,
                changed_files,
            )
        return pr_id  # Return the processed PR ID for progress tracking.

    # Use a ThreadPoolExecutor to process multiple PRs concurrently.
    count = 0  # Counter for processed PRs.
    with ThreadPoolExecutor() as executor:
        # Submit a separate thread for each PR that needs processing.
        futures = {
            executor.submit(process_pr, pr_id): pr_id
            for pr_id in unprocessed_pull_request_ids
        }
        # As each thread completes, update the count and print progress.
        for future in as_completed(futures):
            future.result()  # This call propagates any exceptions raised during processing.
            count += 1
            if count % 50 == 0:
                print("DEBUG --- Collected details for {} PRs".format(count))

    end_time = time.time()
    print(
        "[✅] Completed getting details for {} PRs in {} seconds".format(
            len(unprocessed_pull_request_ids), end_time - start_time
        )
    )


def __get_pr_file_details(pull_request_id, organization, project, repo_id, pat):
    """
    Retrieves the file details for a single PR.

    Since a PR can have multiple iterations (commits), this function:
      1. Retrieves all iteration details for the PR using an API call.
      2. Selects the last iteration (assumed to be the most recent set of changes).
      3. Fetches the files  for that iteration.
      4. Processes each file to capture file paths and change types.
         - For "rename" chage_type, both the new and original file paths are captured.
         - For "delete" change_type, only the original file path is captured.
         - For other change_types, only the current file path is captured.

    Parameters:
      pull_request_id: The ID of the pull request.
      organization (str): The Azure DevOps organization name.
      project (str): The project name.
      repo_id (str): The repository ID associated with the PR.
      pat (str): The Personal Access Token for authentication.

    Returns:
      tuple: A tuple containing:
             - last_iteration_detail: The details of the last iteration of the PR.
             - changed_files: A list of dictionaries, each representing a file change.
    """
    # Retrieve all iterations (commits) for the PR.
    iteration_details: list = apis.get_pr_iterations(
        pull_request_id, organization, project, repo_id, pat
    )
    # Assume that the last iteration in the list is the most recent.
    last_iteration_detail = iteration_details[-1]
    last_iteration_id = last_iteration_detail["id"]

    # Retrieve the list of file for the last iteration.
    files = apis.get_files_for_iteration(
        pull_request_id, last_iteration_id, organization, project, repo_id, pat
    )

    # Process each change to construct a list of file change details.
    changed_files = []
    for file in files:
        change_type = file["changeType"]
        file_path = file["item"]["path"]

        # Depending on the change type, prepare the file change dictionary.
        if "rename" in change_type:
            # For rename operations, both the new file path and the original file path are provided.
            changed_files.append(
                {
                    "file_path": file_path,
                    "original_file_path": file["originalPath"],
                    "change_type": change_type,
                }
            )
        elif change_type == "delete":
            # For deletions, the new file path is not applicable; only the original file path is relevant.
            changed_files.append(
                {
                    "file_path": None,
                    "original_file_path": file["originalPath"],
                    "change_type": change_type,
                }
            )
        else:
            # For other change types (e.g., edits), only the new file path is provided.
            changed_files.append(
                {
                    "file_path": file_path,
                    "original_file_path": None,
                    "change_type": change_type,
                }
            )

    # Return the last iteration detail and the list of changed file details.
    return last_iteration_detail, changed_files
