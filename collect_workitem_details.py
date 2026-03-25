import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from ado_apis import apis
from data import ado_data
from db import db_exec_pr_workitem_details, db_exec_workitem_details


def run():
    """
    Main wrapper function that updates the workitem_details table in the database.

    Steps:
        1. Retrieve configuration values (PAT, organization, project) from the data module.
        2. Fetch detailed information for all workitems using the __get_workitem_details function.
        3. Update the database with the fetched workitem details via the __populate_db function.
    """
    pat = ado_data.pat
    organization = ado_data.organization
    project = ado_data.project

    workitem_details = __get_workitem_details(organization, project, pat)
    __populate_db(workitem_details)


def __populate_db(workitem_details: dict):
    """
    Inserts or updates workitem details in the database concurrently.

    For each workitem, the function extracts relevant fields and uses a thread pool to
    concurrently execute the upsert operation to the database.

    Parameters:
        workitem_details (dict): A dictionary mapping workitem IDs to their detail fields.
                                  Expected keys in each details dictionary include:
                                  "System.Title", "System.WorkItemType", "System.State",
                                  "System.CreatedDate", and "System.ChangedDate".
    """
    start_time = time.time()
    print("[❕] Populating DB with Workitem details")

    db_exec_workitem_details.truncate_workitem_details_table()
    for workitem_id, details in workitem_details.items():
        # Extract the necessary fields for the upsert.
        title = details["System.Title"]
        # Use a different variable name ('workitem_type') instead of 'type' to avoid shadowing the built-in.
        workitem_type = details["System.WorkItemType"]

        feature = None
        if "Custom.Feature" in details:
            feature = details["Custom.Feature"]

        target_release = None
        if "Custom.TargetRelease" in details:
            target_release = details["Custom.TargetRelease"]

        state = details["System.State"]
        created_date = details["System.CreatedDate"]
        changed_date = details["System.ChangedDate"]

        exists_in_prod = False
        if "Custom.ExistsInProd" in details:
            exists_in_prod = details["Custom.ExistsInProd"]

        exists_in_sit = False
        if "Custom.ExistsInSit2" in details:
            exists_in_sit = details["Custom.ExistsInSit2"]

        is_recidivistic = False
        if "Custom.Recidivistic" in details:
            is_recidivistic = details["Custom.Recidivistic"]

        priority = None
        if "Microsoft.VSTS.Common.Priority" in details:
            priority = details["Microsoft.VSTS.Common.Priority"]

        severity = None
        if "Microsoft.VSTS.Common.Severity" in details:
            severity = details["Microsoft.VSTS.Common.Severity"]

        reporter = None
        if "Custom.Reporter" in details:
            reporter = details["Custom.Reporter"]

        reporter_role = None
        if "Custom.ReporterRole" in details:
            reporter_role = details["Custom.ReporterRole"]

        # Submit the upsert operation as a task to the thread pool.
        db_exec_workitem_details.insert_into_workitem_details_table(
            workitem_id,
            title,
            workitem_type,
            severity,
            priority,
            feature,
            target_release,
            state,
            exists_in_sit,
            exists_in_prod,
            is_recidivistic,
            reporter,
            reporter_role,
            created_date,
            changed_date,
        )

    end_time = time.time()
    print(
        "[✅] Completed populating DB with Workitem details in {} seconds".format(
            end_time - start_time
        )
    )


def __get_workitem_details(organization: str, project: str, pat: str) -> dict:
    """
    Retrieves detailed information for each workitem from ADO.

    Steps:
        1. Fetch all workitem IDs from the pr_workitem_details table.
        2. Create chunks of these IDs for efficient API requests.
        3. Use a thread pool to concurrently request workitem details for each chunk.
        4. Aggregate the details into a single dictionary.

    Parameters:
        organization (str): The Azure DevOps organization name.
        project (str): The project name.
        pat (str): The Personal Access Token for authentication.

    Returns:
        dict: A dictionary mapping each workitem ID to its detail fields.
    """
    # Get a list of all workitem IDs from the database.
    workitems_ids = db_exec_pr_workitem_details.get_all_workitem_ids()

    print("[❕] Getting workitems details for {} workitems".format(len(workitems_ids)))
    start_time = time.time()

    workitem_details = {}  # Initialize an empty dictionary to store the results.

    # Create chunks of workitem IDs. Here we use a chunk size of 100 for each API request.
    chunks = list(__get_chunks(workitems_ids, chunk_size=100))

    # Use a thread pool to fetch details for each chunk concurrently.
    with ThreadPoolExecutor() as executor:
        # Submit a task for each chunk using the helper function __fetch_chunk.
        futures = [
            executor.submit(__fetch_chunk, chunk, organization, project, pat)
            for chunk in chunks
        ]
        # As each future completes, process its result.
        for future in as_completed(futures):
            # Each future returns a list of workitem detail dictionaries.
            chunk_details = future.result()
            # Aggregate the returned details into the workitem_details dictionary.
            for detail in chunk_details:
                # The key is the workitem ID and the value is the corresponding fields.
                workitem_details[detail["id"]] = detail["fields"]

    end_time = time.time()
    print(
        "[✅] Completed getting workitem details for {} workitems in {} seconds".format(
            len(workitems_ids), end_time - start_time
        )
    )
    return workitem_details


def __fetch_chunk(chunk, organization: str, project: str, pat: str) -> list:
    """
    Helper function that fetches workitem details for a given chunk of workitem IDs.

    Parameters:
        chunk (list): A list of workitem IDs to fetch details for.
        organization (str): The Azure DevOps organization name.
        project (str): The project name.
        pat (str): The Personal Access Token for authentication.

    Returns:
        list: A list of dictionaries, each containing details for a workitem.
    """
    # Call the API to retrieve workitem details for the provided chunk.
    return apis.get_workitem_details(chunk, organization, project, pat)


def __get_chunks(numbers, chunk_size=200):
    """
    Generator function that yields successive chunks of a given list.

    Parameters:
        numbers (list): The list of items to be divided into chunks.
        chunk_size (int): The maximum number of items per chunk.

    Yields:
        list: A slice of the original list with length up to chunk_size.
    """
    # Iterate over the list, yielding slices of size 'chunk_size'.
    for i in range(0, len(numbers), chunk_size):
        yield numbers[i : i + chunk_size]


if __name__ == "__main__":
    run()
