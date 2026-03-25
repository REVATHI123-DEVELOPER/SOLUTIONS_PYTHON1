import time

from data import ado_data
from db import db_exec_pr_changes, db_exec_pr_details


def run():
    """
    This wrapper function is responsible for fiding the changes for the PRs
    """

    pat = ado_data.pat
    organization = ado_data.organization
    project = ado_data.project

    __construct_changes(organization, project, pat)


def __construct_changes(organization: str, project: str, pat: str):

    # Get all the PR Ids in the order they were closed
    all_pr_ids = db_exec_pr_details.get_all_pr_ids(closed_date_asc=True)

    # Get all the processed PR Ids for which the change has been computed
    processed_pr_ids = db_exec_pr_changes.get_all_pr_ids()

    # Find all the PR Ids for which the change needs to be computed
    unprocessed_pr_ids = [_ for _ in all_pr_ids if _ not in processed_pr_ids]

    print("[❕] Getting change details for {} PRs".format(len(unprocessed_pr_ids)))
    start_time = time.time()
    count = 0
    for unprocessed_pr_id in unprocessed_pr_ids:

        pr_detail_rows = db_exec_pr_details.get_pr_detail(unprocessed_pr_id)
        for pr_detail_row in pr_detail_rows:
            # pr_id = pr_details_row[0]
            closed_date = pr_detail_row[1]
            # source_ref_commit_id = pr_details_row[2]
            # target_ref_commit_id = pr_details_row[3]
            file_path = pr_detail_row[4]
            original_file_path = pr_detail_row[5]
            change_type = pr_detail_row[8]

            if change_type == "add":
                # If change_type is add means that a new file has been added so
                # add it to the pr_changes table
                db_exec_pr_changes.insert_into_pr_changes_table(
                    unprocessed_pr_id, closed_date, file_path, None, None
                )
            elif change_type == "delete":
                db_exec_pr_changes.insert_into_pr_changes_table(
                    unprocessed_pr_id, closed_date, original_file_path, None, None
                )
                # If change_type is delete, means that the file has been deleted.
                # Add it to the pr_changes table. This is the last time, the file will ever appear unless that PR has been reverted
            elif change_type == "edit":
                # If change_type is edit, means that the file has changes.
                # Add it to the pr_changes table.
                db_exec_pr_changes.insert_into_pr_changes_table(
                    unprocessed_pr_id, closed_date, file_path, None, None
                )
            elif "rename" in change_type:
                # If change_type contains rename then
                # 1. Enter a new entry with th enew fi
                # 2. Rename all previous references
                db_exec_pr_changes.insert_into_pr_changes_table(
                    unprocessed_pr_id, closed_date, file_path, None, None
                )
                db_exec_pr_changes.update_old_file_references_in_pr_changes_table(
                    original_file_path, file_path
                )
        count += 1
        if count % 50 == 0:
            print("DEBUG --- Collect change details for {} PRs".format(count))
    end_time = time.time()
    print(
        "[✅] Completed collecting changes for {} PRs in {} seconds".format(
            len(unprocessed_pr_ids), end_time - start_time
        )
    )
