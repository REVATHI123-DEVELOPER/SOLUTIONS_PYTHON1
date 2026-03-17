import copy
import random
from datetime import datetime
from pathlib import Path

import pytz

from ado_apis import apis
from data import ado_data, confidence_metrics, whitelist
from db import db_exec_recommendation_details, db_recommendations
from services import confidence


def run(repo_id, pull_request_id):
    # Get current time
    current_time = datetime.now()

    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    print("===================================================")
    pid = random.randint(10000, 32768)

    print("PID {} - Recommendation Engine Invoked at : {}".format(pid, formatted_time))

    repo_name = None
    repo_details = ado_data.repo_details
    for repo_detail in repo_details:
        if repo_id == repo_detail["repo_id"]:
            repo_name = repo_detail["repo_name"]

    if repo_name is None:
        print(
            "PID {} - RepoId {} is not one among the known Repos. Please contact Admin for support.".format(
                pid, repo_id
            )
        )
        print("PID {} - Recommendation Engine exiting.".format(pid))
        return None

    print(
        "PID {} - Analyzing Pull Request {} from Repo {}".format(
            pid, pull_request_id, repo_name
        )
    )

    pat = ado_data.pat
    organization = ado_data.organization
    project = ado_data.project

    # Step 1: For a given PR Id, get all associated WorkItems
    associated_workitem_details: list = apis.get_associated_workitems_for_pull_request(
        organization, project, repo_id, pull_request_id, pat
    )
    associated_workitem_ids: list = []
    for associated_workitem_detail in associated_workitem_details:
        workitem_id = associated_workitem_detail["id"]
        associated_workitem_ids.append(workitem_id)

    associated_workitem_details = apis.get_workitem_details(
        associated_workitem_ids, organization, project, pat
    )

    # Step 1.1: Filter only associated workitems of type 'Bug'.
    # Post comments only for Bugs
    # Step 1.2: Filter only associated workitems that are in the whitelisted category
    # Post comments only for associated workitems that are whitelisted

    filtered_associated_workitem_details: list = []
    for associated_workitem_detail in associated_workitem_details:
        workitem_type = associated_workitem_detail["fields"]["System.WorkItemType"]
        workitem_feature = associated_workitem_detail["fields"]["Custom.Feature"]
        if (
            workitem_type == "Bug"
            and workitem_feature in whitelist.whitelisted_features
        ):
            filtered_associated_workitem_details.append(associated_workitem_detail)

    # Reassign to associated_workitem_details and recalculate the associated_workitem_ids
    associated_workitem_details = filtered_associated_workitem_details
    associated_workitem_ids: list = []
    for associated_workitem_detail in associated_workitem_details:
        workitem_id = associated_workitem_detail["id"]
        associated_workitem_ids.append(workitem_id)

    if len(associated_workitem_ids) == 0:
        print(
            "PID {} - Recommendation Engine exiting since no valid associated workitem found.".format(
                pid
            )
        )
        exit(0)

    # Step 2: Find the files that are part of the PR
    # Step 2.1 Get all the iterations
    iteration_details: list = apis.get_pr_iterations(
        pull_request_id, organization, project, repo_id, pat
    )
    last_iteration_detail = iteration_details[-1]
    last_iteration_id = last_iteration_detail["id"]

    # Step 2.2 Get all the changes for the iterations
    changes = apis.get_files_for_iteration(
        pull_request_id, last_iteration_id, organization, project, repo_id, pat
    )

    changed_files = []
    for change in changes:
        change_type = change["changeType"]
        file_path = change["item"]["path"]

        # if it is renamed or deleted, get the original file path
        if "rename" in change_type:
            changed_files.append(
                change["originalPath"],
            )
        elif change_type == "delete":
            changed_files.append(
                change["originalPath"],
            )
        else:
            changed_files.append(file_path)

    recommended_workitems = __get_workitem_recommendation(
        associated_workitem_details, changed_files, repo_id
    )

    for associated_workitem_id, v in recommended_workitems.items():
        message_header = """
        <h3 style='color:red'>*** This is an auto-generated message posted as part of the Beta Trials for ImpactInsights. ***</h3>
        """

        message_confidence = """
        <br><h3>Confidence of Recommendations: <b>{}</b> </h3>
        """.format(
            str(v["confidence"]) + "%"
        )

        recommended_recidivistic_defect_workitems = v.get("recidivistic_defects", [])
        recommended_defect_workitems = v.get("defects", [])
        recommended_extended_defect_workitems = v.get("extended_defects", [])

        message_body = ""

        if recommended_recidivistic_defect_workitems:
            message_body += """<br>
                            <p>Verify the below <b>recidivistic & customer reported</b> defects after <b>PR : <a target='_blank'
                                href='https://dev.azure.com/Buckman-Digital-Innovation-Hub/OnSitePro/_git/OnSitePro/pullrequest/{}'>{}</a></b>
                                has been merged</p><br>
            """.format(
                pull_request_id, pull_request_id
            )

            # Start table
            message_body += """
            <table border='1' style='border-collapse: collapse; table-layout: auto;'>
                <tr>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>#</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>ID</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>P</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>S</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>Title</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>Feature</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>Status</th>
                </tr>
            """

            # Add rows for each recommended defect work item
            index = 1  # Initialize index separately

            for (
                recommended_recidivistic_defect
            ) in recommended_recidivistic_defect_workitems:
                recommended_workitem_id = recommended_recidivistic_defect["workitem_id"]
                recommended_workitem_priority = recommended_recidivistic_defect[
                    "workitem_priority"
                ]
                recommended_workitem_severity = recommended_recidivistic_defect[
                    "workitem_severity"
                ].split("-")[0]
                recommended_workitem_title = recommended_recidivistic_defect[
                    "workitem_title"
                ]
                recommended_workitem_feature = recommended_recidivistic_defect[
                    "workitem_feature"
                ]
                recommended_workitem_status = recommended_recidivistic_defect[
                    "workitem_status"
                ]

                message_body += """
                <tr>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'><a target='_blank' href='https://dev.azure.com/Buckman-Digital-Innovation-Hub/OnSitePro/_workitems/edit/{}'>{}</a></td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                </tr>
                """.format(
                    index,
                    recommended_workitem_id,
                    recommended_workitem_id,
                    recommended_workitem_priority,
                    recommended_workitem_severity,
                    recommended_workitem_title,
                    recommended_workitem_feature,
                    recommended_workitem_status,
                )
                index += 1  # Increment index only when adding a row

            # Close table
            message_body += "</table>"

        if recommended_defect_workitems:
            message_body += """<br>
                            <p>Verify the below <b>non-recidivistic</b> defects (created within last 1 year) after <b>PR : <a target='_blank'
                                href='https://dev.azure.com/Buckman-Digital-Innovation-Hub/OnSitePro/_git/OnSitePro/pullrequest/{}'>{}</a></b>
                                has been merged</p><br>
            """.format(
                pull_request_id, pull_request_id
            )

            # Start table
            message_body += """
            <table border='1' style='border-collapse: collapse; table-layout: auto;'>
                <tr>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>#</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>ID</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>P</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>S</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>Title</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>Feature</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>Status</th>
                </tr>
            """

            # Add rows for each recommended defect work item
            index = 1  # Initialize index separately

            for recommended_defect in recommended_defect_workitems:
                recommended_workitem_id = recommended_defect["workitem_id"]
                recommended_workitem_priority = recommended_defect["workitem_priority"]
                recommended_workitem_severity = recommended_defect[
                    "workitem_severity"
                ].split("-")[0]
                recommended_workitem_title = recommended_defect["workitem_title"]
                recommended_workitem_feature = recommended_defect["workitem_feature"]
                recommended_workitem_status = recommended_defect["workitem_status"]

                message_body += """
                <tr>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'><a target='_blank' href='https://dev.azure.com/Buckman-Digital-Innovation-Hub/OnSitePro/_workitems/edit/{}'>{}</a></td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                </tr>
                """.format(
                    index,
                    recommended_workitem_id,
                    recommended_workitem_id,
                    recommended_workitem_priority,
                    recommended_workitem_severity,
                    recommended_workitem_title,
                    recommended_workitem_feature,
                    recommended_workitem_status,
                )
                index += 1  # Increment index only when adding a row

            # Close table
            message_body += "</table>"

        if recommended_extended_defect_workitems:
            message_body += """<br><br>
                            <p>Here is the extended list of defects that <b>might also be impacted</b> that is worth a quick review.</p>
                        <br>
            """

            # Start table
            message_body += """
            <table border='1' style='border-collapse: collapse; table-layout: auto;'>
                <tr>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>#</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>ID</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>P</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>S</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>Title</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>Feature</th>
                    <td style='text-align: left; background-color: #f2f2f2; font-weight:900;'>Status</th>
                </tr>
            """

            # Add rows for each recommended defect work item
            index = 1  # Initialize index separately

            for recommended_extended_defect in recommended_extended_defect_workitems:
                recommended_workitem_id = recommended_extended_defect["workitem_id"]
                recommended_workitem_priority = recommended_extended_defect[
                    "workitem_priority"
                ]
                recommended_workitem_severity = recommended_extended_defect[
                    "workitem_severity"
                ].split("-")[0]
                recommended_workitem_title = recommended_extended_defect[
                    "workitem_title"
                ]
                recommended_workitem_feature = recommended_extended_defect[
                    "workitem_feature"
                ]
                recommended_workitem_status = recommended_extended_defect[
                    "workitem_status"
                ]

                message_body += """
                <tr>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'><a target='_blank' href='https://dev.azure.com/Buckman-Digital-Innovation-Hub/OnSitePro/_workitems/edit/{}'>{}</a></td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                    <td style='padding: 8px;'>{}</td>
                </tr>
                """.format(
                    index,
                    recommended_workitem_id,
                    recommended_workitem_id,
                    recommended_workitem_priority,
                    recommended_workitem_severity,
                    recommended_workitem_title,
                    recommended_workitem_feature,
                    recommended_workitem_status,
                )
                index += 1  # Increment index only when adding a row

            # Close table
            message_body += "</table>"

        if (
            recommended_recidivistic_defect_workitems
            or recommended_defect_workitems
            or recommended_extended_defect_workitems
        ):
            message = ""
            if (
                recommended_recidivistic_defect_workitems
                or recommended_defect_workitems
            ):
                message = message_header + message_confidence + message_body
            elif recommended_extended_defect_workitems:
                message = message_header + message_body

            apis.add_comment_to_workitem(
                workitem_id, message, organization, project, pat
            )

            # apis.add_comment_to_workitem(60528, message, organization, project, pat)

            print(
                "PID {} - Recommendations successfully posted to workitem {}".format(
                    pid, associated_workitem_id
                )
            )

            current_time = datetime.now()
            recommendation_posted_formatted_time = current_time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            # Insert the posted data into the table (recommendation_details) for future reference
            for recommended_workitem in recommended_recidivistic_defect_workitems:
                db_exec_recommendation_details.insert_into_recommendation_details(
                    pull_request_id,
                    associated_workitem_id,
                    recommended_workitem["workitem_id"],
                    recommendation_posted_formatted_time,
                    recommended_workitem["confidence"],
                )
            for recommended_workitem in recommended_defect_workitems:
                db_exec_recommendation_details.insert_into_recommendation_details(
                    pull_request_id,
                    associated_workitem_id,
                    recommended_workitem["workitem_id"],
                    recommendation_posted_formatted_time,
                    recommended_workitem["confidence"],
                )
        else:
            print(
                "PID {} - No Recommendations found to be posted to workitem {}".format(
                    pid, associated_workitem_id
                )
            )


def __get_workitem_recommendation(associated_workitem_details, changed_files, repo_id):
    filtered_changed_files = []
    for changed_file in changed_files:
        if Path(changed_file).suffix in whitelist.whiteliested_file_types:
            filtered_changed_files.append(changed_file)

    # Step 2: Query to get pr_ids from pr_changes where file_name matches
    pr_ids = db_recommendations.get_pr_ids_matching_files(filtered_changed_files)

    # Step 3: Same PR can be present more than once. Remove duplicates
    pr_ids = list(set(pr_ids))

    # Step 4: Get recommended workitem_ids from pr_workitem_details for each pr_id
    recommended_workitem_id_list = []
    for pr_id in pr_ids:
        workitem_ids = db_recommendations.get_workitems_ids_matching_pr_id(pr_id)
        recommended_workitem_id_list.extend(workitem_ids)

    # Step 5: Remove duplicates from the recommended workitems
    recommended_workitem_id_list = list(set(recommended_workitem_id_list))

    # Step 6: Get details for recommended workitems
    recommended_defect_details = []
    recommended_recidivistic_defect_details = []

    for recommended_workitem_id in recommended_workitem_id_list:
        recommended_workitem_detail = (
            db_recommendations.get_workitem_details_matching_workitem_id(
                recommended_workitem_id
            )
        )
        recommended_workitem_title = recommended_workitem_detail[1]
        recommended_workitem_type = recommended_workitem_detail[2]
        recommended_workitem_severity = recommended_workitem_detail[3]
        recommended_workitem_priority = recommended_workitem_detail[4]
        recommended_workitem_feature = recommended_workitem_detail[5]
        # recommended_workitem_target_release = recommended_workitem_detail[6]
        recommended_workitem_status = recommended_workitem_detail[7]
        recommended_workitem_exists_in_sit = recommended_workitem_detail[8]
        recommended_workitem_exists_in_prod = recommended_workitem_detail[9]
        recommended_workitem_is_recidivistic = recommended_workitem_detail[10]
        recommended_workitem_reporter = recommended_workitem_detail[11]
        recommended_workitem_reporter_role = recommended_workitem_detail[12]
        recommended_workitem_created_date = recommended_workitem_detail[13]

        # Recommend only workitems that are in Done, Ready for Prod
        if recommended_workitem_status in whitelist.whiteliested_state:
            # Recommend only workitems that are created not more than 1 year ago
            # Convert the recommended_wortkitem_created_date string to a datetime object
            def parse_datetime(time_str):
                # List of possible formats to try
                formats = [
                    "%Y-%m-%dT%H:%M:%S.%fZ",  # Format with fractional seconds
                    "%Y-%m-%dT%H:%M:%SZ",  # Format without fractional seconds
                ]

                for fmt in formats:
                    try:
                        return datetime.strptime(time_str, fmt)
                    except ValueError:
                        continue  # Try the next format if this one fails

                # If none of the formats work, raise an error
                raise ValueError(
                    f"Time data '{time_str}' does not match any of the expected formats."
                )

            given_date = parse_datetime(recommended_workitem_created_date)
            given_date = given_date.replace(tzinfo=pytz.utc)

            # Get the current date and time in UTC
            current_date = datetime.now(pytz.utc)

            # Calculate the difference
            difference = current_date - given_date

            # Check if the difference is less than 1 year (365 days)
            is_less_than_1year = difference.days < 365

            # If a bug is recidivistic or reported by extenal, then recommend it regardless of when it was created. Else it should be createrd less than a year
            if recommended_workitem_type == "Bug":
                if recommended_workitem_is_recidivistic or (
                    recommended_workitem_reporter or recommended_workitem_reporter_role
                ):
                    recommended_recidivistic_defect_details.append(
                        {
                            "workitem_id": recommended_workitem_id,
                            "workitem_priority": recommended_workitem_priority,
                            "workitem_severity": recommended_workitem_severity,
                            "workitem_title": recommended_workitem_title,
                            "workitem_type": recommended_workitem_type,
                            "workitem_feature": recommended_workitem_feature,
                            "workitem_status": recommended_workitem_status,
                            "workitem_created": recommended_workitem_created_date,
                        }
                    )
                else:
                    if (
                        is_less_than_1year
                        and recommended_workitem_feature
                        in whitelist.whitelisted_features
                        and (
                            recommended_workitem_exists_in_sit
                            or recommended_workitem_exists_in_prod
                        )
                    ):
                        recommended_defect_details.append(
                            {
                                "workitem_id": recommended_workitem_id,
                                "workitem_priority": recommended_workitem_priority,
                                "workitem_severity": recommended_workitem_severity,
                                "workitem_title": recommended_workitem_title,
                                "workitem_type": recommended_workitem_type,
                                "workitem_feature": recommended_workitem_feature,
                                "workitem_status": recommended_workitem_status,
                                "workitem_created": recommended_workitem_created_date,
                            }
                        )

    # Sort by priority first, then severity
    recommended_defect_details = sorted(
        recommended_defect_details,
        key=lambda x: (x["workitem_priority"], x["workitem_severity"]),
    )
    # Sort by priority first, then severity
    recommended_recidivistic_defect_details = sorted(
        recommended_recidivistic_defect_details,
        key=lambda x: (x["workitem_priority"], x["workitem_severity"]),
    )

    # Get the confidence score for each recommendation with respect to the associated workitems
    workitem_recommendations = {}
    for associated_workitem in associated_workitem_details:
        dc_recommended_recidivistic_defect_details = copy.deepcopy(
            recommended_recidivistic_defect_details
        )
        dc_recommended_defect_details = copy.deepcopy(recommended_defect_details)
        for recommended_workitem in dc_recommended_recidivistic_defect_details:
            confidence.calculate_confidence(
                associated_workitem, changed_files, recommended_workitem, repo_id
            )
        for recommended_workitem in dc_recommended_defect_details:
            confidence.calculate_confidence(
                associated_workitem, changed_files, recommended_workitem, repo_id
            )

        workitem_recommendations[associated_workitem["id"]] = {
            "recidivistic_defects": dc_recommended_recidivistic_defect_details,
            "defects": dc_recommended_defect_details,
        }

    # Remove if associated workitem is also present in the recommendations. It only happens while we are testing and doesnt happen in production
    for k, v in workitem_recommendations.items():
        defects = v.get("defects", [])
        i = 0
        while i < len(defects):
            d = defects[i]
            if str(d["workitem_id"]) == str(k):
                defects.pop(i)  # don't increment i, since list shrinks
            else:
                i += 1

        recidivistic_defects = v.get("recidivistic_defects", [])
        i = 0
        while i < len(recidivistic_defects):
            d = recidivistic_defects[i]
            if str(d["workitem_id"]) == str(k):
                recidivistic_defects.pop(i)  # don't increment i, since list shrinks
            else:
                i += 1

    # Remove recommendations that that have the confidence level below threshold
    for k, v in workitem_recommendations.items():
        if "extended_defects" in v:
            extended_defects = v["extended_defects"]
        else:
            extended_defects = []
            v["extended_defects"] = extended_defects

        defects = v.get("defects", [])
        i = 0
        while i < len(defects):
            d = defects[i]
            if d["confidence"] < confidence_metrics.confidence_threshold:
                if d["confidence"] >= confidence_metrics.extended_confidence_threshold:
                    extended_defects.append(d)
                defects.pop(i)  # don't increment i, since list shrinks
            else:
                i += 1

        recidivistic_defects = v.get("recidivistic_defects", [])
        i = 0
        while i < len(recidivistic_defects):
            d = recidivistic_defects[i]
            if d["confidence"] < confidence_metrics.confidence_threshold:
                if d["confidence"] >= confidence_metrics.extended_confidence_threshold:
                    extended_defects.append(d)
                recidivistic_defects.pop(i)  # don't increment i, since list shrinks
            else:
                i += 1

    # Compute overall confidence
    for k, v in workitem_recommendations.items():
        defects = v.get("defects", [])
        recidivistic_defects = v.get("recidivistic_defects", [])
        confidence_summation = 0
        total_defects_recommended = len(defects) + len(recidivistic_defects)

        for d in defects + recidivistic_defects:
            confidence_summation += d["confidence"]

        if total_defects_recommended == 0:
            overall_confidence = 0
        else:
            overall_confidence = round(
                (confidence_summation / total_defects_recommended), 2
            )
        v["confidence"] = overall_confidence

    return workitem_recommendations


if __name__ == "__main__":
    repo_id = ado_data.be_repo_id
    pull_request_id = "59407"
    run(repo_id, pull_request_id)
