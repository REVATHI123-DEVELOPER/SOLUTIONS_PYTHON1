from pathlib import Path

from data import ado_data, whitelist
from data.confidence_metrics import (feature_coupling,
                                     feature_coupling_weightage,
                                     file_frequency_weightage,
                                     file_intersection_weightage)
from db.db_exec_pr_changes import get_frequency_of_file, get_pr_detail
from db.db_exec_pr_workitem_details import get_pr_id_for_workitem

pat = ado_data.pat
organization = ado_data.organization
project = ado_data.project


def __feature_coupling(associated_workitem, recommended_workitem):
    associated_workitem_feature = associated_workitem["fields"]["Custom.Feature"]
    recommended_workitem_feature = recommended_workitem["workitem_feature"]

    if associated_workitem_feature == recommended_workitem_feature:
        return feature_coupling_weightage["DITTO"]

    if associated_workitem_feature not in feature_coupling.keys():
        return feature_coupling_weightage["default"]

    weightage_details_for_feature = feature_coupling[associated_workitem_feature]
    tc = weightage_details_for_feature.get("TC", [])
    mc = weightage_details_for_feature.get("MC", [])
    lc = weightage_details_for_feature.get("LC", [])

    if recommended_workitem_feature in tc:
        return feature_coupling_weightage["TC"]
    elif recommended_workitem_feature in mc:
        return feature_coupling_weightage["MC"]
    elif recommended_workitem_feature in lc:
        return feature_coupling_weightage["LC"]
    else:
        return feature_coupling_weightage["default"]


def __file_overlap(changed_files, recommended_workitem, repo_id):
    pr_ids = get_pr_id_for_workitem(recommended_workitem["workitem_id"], repo_id)
    all_files_part_of_recommendation = []
    for pr_id in pr_ids:
        pr_details: list = get_pr_detail(pr_id)
        for pr_detail in pr_details:
            if pr_detail[2] not in all_files_part_of_recommendation:
                all_files_part_of_recommendation.append(pr_detail[2])

    filtered_all_files_part_of_recommendation = []
    for file in all_files_part_of_recommendation:
        if Path(file).suffix in whitelist.whiteliested_file_types:
            filtered_all_files_part_of_recommendation.append(file)

    intersection = (
        len(set(filtered_all_files_part_of_recommendation) & set(changed_files))
        / len(set(filtered_all_files_part_of_recommendation))
        * 100
    )
    if intersection >= 70:
        confidence = file_intersection_weightage["70"]
    elif intersection >= 60:
        confidence = file_intersection_weightage["60"]
    elif intersection >= 50:
        confidence = file_intersection_weightage["50"]
    else:
        confidence = file_intersection_weightage["default"]
    return confidence


def __file_frequency(changed_files):
    frequencies = []
    for file in changed_files:
        frequency = get_frequency_of_file(file, month=1)
        frequencies.append(frequency)

    avg_frequency = sum(frequencies) / len(frequencies)

    if avg_frequency >= 7:
        confidence = file_frequency_weightage["7"]
    elif avg_frequency >= 5:
        confidence = file_frequency_weightage["5"]
    elif avg_frequency >= 4:
        confidence = file_frequency_weightage["4"]
    else:
        confidence = file_frequency_weightage["default"]
    return confidence


def calculate_confidence(
    associated_workitem, changed_files, recommended_workitem, repo_id
):
    feature_coupling_weightage = __feature_coupling(
        associated_workitem, recommended_workitem
    )
    file_overlap_weightage = __file_overlap(
        changed_files, recommended_workitem, repo_id
    )

    file_frequency_weightage = __file_frequency(changed_files)

    confidence = round(
        (feature_coupling_weightage + file_overlap_weightage + file_frequency_weightage)
        * 100,
        3,
    )
    recommended_workitem["confidence"] = confidence
