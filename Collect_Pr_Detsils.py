#!/usr/bin/env python

from collector import collect_pr_details


def run():
    # This function will act upon the New PRs and update the DB with change information
    collect_pr_details.run()

if __name__ == "__main__":
    run()
