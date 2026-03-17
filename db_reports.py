import sqlite3


def __connect_to_db():
    conn = sqlite3.connect("data/insight.db")
    cursor = conn.cursor()
    return conn, cursor


def __close_db(conn):
    conn.commit()
    conn.close()


def get_recommended_workitems_per_release(release):
    conn, cursor = __connect_to_db()
    try:
        cursor.execute(
            """
                SELECT
                    rd.pr_id AS PR,
                    pwd.repo_id AS 'Repo ID',
                    rd.a_workitem_id AS 'Associated WorkItem',
                    wd.feature AS 'Associated WorkItem Feature',
                    wd.target_release AS 'Associated WorkItem Target Release',
                    rd.r_workitem_id AS 'Recommended Workitem',
                    rwd.priority AS 'Recommended WorkItem Priority',
                    rwd.severity AS 'Recommended WorkItem Severity',
                    rwd.feature AS 'Recommended WorkItem Feature',
                    rd.r_posted_date AS 'Recommended Date',
                    rd.r_confidence AS 'Confidence Score'
                FROM
                    recommendation_details rd
                LEFT JOIN
                    workitem_details wd
                ON
                    rd.a_workitem_id = wd.id
                LEFT JOIN
                    workitem_details rwd
                ON
                    rd.r_workitem_id = rwd.id
                LEFT JOIN
                    pr_workitem_details pwd
                ON
                    rd.pr_id = pwd.pr_id
                WHERE
                    wd.target_release = ?;
            """,
            (release,),
        )

        rows = cursor.fetchall()
        return rows
    finally:
        __close_db(conn)
