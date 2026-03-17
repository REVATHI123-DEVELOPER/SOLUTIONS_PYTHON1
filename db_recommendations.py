import sqlite3


def __connect_to_db():
    conn = sqlite3.connect("data/insight.db")
    cursor = conn.cursor()
    return conn, cursor


def __close_db(conn):
    conn.commit()
    conn.close()


def get_pr_ids_matching_files(changed_files):
    conn, cursor = __connect_to_db()

    # Generate placeholders dynamically for each file
    placeholders = ", ".join(
        ["?"] * len(changed_files)
    )  # Create placeholders like ?, ?, ?
    try:
        cursor.execute(
            f"""
                SELECT
                    id
                FROM
                    pr_changes
                WHERE
                    file_name IN ({placeholders});
            """,
            changed_files,
        )

        rows = cursor.fetchall()
        ids = [row[0] for row in rows]
        return ids
    finally:
        __close_db(conn)


def get_workitems_ids_matching_pr_id(id):
    conn, cursor = __connect_to_db()
    try:
        cursor.execute(
            """
                SELECT
                    workitem_id
                FROM
                    pr_workitem_details
                WHERE
                    pr_id = ?;
            """,
            (id,),
        )

        rows = cursor.fetchall()
        ids = [row[0] for row in rows]
        return ids
    finally:
        __close_db(conn)


def get_workitem_details_matching_workitem_id(id):
    conn, cursor = __connect_to_db()
    try:
        cursor.execute(
            """
            SELECT
                *
            FROM
                workitem_details
            WHERE
                id = ?;
        """,
            (id,),
        )

        rows = cursor.fetchall()
        # There will be only one row. So dont worry
        for row in rows:
            return row
    finally:
        __close_db(conn)
