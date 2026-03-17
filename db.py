import sqlite3


def __connect_to_db():
    conn = sqlite3.connect("data/insight.db")
    cursor = conn.cursor()
    return conn, cursor


def __close_db(conn):
    conn.commit()
    conn.close()


# pr_details Table Operations
def insert_into_pr_changes_table(id, closed_date, file_name, class_name, function_name):
    conn, cursor = __connect_to_db()
    try:
        cursor.execute(
            """
                INSERT INTO
                    pr_changes (id, closed_date, file_name, class_name, function_name)
                VALUES
                    (?, ?, ?, ?, ?);
            """,
            (
                id,
                closed_date,
                file_name,
                class_name,
                function_name,
            ),
        )
    finally:
        __close_db(conn)


def get_pr_detail(id):
    conn, cursor = __connect_to_db()
    try:
        cursor.execute(
            """
                SELECT
                    *
                FROM
                    pr_changes
                WHERE
                    id=?;
            """,
            (id,),
        )
        rows = cursor.fetchall()
        return rows
    finally:
        __close_db(conn)


def get_all_pr_ids():
    conn, cursor = __connect_to_db()
    try:
        cursor.execute(
            """
                SELECT DISTINCT
                    id
                FROM
                    pr_changes;
            """
        )
        rows = (
            cursor.fetchall()
        )  # This returns a list of tuples, e.g., [(1,), (2,), (3,)]
        ids = [row[0] for row in rows]  # Extract the first element from each tuple
        return ids
    finally:
        __close_db(conn)


def update_old_file_references_in_pr_changes_table(old_file_path, new_file_path):
    conn, cursor = __connect_to_db()
    try:
        cursor.execute(
            """
            UPDATE pr_changes
            SET
                file_name=?
            WHERE
                file_name=?;
        """,
            (new_file_path, old_file_path),
        )
    finally:
        __close_db(conn)


def get_frequency_of_file(file, month=1):
    conn, cursor = __connect_to_db()
    month_string = "-" + str(month) + " month"
    try:
        cursor.execute(
            """
            SELECT
                COUNT(*)
            FROM
                pr_changes
            WHERE
                file_name = ?
            AND
                DATE(closed_date) >= DATE('now', ?);
        """,
            (file, month_string),
        )
        rows = (
            cursor.fetchall()
        )  # This returns a list of tuples, e.g., [(1,), (2,), (3,)]
        for row in rows:  # Extract the first element from each tuple
            return int(row[0])
        return 0
    finally:
        __close_db(conn)
