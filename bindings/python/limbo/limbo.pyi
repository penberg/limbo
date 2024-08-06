from typing import Any, Dict, List

class Database:
    def __init__(self, path: str) -> None:
        """
        Initializes a new Database instance.

        :param path: The path to the database file.
        :raises Exception: If the database could not be opened.
        """
        ...

    def exec(self, sql: str) -> List[Dict[Any]]:
        """
        Executes a SQL query on the database and returns the results.

        :param sql: The SQL query to execute.
        :return: A list of rows, where each row is a list of column values.
        :raises Exception: If there is an error executing the query.
        """
        ...