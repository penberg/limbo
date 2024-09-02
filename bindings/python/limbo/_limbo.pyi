from typing import Any, List, Optional, Tuple

__version__: str

class Connection:
    def cursor(self) -> "Cursor":
        """
        Creates a new cursor object using this connection.

        :return: A new Cursor object.
        :raises InterfaceError: If the cursor cannot be created.
        """
        ...

    def close(self) -> None:
        """
        Closes the connection to the database.

        :raises OperationalError: If there is an error closing the connection.
        """
        ...

    def commit(self) -> None:
        """
        Commits the current transaction.

        :raises OperationalError: If there is an error during commit.
        """
        ...

    def rollback(self) -> None:
        """
        Rolls back the current transaction.

        :raises OperationalError: If there is an error during rollback.
        """
        ...

class Cursor:
    arraysize: int
    description: Optional[
        Tuple[
            str,
            str,
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
        ]
    ]
    rowcount: int

    def execute(self, sql: str, parameters: Optional[Tuple[Any, ...]] = None) -> "Cursor":
        """
        Prepares and executes a SQL statement using the connection.

        :param sql: The SQL query to execute.
        :param parameters: The parameters to substitute into the SQL query.
        :raises ProgrammingError: If there is an error in the SQL query.
        :raises OperationalError: If there is an error executing the query.
        :return: The cursor object.
        """
        ...

    def executemany(self, sql: str, parameters: Optional[List[Tuple[Any, ...]]] = None) -> None:
        """
        Executes a SQL command against all parameter sequences or mappings found in the sequence `parameters`.

        :param sql: The SQL command to execute.
        :param parameters: A list of parameter sequences or mappings.
        :raises ProgrammingError: If there is an error in the SQL query.
        :raises OperationalError: If there is an error executing the query.
        """
        ...

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """
        Fetches the next row from the result set.

        :return: A tuple representing the next row, or None if no more rows are available.
        :raises OperationalError: If there is an error fetching the row.
        """
        ...

    def fetchall(self) -> List[Tuple[Any, ...]]:
        """
        Fetches all remaining rows from the result set.

        :return: A list of tuples, each representing a row in the result set.
        :raises OperationalError: If there is an error fetching the rows.
        """
        ...

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        """
        Fetches the next set of rows of a size specified by the `arraysize` property.

        :param size: Optional integer to specify the number of rows to fetch.
        :return: A list of tuples, each representing a row in the result set.
        :raises OperationalError: If there is an error fetching the rows.
        """
        ...

    def close(self) -> None:
        """
        Closes the cursor.

        :raises OperationalError: If there is an error closing the cursor.
        """
        ...

# Exception classes
class Warning(Exception):
    """Exception raised for important warnings like data truncations while inserting."""

    ...

class Error(Exception):
    """Base class for all other error exceptions. Catch all database-related errors using this class."""

    ...

class InterfaceError(Error):
    """Exception raised for errors related to the database interface rather than the database itself."""

    ...

class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""

    ...

class DataError(DatabaseError):
    """
    Exception raised for errors due to problems with the processed data like division by zero, numeric value out of
    range, etc.
    """

    ...

class OperationalError(DatabaseError):
    """
    Exception raised for errors related to the database’s operation, not necessarily under the programmer's control.
    """

    ...

class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected, e.g., a foreign key check fails."""

    ...

class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error, e.g., cursor is not valid anymore, transaction out
    of sync.
    """

    ...

class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors, e.g., table not found, syntax error in SQL, wrong number of parameters
    specified.
    """

    ...

class NotSupportedError(DatabaseError):
    """Exception raised when a method or database API is used which is not supported by the database."""

    ...

def connect(path: str) -> Connection:
    """
    Connects to a database at the specified path.

    :param path: The path to the database file.
    :return: A Connection object to the database.
    :raises InterfaceError: If the database cannot be connected.
    """
    ...
