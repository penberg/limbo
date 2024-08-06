use pyo3::create_exception;
use pyo3::exceptions::PyException;

create_exception!(
    limbo,
    Warning,
    PyException,
    "Exception raised for important warnings like data truncations while inserting."
);
create_exception!(limbo, Error, PyException, "Base class for all other error exceptions. Catch all database-related errors using this class.");

create_exception!(
    limbo,
    InterfaceError,
    Error,
    "Raised for errors related to the database interface rather than the database itself."
);
create_exception!(
    limbo,
    DatabaseError,
    Error,
    "Raised for errors that are related to the database."
);

create_exception!(limbo, DataError, DatabaseError, "Raised for errors due to problems with the processed data like division by zero, numeric value out of range, etc.");
create_exception!(limbo, OperationalError, DatabaseError, "Raised for errors related to the database’s operation, not necessarily under the programmer's control.");
create_exception!(limbo, IntegrityError, DatabaseError, "Raised when the relational integrity of the database is affected, e.g., a foreign key check fails.");
create_exception!(limbo, InternalError, DatabaseError, "Raised when the database encounters an internal error, e.g., cursor is not valid anymore, transaction out of sync.");
create_exception!(limbo, ProgrammingError, DatabaseError, "Raised for programming errors, e.g., table not found, syntax error in SQL, wrong number of parameters specified.");
create_exception!(
    limbo,
    NotSupportedError,
    DatabaseError,
    "Raised when a method or database API is used which is not supported by the database."
);
