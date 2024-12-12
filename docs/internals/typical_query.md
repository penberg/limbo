The following sequence diagram shows the typical flow of a query from the CLI to the VDBE.

```mermaid
sequenceDiagram

participant main as cli/main
participant Database as core/lib/Database
participant Connection as core/lib/Connection
participant Statement as core/lib/Statement
participant Parser as sql/mod/Parser
participant Scanner as sql/mod/Scanner
participant translate as translate/mod
participant Program as vdbe/mod/Program

main->>Database: open_file
Database->>main: Connection
main->>Connection: query(sql)
Note right of Parser: Uses vendored sqlite3-parser
Connection->>Parser: next()
Note left of Parser: Passes the SQL query to Parser

Parser->>Scanner: scan()
Scanner->>Parser: returns tokens


Parser->>Connection: Cmd::Stmt
Connection->>translate:translate() 

Note left of translate: Translates SQL statement into bytecode

Connection->>main: Ok(Some(Rows { Statement }))
main->>Statement: step()
Statement->>Program: step()
Note left of Program: Executes bytecode instructions<br />See https://www.sqlite.org/opcode.html
Program->>Statement: StepResult
Statement->>main: StepResult
```
