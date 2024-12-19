//! The virtual database engine (VDBE).
//!
//! The VDBE is a register-based virtual machine that execute bytecode
//! instructions that represent SQL statements. When an application prepares
//! an SQL statement, the statement is compiled into a sequence of bytecode
//! instructions that perform the needed operations, such as reading or
//! writing to a b-tree, sorting, or aggregating data.
//!
//! The instruction set of the VDBE is similar to SQLite's instruction set,
//! but with the exception that bytecodes that perform I/O operations are
//! return execution back to the caller instead of blocking. This is because
//! Limbo is designed for applications that need high concurrency such as
//! serverless runtimes. In addition, asynchronous I/O makes storage
//! disaggregation easier.
//!
//! You can find a full list of SQLite opcodes at:
//!
//! https://www.sqlite.org/opcode.html

pub mod builder;
pub mod explain;
pub mod sorter;

mod datetime;

use crate::error::{LimboError, SQLITE_CONSTRAINT_PRIMARYKEY};
use crate::function::{AggFunc, FuncCtx, MathFunc, MathFuncArity, ScalarFunc};
use crate::pseudo::PseudoCursor;
use crate::schema::Table;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::{btree::BTreeCursor, pager::Pager};
use crate::types::{
    AggContext, Cursor, CursorResult, OwnedRecord, OwnedValue, Record, SeekKey, SeekOp,
};
use crate::util::parse_schema_rows;
use crate::{enum_with_docs, Connection, Result, TransactionState};
#[cfg(feature = "json")]
use crate::{function::JsonFunc, json::get_json};
use crate::{Rows, DATABASE_VERSION};

use datetime::{exec_date, exec_time, exec_unixepoch};

use rand::distributions::{Distribution, Uniform};
use rand::{thread_rng, Rng};
use regex::Regex;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::rc::{Rc, Weak};

pub type BranchOffset = i64;

pub type CursorID = usize;

pub type PageIdx = usize;

mod enum_with_docs;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Func {
    Scalar(ScalarFunc),
    #[cfg(feature = "json")]
    Json(JsonFunc),
}

impl Display for Func {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Func::Scalar(scalar_func) => scalar_func.to_string(),
            #[cfg(feature = "json")]
            Func::Json(json_func) => json_func.to_string(),
        };
        write!(f, "{}", str)
    }
}

enum_with_docs! {
    Insn {
        // Initialize the program state and jump to the given PC.
        #[description = "Programs contain a single instance of this opcode as the very first opcode. If tracing is enabled (by the sqlite3_trace()) interface, then the UTF-8 string contained in P4 is emitted on the trace callback. Or if P4 is blank, use the string returned by sqlite3_sql(). If P2 is not zero, jump to instruction P2. Increment the value of P1 so that Once opcodes will jump the first time they are evaluated for this run. If P3 is not zero, then it is an address to jump to if an SQLITE_CORRUPT error is encountered."]
        Init {
            target_pc: BranchOffset,
        },
        // Write a NULL into register dest. If dest_end is Some, then also write NULL into register dest_end and every register in between dest and dest_end. If dest_end is not set, then only register dest is set to NULL.
        #[description = "Write a NULL into registers P2. If P3 greater than P2, then also write NULL into register P3 and every register in between P2 and P3. If P3 is less than P2 (typically P3 is zero) then only register P2 is set to NULL. If the P1 value is non-zero, then also set the MEM_Cleared flag so that NULL values will not compare equal even if SQLITE_NULLEQ is set on Ne or Eq."]
        Null {
            dest: usize,
            dest_end: Option<usize>,
        },
        // Move the cursor P1 to a null row. Any Column operations that occur while the cursor is on the null row will always write a NULL.
        #[description = "Move the cursor P1 to a null row. Any Column operations that occur while the cursor is on the null row will always write a NULL. If cursor P1 is not previously opened, open it now to a special pseudo-cursor that always returns NULL for every column."]
        NullRow {
            cursor_id: CursorID,
        },
        // Add two registers and store the result in a third register.
        #[description = "Add the value in register P1 to the value in register P2 and store the result in register P3. If either input is NULL, the result is NULL."]
        Add {
            lhs: usize,
            rhs: usize,
            dest: usize,
        },
        // Subtract rhs from lhs and store in dest
        #[description = "Subtract the value in register P1 from the value in register P2 and store the result in register P3. If either input is NULL, the result is NULL."]
        Subtract {
            lhs: usize,
            rhs: usize,
            dest: usize,
        },
        // Multiply two registers and store the result in a third register.
        #[description = "Multiply the value in register P1 by the value in register P2 and store the result in register P3. If either input is NULL, the result is NULL."]
        Multiply {
            lhs: usize,
            rhs: usize,
            dest: usize,
        },
        // Divide lhs by rhs and store the result in a third register.
        #[description =  "Divide the value in register P1 by the value in register P2 and store the result in register P3 (P3=P2/P1). If the value in register P1 is zero, then the result is NULL. If either input is NULL, the result is NULL."]
        Divide {
            lhs: usize,
            rhs: usize,
            dest: usize,
        },
        // Compare two vectors of registers in reg(P1)..reg(P1+P3-1) (call this vector "A") and in reg(P2)..reg(P2+P3-1) ("B"). Save the result of the comparison for use by the next Jump instruct.
        #[description = "Compare two vectors of registers in reg(P1)..reg(P1+P3-1) (call this vector \"A\") and in reg(P2)..reg(P2+P3-1) (\"B\"). Save the result of the comparison for use by the next Jump instruct. If P5 has the OPFLAG_PERMUTE bit set, then the order of comparison is determined by the most recent Permutation operator. If the OPFLAG_PERMUTE bit is clear, then register are compared in sequential order. P4 is a KeyInfo structure that defines collating sequences and sort orders for the comparison. The permutation applies to registers only. The KeyInfo elements are used sequentially. The comparison is a sort comparison, so NULLs compare equal, NULLs are less than numbers, numbers are less than strings, and strings are less than blobs. This opcode must be immediately followed by an Jump opcode."]
        Compare {
            start_reg_a: usize,
            start_reg_b: usize,
            count: usize,
        },
        // Place the result of rhs bitwise AND lhs in third register.
        #[description = "Take the bit-wise AND of the values in register P1 and P2 and store the result in register P3. If either input is NULL, the result is NULL."]
        BitAnd {
            lhs: usize,
            rhs: usize,
            dest: usize,
        },
        // Place the result of rhs bitwise OR lhs in third register.
        #[description = "Take the bit-wise OR of the values in register P1 and P2 and store the result in register P3. If either input is NULL, the result is NULL."]
        BitOr {
            lhs: usize,
            rhs: usize,
            dest: usize,
        },
        // Place the result of bitwise NOT register P1 in dest register.
        #[description = "Interpret the content of register P1 as an integer. Store the ones-complement of the P1 value into register P2. If P1 holds a NULL then store a NULL in P2."]
        BitNot {
            reg: usize,
            dest: usize,
        },
        // Jump to the instruction at address P1, P2, or P3 depending on whether in the most recent Compare instruction the P1 vector was less than, equal to, or greater than the P2 vector, respectively.
        #[description = "Jump to the instruction at address P1, P2, or P3 depending on whether in the most recent Compare instruction the P1 vector was less than, equal to, or greater than the P2 vector, respectively. This opcode must immediately follow an Compare opcode."]
        Jump {
            target_pc_lt: BranchOffset,
            target_pc_eq: BranchOffset,
            target_pc_gt: BranchOffset,
        },
        // Move the P3 values in register P1..P1+P3-1 over into registers P2..P2+P3-1. Registers P1..P1+P3-1 are left holding a NULL. It is an error for register ranges P1..P1+P3-1 and P2..P2+P3-1 to overlap. It is an error for P3 to be less than 1.
        #[description = "Move the P3 values in register P1..P1+P3-1 over into registers P2..P2+P3-1. Registers P1..P1+P3-1 are left holding a NULL. It is an error for register ranges P1..P1+P3-1 and P2..P2+P3-1 to overlap. It is an error for P3 to be less than 1."]
        Move {
            source_reg: usize,
            dest_reg: usize,
            count: usize,
        },
        // If the given register is a positive integer, decrement it by decrement_by and jump to the given PC.
        #[description = "Register P1 must contain an integer. If the value of register P1 is 1 or greater, subtract P3 from the value in P1 and jump to P2. If the initial value of register P1 is less than 1, then the value is unchanged and control passes through to the next instruction."]
        IfPos {
            reg: usize,
            target_pc: BranchOffset,
            decrement_by: usize,
        },
        // If the given register is not NULL, jump to the given PC.
        #[description = "Jump to P2 if the value in register P1 is not NULL."]
        NotNull {
            reg: usize,
            target_pc: BranchOffset,
        },
        // Compare two registers and jump to the given PC if they are equal.
        #[description = "Compare the values in register P1 and P3. If reg(P3)==reg(P1) then jump to address P2. The SQLITE_AFF_MASK portion of P5 must be an affinity character - SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth. An attempt is made to coerce both inputs according to this affinity before the comparison is made. If the SQLITE_AFF_MASK is 0x00, then numeric affinity is used. Note that the affinity conversions are stored back into the input registers P1 and P3. So this opcode can cause persistent changes to registers P1 and P3. Once any conversions have taken place, and neither value is NULL, the values are compared. If both values are blobs then memcmp() is used to determine the results of the comparison. If both values are text, then the appropriate collating function specified in P4 is used to do the comparison. If P4 is not specified then memcmp() is used to compare text string. If both values are numeric, then a numeric comparison is used. If the two values are of different types, then numbers are considered less than strings and strings are considered less than blobs. If SQLITE_NULLEQ is set in P5 then the result of comparison is always either true or false and is never NULL. If both operands are NULL then the result of comparison is true. If either operand is NULL then the result is false. If neither operand is NULL the result is the same as it would be if the SQLITE_NULLEQ flag were omitted from P5. This opcode saves the result of comparison for use by the new Jump opcode."]
        Eq {
            lhs: usize,
            rhs: usize,
            target_pc: BranchOffset,
        },
        // Compare two registers and jump to the given PC if they are not equal.
        #[description = "This works just like the Eq opcode except that the jump is taken if the operands in registers P1 and P3 are not equal. See the Eq opcode for additional information."]
        Ne {
            lhs: usize,
            rhs: usize,
            target_pc: BranchOffset,
        },
        // Compare two registers and jump to the given PC if the left-hand side is less than the right-hand side.
        #[description = "Compare the values in register P1 and P3. If reg(P3)<reg(P1) then jump to address P2. If the SQLITE_JUMPIFNULL bit of P5 is set and either reg(P1) or reg(P3) is NULL then the take the jump. If the SQLITE_JUMPIFNULL bit is clear then fall through if either operand is NULL. The SQLITE_AFF_MASK portion of P5 must be an affinity character - SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth. An attempt is made to coerce both inputs according to this affinity before the comparison is made. If the SQLITE_AFF_MASK is 0x00, then numeric affinity is used. Note that the affinity conversions are stored back into the input registers P1 and P3. So this opcode can cause persistent changes to registers P1 and P3. Once any conversions have taken place, and neither value is NULL, the values are compared. If both values are blobs then memcmp() is used to determine the results of the comparison. If both values are text, then the appropriate collating function specified in P4 is used to do the comparison. If P4 is not specified then memcmp() is used to compare text string. If both values are numeric, then a numeric comparison is used. If the two values are of different types, then numbers are considered less than strings and strings are considered less than blobs. This opcode saves the result of comparison for use by the new Jump opcode."]
        Lt {
            lhs: usize,
            rhs: usize,
            target_pc: BranchOffset,
        },
        // Compare two registers and jump to the given PC if the left-hand side is less than or equal to the right-hand side.
        #[description = "This works just like the Lt opcode except that the jump is taken if the content of register P3 is less than or equal to the content of register P1. See the Lt opcode for additional information."]
        Le {
            lhs: usize,
            rhs: usize,
            target_pc: BranchOffset,
        },
        // Compare two registers and jump to the given PC if the left-hand side is greater than the right-hand side.
        #[description = "This works just like the Lt opcode except that the jump is taken if the content of register P3 is greater than the content of register P1. See the Lt opcode for additional information."]
        Gt {
            lhs: usize,
            rhs: usize,
            target_pc: BranchOffset,
        },
        // Compare two registers and jump to the given PC if the left-hand side is greater than or equal to the right-hand side.
        #[description =  "This works just like the Lt opcode except that the jump is taken if the content of register P3 is greater than or equal to the content of register P1. See the Lt opcode for additional information."]
        Ge {
            lhs: usize,
            rhs: usize,
            target_pc: BranchOffset,
        },
        // Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[null_reg\] != 0)
        #[description = "Jump to P2 if the value in register P1 is true. The value is considered true if it is numeric and non-zero. If the value in P1 is NULL then take the jump if and only if P3 is non-zero."]
        If {
            reg: usize,              // P1
            target_pc: BranchOffset, // P2
            // P3. If r\[reg\] is null, jump iff r\[null_reg\] != 0
            null_reg: usize,
        },
        // Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[null_reg\] != 0)
        #[description = "Jump to P2 if the value in register P1 is False. The value is considered false if it has a numeric value of zero. If the value in P1 is NULL then take the jump if and only if P3 is non-zero."]
        IfNot {
            reg: usize,              // P1
            target_pc: BranchOffset, // P2
            // P3. If r\[reg\] is null, jump iff r\[null_reg\] != 0
            null_reg: usize,
        },
        // Open a cursor for reading.
        #[description = "Opens a cursor for reading."]
        OpenReadAsync {
            cursor_id: CursorID,
            root_page: PageIdx,
        },

        // Await for the completion of open cursor.
        #[description = "Awaits for the completion of open cursor."]
        OpenReadAwait,

        // Open a cursor for a pseudo-table that contains a single row.
        #[description = "Open a new cursor that points to a fake table that contains a single row of data. The content of that one row is the content of memory register P2. In other words, cursor P1 becomes an alias for the MEM_Blob content contained in register P2. A pseudo-table created by this opcode is used to hold a single row output from the sorter so that the row can be decomposed into individual columns using the Column opcode. The Column opcode is the only cursor opcode that works with a pseudo-table. P3 is the number of fields in the records that will be stored by the pseudo-table. If P2 is 0 or negative then the pseudo-cursor will return NULL for every column."]
        OpenPseudo {
            cursor_id: CursorID,
            content_reg: usize,
            num_fields: usize,
        },

        // Rewind the cursor to the beginning of the B-Tree.
        #[description = "Rewind the cursor to the beginning of the B-Tree."]
        RewindAsync {
        cursor_id: CursorID,
        },

        // Await for the completion of cursor rewind.
        #[description = "Await for the completion of cursor rewind."]
        RewindAwait {
            cursor_id: CursorID,
            pc_if_empty: BranchOffset,
        },

        #[description = "Represents an asynchronous operation with a cursor tracking the current position in the sequence."]
        LastAsync {
        cursor_id: CursorID,
        },

        #[description = "Represents a waiting state with a cursor tracking the current position and an offset for conditional branching if empty."]
        LastAwait {
            cursor_id: CursorID,
            pc_if_empty: BranchOffset,
        },

        // Read a column from the current row of the cursor.
        #[description = "Interpret the data that cursor P1 points to as a structure built using the MakeRecord instruction. (See the MakeRecord opcode for additional information about the format of the data.) Extract the P2-th column from this record. If there are less than (P2+1) values in the record, extract a NULL. The value extracted is stored in register P3. If the record contains fewer than P2 fields, then extract a NULL. Or, if the P4 argument is a P4_MEM use the value of the P4 argument as the result. If the OPFLAG_LENGTHARG bit is set in P5 then the result is guaranteed to only be used by the length() function or the equivalent. The content of large blobs is not loaded, thus saving CPU cycles. If the OPFLAG_TYPEOFARG bit is set then the result will only be used by the typeof() function or the IS NULL or IS NOT NULL operators or the equivalent. In this case, all content loading can be omitted."]
        Column {
            cursor_id: CursorID,
            column: usize,
            dest: usize,
        },

        // Make a record and write it to destination register.
        #[description = "Convert P2 registers beginning with P1 into the record format use as a data record in a database table or as a key in an index. The Column opcode can decode the record later. P4 may be a string that is P2 characters long. The N-th character of the string indicates the column affinity that should be used for the N-th field of the index key. The mapping from character to affinity is given by the SQLITE_AFF_ macros defined in sqliteInt.h. If P4 is NULL then all index fields have the affinity BLOB. The meaning of P5 depends on whether or not the SQLITE_ENABLE_NULL_TRIM compile-time option is enabled: * If SQLITE_ENABLE_NULL_TRIM is enabled, then the P5 is the index of the right-most table that can be null-trimmed. * If SQLITE_ENABLE_NULL_TRIM is omitted, then P5 has the value OPFLAG_NOCHNG_MAGIC if the MakeRecord opcode is allowed to accept no-change records with serial_type 10. This value is only used inside an assert() and does not affect the end result."]
        MakeRecord {
            start_reg: usize, // P1
            count: usize,     // P2
            dest_reg: usize,  // P3
        },

        // Emit a row of results.
        #[description = "The registers P1 through P1+P2-1 contain a single row of results. This opcode causes the sqlite3_step() call to terminate with an SQLITE_ROW return code and it sets up the sqlite3_stmt structure to provide access to the r(P1)..r(P1+P2-1) values as the result row."]
        ResultRow {
            start_reg: usize, // P1
            count: usize,     // P2
        },

        // Advance the cursor to the next row.
        #[description = "Advance the cursor to the next row."]
        NextAsync {
            cursor_id: CursorID,
        },

        // Await for the completion of cursor advance.
        #[description = "Await for the completion of cursor advance."]
        NextAwait {
            cursor_id: CursorID,
            pc_if_next: BranchOffset,
        },

        #[description = "Advance the cursor to the previous row."]
        PrevAsync {
            cursor_id: CursorID,
        },

        #[description = "Await for the completion of cursor to go back"]
        PrevAwait {
            cursor_id: CursorID,
            pc_if_next: BranchOffset,
        },

        // Halt the program.
        #[description = "Exit immediately. All open cursors, etc are closed automatically. P1 is the result code returned by sqlite3_exec(), sqlite3_reset(), or sqlite3_finalize(). For a normal halt, this should be SQLITE_OK (0). For errors, it can be some other value. If P1!=0 then P2 will determine whether or not to rollback the current transaction. Do not rollback if P2==OE_Fail. Do the rollback if P2==OE_Rollback. If P2==OE_Abort, then back out all changes that have occurred during this execution of the VDBE, but do not rollback the transaction. If P4 is not null then it is an error message string. P5 is a value between 0 and 4, inclusive, that modifies the P4 string. 0: (no change) 1: NOT NULL constraint failed: P4 2: UNIQUE constraint failed: P4 3: CHECK constraint failed: P4 4: FOREIGN KEY constraint failed: P4 If P5 is not zero and P4 is NULL, then everything after the \":\" is omitted. There is an implied \"Halt 0 0 0\" instruction inserted at the very end of every program. So a jump past the last instruction of the program is the same as executing Halt."]
        Halt {
            err_code: usize,
            description: String,
        },

        // Start a transaction.
        #[description = "Begin a transaction on database P1 if a transaction is not already active. If P2 is non-zero, then a write-transaction is started, or if a read-transaction is already active, it is upgraded to a write-transaction. If P2 is zero, then a read-transaction is started. If P2 is 2 or more then an exclusive transaction is started. P1 is the index of the database file on which the transaction is started. Index 0 is the main database file and index 1 is the file used for temporary tables. Indices of 2 or more are used for attached databases. If a write-transaction is started and the Vdbe.usesStmtJournal flag is true (this flag is set if the Vdbe may modify more than one row and may throw an ABORT exception), a statement transaction may also be opened. More specifically, a statement transaction is opened iff the database connection is currently not in autocommit mode, or if there are other active statements. A statement transaction allows the changes made by this VDBE to be rolled back after an error without having to roll back the entire transaction. If no error is encountered, the statement transaction will automatically commit when the VDBE halts. If P5!=0 then this opcode also checks the schema cookie against P3 and the schema generation counter against P4. The cookie changes its value whenever the database schema changes. This operation is used to detect when that the cookie has changed and that the current process needs to reread the schema. If the schema cookie in P3 differs from the schema cookie in the database header or if the schema generation counter in P4 differs from the current generation counter, then an SQLITE_SCHEMA error is raised and execution halts. The sqlite3_step() wrapper function might then reprepare the statement and rerun it from the beginning."]
        Transaction {
            write: bool,
        },

        // Branch to the given PC.
        #[description = "An unconditional jump to address P2. The next instruction executed will be the one at index P2 from the beginning of the program. The P1 parameter is not actually used by this opcode. However, it is sometimes set to 1 instead of 0 as a hint to the command-line shell that this Goto is the bottom of a loop and that the lines from P2 down to the current line should be indented for EXPLAIN output."]
        Goto {
            target_pc: BranchOffset,
        },

        // Stores the current program counter into register 'return_reg' then jumps to address target_pc.
        #[description = "Write the current address onto register P1 and then jump to address P2."]
        Gosub {
            target_pc: BranchOffset,
            return_reg: usize,
        },

        // Returns to the program counter stored in register 'return_reg'.
        #[description = "Jump to the address stored in register P1. If P1 is a return address register, then this accomplishes a return from a subroutine. If P3 is 1, then the jump is only taken if register P1 holds an integer values, otherwise execution falls through to the next opcode, and the Return becomes a no-op. If P3 is 0, then register P1 must hold an integer or else an assert() is raised. P3 should be set to 1 when this opcode is used in combination with BeginSubrtn, and set to 0 otherwise. The value in register P1 is unchanged by this opcode. P2 is not used by the byte-code engine. However, if P2 is positive and also less than the current address, then the \"EXPLAIN\" output formatter in the CLI will indent all opcodes from the P2 opcode up to be not including the current Return. P2 should be the first opcode in the subroutine from which this opcode is returning. Thus the P2 value is a byte-code indentation hint. See tag-20220407a in wherecode.c and shell.c."]
        Return {
            return_reg: usize,
        },

        // Write an integer value into a register.
        #[description = "The 32-bit integer value P1 is written into register P2."]
        Integer {
            value: i64,
            dest: usize,
        },

        // Write a float value into a register
        #[description = "P4 is a pointer to a 64-bit floating point value. Write that value into register P2."]
        Real {
            value: f64,
            dest: usize,
        },

        // If register holds an integer, transform it to a float
        #[description = "If register P1 holds an integer convert it to a real value. This opcode is used when extracting information from a column that has REAL affinity. Such column values may still be stored as integers, for space efficiency, but after extraction we want them to have only a real value."]
        RealAffinity {
            register: usize,
        },

        // Write a string value into a register.
        #[description =  "P4 points to a nul terminated UTF-8 string. This opcode is transformed into a String opcode before it is executed for the first time. During this transformation, the length of string P4 is computed and stored as the P1 parameter."]
        String8 {
            value: String,
            dest: usize,
        },

        // Write a blob value into a register.
        #[description = "P4 points to a blob of data P1 bytes long. Store this blob in register P2. If P4 is a NULL pointer, then construct a zero-filled blob that is P1 bytes long in P2."]
        Blob {
            value: Vec<u8>,
            dest: usize,
        },

        // Read the rowid of the current row.
        #[description = "Store in register P2 an integer which is the key of the table entry that P1 is currently point to. P1 can be either an ordinary table or a virtual table. There used to be a separate OP_VRowid opcode for use with virtual tables, but this one opcode now works for both table types."]
        RowId {
            cursor_id: CursorID,
            dest: usize,
        },

        // Seek to a rowid in the cursor. If not found, jump to the given PC. Otherwise, continue to the next instruction.
        #[description = "P1 is the index of a cursor open on an SQL table btree (with integer keys). If register P3 does not contain an integer or if P1 does not contain a record with rowid P3 then jump immediately to P2. Or, if P2 is 0, raise an SQLITE_CORRUPT error. If P1 does contain a record with rowid P3 then leave the cursor pointing at that record and fall through to the next instruction. The NotExists opcode performs the same operation, but with NotExists the P3 register must be guaranteed to contain an integer value. With this opcode, register P3 might not contain an integer. The NotFound opcode performs the same operation on index btrees (with arbitrary multi-value keys). This opcode leaves the cursor in a state where it cannot be advanced in either direction. In other words, the Next and Prev opcodes will not work following this opcode. See also: Found, NotFound, NoConflict, SeekRowid"]
        SeekRowid {
            cursor_id: CursorID,
            src_reg: usize,
            target_pc: BranchOffset,
        },

        // P1 is an open index cursor and P3 is a cursor on the corresponding table. This opcode does a deferred seek of the P3 table cursor to the row that corresponds to the current row of P1.
        // This is a deferred seek. Nothing actually happens until the cursor is used to read a record. That way, if no reads occur, no unnecessary I/O happens.
        #[description = "P1 is an open index cursor and P3 is a cursor on the corresponding table. This opcode does a deferred seek of the P3 table cursor to the row that corresponds to the current row of P1. This is a deferred seek. Nothing actually happens until the cursor is used to read a record. That way, if no reads occur, no unnecessary I/O happens. P4 may be an array of integers (type P4_INTARRAY) containing one entry for each column in the P3 table. If array entry a(i) is non-zero, then reading column a(i)-1 from cursor P3 is equivalent to performing the deferred seek and then reading column i from P1. This information is stored in P3 and used to redirect reads against P3 over to P1, thus possibly avoiding the need to seek and read cursor P3."]
        DeferredSeek {
            index_cursor_id: CursorID,
            table_cursor_id: CursorID,
        },

        // If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
        // If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
        // Seek to the first index entry that is greater than or equal to the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
        #[description = "If cursor P1 refers to an SQL table (B-Tree that uses integer keys), use the value in register P3 as the key. If cursor P1 refers to an SQL index, then P3 is the first in an array of P4 registers that are used as an unpacked index key. Reposition cursor P1 so that it points to the smallest entry that is greater than or equal to the key value. If there are no records greater than or equal to the key and P2 is not zero, then jump to P2. If the cursor P1 was opened using the OPFLAG_SEEKEQ flag, then this opcode will either land on a record that exactly matches the key, or else it will cause a jump to P2. When the cursor is OPFLAG_SEEKEQ, this opcode must be followed by an IdxLE opcode with the same arguments. The IdxGT opcode will be skipped if this opcode succeeds, but the IdxGT opcode will be used on subsequent loop iterations. The OPFLAG_SEEKEQ flags is a hint to the btree layer to say that this is an equality search. This opcode leaves the cursor configured to move in forward order, from the beginning toward the end. In other words, the cursor is configured to use Next, not Prev. See also: Found, NotFound, SeekLt, SeekGt, SeekLe"]
        SeekGE {
            is_index: bool,
            cursor_id: CursorID,
            start_reg: usize,
            num_regs: usize,
            target_pc: BranchOffset,
        },

        // If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
        // If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
        // Seek to the first index entry that is greater than the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
        #[description = "If cursor P1 refers to an SQL table (B-Tree that uses integer keys), use the value in register P3 as a key. If cursor P1 refers to an SQL index, then P3 is the first in an array of P4 registers that are used as an unpacked index key. Reposition cursor P1 so that it points to the smallest entry that is greater than the key value. If there are no records greater than the key and P2 is not zero, then jump to P2. This opcode leaves the cursor configured to move in forward order, from the beginning toward the end. In other words, the cursor is configured to use Next, not Prev. See also: Found, NotFound, SeekLt, SeekGe, SeekLe"]
        SeekGT {
            is_index: bool,
            cursor_id: CursorID,
            start_reg: usize,
            num_regs: usize,
            target_pc: BranchOffset,
        },

        // The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
        // If the P1 index entry is greater or equal than the key value then jump to P2. Otherwise fall through to the next instruction.
        #[description = "The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end. If the P1 index entry is greater than or equal to the key value then jump to P2. Otherwise fall through to the next instruction."]
        IdxGE {
            cursor_id: CursorID,
            start_reg: usize,
            num_regs: usize,
            target_pc: BranchOffset,
        },

        // The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
        // If the P1 index entry is greater than the key value then jump to P2. Otherwise fall through to the next instruction.
        #[description = "The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end. If the P1 index entry is greater than the key value then jump to P2. Otherwise fall through to the next instruction."]
        IdxGT {
            cursor_id: CursorID,
            start_reg: usize,
            num_regs: usize,
            target_pc: BranchOffset,
        },

        // Decrement the given register and jump to the given PC if the result is zero.
        #[description = "Register P1 must hold an integer. Decrement the value in P1 and jump to P2 if the new value is exactly zero."]
        DecrJumpZero {
            reg: usize,
            target_pc: BranchOffset,
        },

        #[description = "Execute the xStep function for an aggregate. The function has P5 arguments. P4 is a pointer to the FuncDef structure that specifies the function. Register P3 is the accumulator. The P5 arguments are taken from register P2 and its successors."]
        AggStep {
            acc_reg: usize,
            col: usize,
            delimiter: usize,
            func: AggFunc,
        },

        #[description = "P1 is the memory location that is the accumulator for an aggregate or window function. Execute the finalizer function for an aggregate and store the result in P1. P2 is the number of arguments that the step function takes and P4 is a pointer to the FuncDef for this function. The P2 argument is not used by this opcode. It is only there to disambiguate functions that can take varying numbers of arguments. The P4 argument is only needed for the case where the step function was not previously called."]
        AggFinal {
            register: usize,
            func: AggFunc,
        },

        // Open a sorter.
        #[description =  "This opcode works like OpenEphemeral except that it opens a transient index that is specifically designed to sort large tables using an external merge-sort algorithm. If argument P3 is non-zero, then it indicates that the sorter may assume that a stable sort considering the first P3 fields of each key is sufficient to produce the required results."]
        SorterOpen {
            cursor_id: CursorID, // P1
            columns: usize,      // P2
            order: OwnedRecord,  // P4. 0 if ASC and 1 if DESC
        },

        // Insert a row into the sorter.
        #[description = "Register P2 holds an SQL index key made using the MakeRecord instructions. This opcode writes that key into the sorter P1. Data for the entry is nil."]
        SorterInsert {
            cursor_id: CursorID,
            record_reg: usize,
        },

        // Sort the rows in the sorter.
        #[description = "After all records have been inserted into the Sorter object identified by P1, invoke this opcode to actually do the sorting. Jump to P2 if there are no records to be sorted. This opcode is an alias for Sort and Rewind that is used for Sorter objects."]
        SorterSort {
            cursor_id: CursorID,
            pc_if_empty: BranchOffset,
        },

        // Retrieve the next row from the sorter.
        #[description = "Write into register P2 the current sorter data for sorter cursor P1. Then clear the column header cache on cursor P3. This opcode is normally used to move a record out of the sorter and into a register that is the source for a pseudo-table cursor created using OpenPseudo. That pseudo-table cursor is the one that is identified by parameter P3. Clearing the P3 column cache as part of this opcode saves us from having to issue a separate NullRow instruction to clear that cache."]
        SorterData {
            cursor_id: CursorID,  // P1
            dest_reg: usize,      // P2
            pseudo_cursor: usize, // P3
        },

        // Advance to the next row in the sorter.
        #[description = "This opcode works just like Next except that P1 must be a sorter object for which the SorterSort opcode has been invoked. This opcode advances the cursor to the next sorted record, or jumps to P2 if there are no more sorted records."]
        SorterNext {
            cursor_id: CursorID,
            pc_if_next: BranchOffset,
        },

        // Function
        #[description = "Invoke a user function (P4 is a pointer to an sqlite3_context object that contains a pointer to the function to be run) with arguments taken from register P2 and successors. The number of arguments is in the sqlite3_context object that P4 points to. The result of the function is stored in register P3. Register P3 must not be one of the function inputs. P1 is a 32-bit bitmask indicating whether or not each argument to the function was determined to be constant at compile time. If the first argument was constant then bit 0 of P1 is set. This is used to determine whether meta data associated with a user function argument using the sqlite3_set_auxdata() API may be safely retained until the next invocation of this opcode. See also: AggStep, AggFinal, PureFunc"]
        Function {
            constant_mask: i32, // P1
            start_reg: usize,   // P2, start of argument registers
            dest: usize,        // P3
            func: FuncCtx,      // P4
        },

        #[description = "Set up register P1 so that it will Yield to the coroutine located at address P3. If P2!=0 then the coroutine implementation immediately follows this opcode. So jump over the coroutine implementation to address P2. See also: EndCoroutine"]
        InitCoroutine {
            yield_reg: usize,
            jump_on_definition: BranchOffset,
            start_offset: BranchOffset,
        },

        #[description = "The instruction at the address in register P1 is a Yield. Jump to the P2 parameter of that Yield. After the jump, the value register P1 is left with a value such that subsequent OP_Yields go back to the this same EndCoroutine instruction. See also: InitCoroutine"]
        EndCoroutine {
            yield_reg: usize,
        },

        #[description = "Swap the program counter with the value in register P1. This has the effect of yielding to a coroutine. If the coroutine that is launched by this instruction ends with Yield or Return then continue to the next instruction. But if the coroutine launched by this instruction ends with EndCoroutine, then jump to P2 rather than continuing with the next instruction. See also: InitCoroutine"]
        Yield {
            yield_reg: usize,
            end_offset: BranchOffset,
        },

        #[description = "Insert at CursorID"]
        InsertAsync {
            cursor: CursorID,
            key_reg: usize,    // Must be int.
            record_reg: usize, // Blob of record data.
            flag: usize,       // Flags used by insert, for now not used.
        },
        #[description = "Await to Insert at CursorID"]
        InsertAwait {
            cursor_id: usize,
        },

        #[description = "Get a new integer record number (a.k.a \"rowid\") used as the key to a table. The record number is not previously used as a key in the database table that cursor P1 points to. The new record number is written written to register P2. If P3>0 then P3 is a register in the root frame of this VDBE that holds the largest previously generated record number. No new record numbers are allowed to be less than this value. When this value reaches its maximum, an SQLITE_FULL error is generated. The P3 register is updated with the ' generated record number. This P3 mechanism is used to help implement the AUTOINCREMENT feature."]
        NewRowid {
            cursor: CursorID,        // P1
            rowid_reg: usize,        // P2  Destination register to store the new rowid
            prev_largest_reg: usize, // P3 Previous largest rowid in the table (Not used for now)
        },
        #[description = "Force the value in register P1 to be an integer. If the value in P1 is not an integer and cannot be converted into an integer without data loss, then jump immediately to P2, or if P2==0 raise an SQLITE_MISMATCH exception."]
        MustBeInt {
            reg: usize,
        },

        #[description = "Set register P1 to have the value NULL as seen by the MakeRecord instruction, but do not free any string or blob memory associated with the register, so that if the value was a string or blob that was previously copied using SCopy, the copies will continue to be valid."]
        SoftNull {
            reg: usize,
        },

        #[description = "P1 is the index of a cursor open on an SQL table btree (with integer keys). P3 is an integer rowid. If P1 does not contain a record with rowid P3 then jump immediately to P2. Or, if P2 is 0, raise an SQLITE_CORRUPT error. If P1 does contain a record with rowid P3 then leave the cursor pointing at that record and fall through to the next instruction. The SeekRowid opcode performs the same operation but also allows the P3 register to contain a non-integer value, in which case the jump is always taken. This opcode requires that P3 always contain an integer. The NotFound opcode performs the same operation on index btrees (with arbitrary multi-value keys). This opcode leaves the cursor in a state where it cannot be advanced in either direction. In other words, the Next and Prev opcodes will not work following this opcode. See also: Found, NotFound, NoConflict, SeekRowid"]
        NotExists {
            cursor: CursorID,
            rowid_reg: usize,
            target_pc: BranchOffset,
        },
        #[description = "Open write at CursorID"]
        OpenWriteAsync {
            cursor_id: CursorID,
            root_page: PageIdx,
        },
        #[description = "Await to open write"]
        OpenWriteAwait {},

        #[description = "Make a copy of registers P1..P1+P3 into registers P2..P2+P3. If the 0x0002 bit of P5 is set then also clear the MEM_Subtype flag in the destination. The 0x0001 bit of P5 indicates that this Copy opcode cannot be merged. The 0x0001 bit is used by the query planner and does not come into play during query execution. This instruction makes a deep copy of the value. A duplicate is made of any string or blob constant. See also SCopy."]
        Copy {
            src_reg: usize,
            dst_reg: usize,
            amount: usize, // 0 amount means we include src_reg, dst_reg..=dst_reg+amount = src_reg..=src_reg+amount
        },

        // Allocate a new b-tree.
        #[description = "Allocate a new b-tree in the main database file if P1==0 or in the TEMP database file if P1==1 or in an attached database if P1>1. The P3 argument must be 1 (BTREE_INTKEY) for a rowid table it must be 2 (BTREE_BLOBKEY) for an index or WITHOUT ROWID table. The root page number of the new b-tree is stored in register P2."]
        CreateBtree {
            // Allocate b-tree in main database if zero or in temp database if non-zero (P1).
            db: usize,
            // The root page of the new b-tree (P2).
            root: usize,
            // Flags (P3).
            flags: usize,
        },

        // Close a cursor.
        #[description = "Close a cursor previously opened as P1. If P1 is not currently open, this instruction is a no-op."]
        Close {
            cursor_id: CursorID,
        },

        // Check if the register is null.
        #[description = "Jump to P2 if the value in register P1 is NULL."]
        IsNull {
            // Source register (P1).
            src: usize,

            // Jump to this PC if the register is null (P2).
            target_pc: BranchOffset,
        },
        #[description = "Read and parse all entries from the schema table of database P1 that match the WHERE clause P4. If P4 is a NULL pointer, then the entire schema for P1 is reparsed. This opcode invokes the parser to create a new virtual machine, then runs the new virtual machine. It is thus a re-entrant opcode."]
        ParseSchema {
            db: usize,
            where_clause: String,
        }
    }
}

// Index of insn in list of insns
type InsnReference = usize;

pub enum StepResult<'a> {
    Done,
    IO,
    Row(Record<'a>),
    Interrupt,
}

/// If there is I/O, the instruction is restarted.
/// Evaluate a Result<CursorResult<T>>, if IO return Ok(StepResult::IO).
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr? {
            CursorResult::Ok(v) => v,
            CursorResult::IO => return Ok(StepResult::IO),
        }
    };
}

struct RegexCache {
    like: HashMap<String, Regex>,
    glob: HashMap<String, Regex>,
}
impl RegexCache {
    fn new() -> Self {
        RegexCache {
            like: HashMap::new(),
            glob: HashMap::new(),
        }
    }
}

/// The program state describes the environment in which the program executes.
pub struct ProgramState {
    pub pc: BranchOffset,
    cursors: RefCell<BTreeMap<CursorID, Box<dyn Cursor>>>,
    registers: Vec<OwnedValue>,
    last_compare: Option<std::cmp::Ordering>,
    deferred_seek: Option<(CursorID, CursorID)>,
    ended_coroutine: bool, // flag to notify yield coroutine finished
    regex_cache: RegexCache,
    interrupted: bool,
}

impl ProgramState {
    pub fn new(max_registers: usize) -> Self {
        let cursors = RefCell::new(BTreeMap::new());
        let mut registers = Vec::with_capacity(max_registers);
        registers.resize(max_registers, OwnedValue::Null);
        Self {
            pc: 0,
            cursors,
            registers,
            last_compare: None,
            deferred_seek: None,
            ended_coroutine: false,
            regex_cache: RegexCache::new(),
            interrupted: false,
        }
    }

    pub fn column_count(&self) -> usize {
        self.registers.len()
    }

    pub fn column(&self, i: usize) -> Option<String> {
        Some(format!("{:?}", self.registers[i]))
    }

    pub fn interrupt(&mut self) {
        self.interrupted = true;
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted
    }
}

#[derive(Debug)]
pub struct Program {
    pub max_registers: usize,
    pub insns: Vec<Insn>,
    pub cursor_ref: Vec<(Option<String>, Option<Table>)>,
    pub database_header: Rc<RefCell<DatabaseHeader>>,
    pub comments: HashMap<BranchOffset, &'static str>,
    pub connection: Weak<Connection>,
    pub auto_commit: bool,
}

impl Program {
    pub fn explain(&self) {
        println!("addr  opcode             p1    p2    p3    p4             p5  comment");
        println!("----  -----------------  ----  ----  ----  -------------  --  -------");
        let mut indent_count: usize = 0;
        let indent = "  ";
        let mut prev_insn: Option<&Insn> = None;
        for (addr, insn) in self.insns.iter().enumerate() {
            indent_count = get_indent_count(indent_count, insn, prev_insn);
            print_insn(
                self,
                addr as InsnReference,
                insn,
                indent.repeat(indent_count),
            );
            prev_insn = Some(insn);
        }
    }

    pub fn step<'a>(
        &self,
        state: &'a mut ProgramState,
        pager: Rc<Pager>,
    ) -> Result<StepResult<'a>> {
        loop {
            if state.is_interrupted() {
                return Ok(StepResult::Interrupt);
            }
            let insn = &self.insns[state.pc as usize];
            trace_insn(self, state.pc as InsnReference, insn);
            let mut cursors = state.cursors.borrow_mut();
            match insn {
                Insn::Init { target_pc } => {
                    assert!(*target_pc >= 0);
                    state.pc = *target_pc;
                }
                Insn::Add { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs + rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs + rhs);
                        }
                        (OwnedValue::Float(f), OwnedValue::Integer(i))
                        | (OwnedValue::Integer(i), OwnedValue::Float(f)) => {
                            state.registers[dest] = OwnedValue::Float(*f + *i as f64);
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Null => {
                                    state.registers[dest] = OwnedValue::Null;
                                }
                                OwnedValue::Integer(i) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc + *i as f64);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Integer(acc + i);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Float(f) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc + f);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Float(*acc as f64 + f);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Agg(aggctx2) => {
                                    let acc = aggctx.final_value();
                                    let acc2 = aggctx2.final_value();
                                    match (acc, acc2) {
                                        (OwnedValue::Integer(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] = OwnedValue::Integer(acc + acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] = OwnedValue::Float(acc + acc2);
                                        }
                                        (OwnedValue::Integer(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(*acc as f64 + acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(acc + *acc2 as f64);
                                        }
                                        _ => {
                                            todo!("{:?} {:?}", acc, acc2);
                                        }
                                    }
                                }
                                rest => unimplemented!("{:?}", rest),
                            }
                        }
                        _ => {
                            todo!();
                        }
                    }
                    state.pc += 1;
                }
                Insn::Subtract { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs - rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs - rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs - *rhs as f64);
                        }
                        (OwnedValue::Integer(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(*lhs as f64 - rhs);
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), rhs) => match rhs {
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            OwnedValue::Integer(i) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(acc - *i as f64);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Integer(acc - i);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Float(f) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(acc - f);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Float(*acc as f64 - f);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Agg(aggctx2) => {
                                let acc = aggctx.final_value();
                                let acc2 = aggctx2.final_value();
                                match (acc, acc2) {
                                    (OwnedValue::Integer(acc), OwnedValue::Integer(acc2)) => {
                                        state.registers[dest] = OwnedValue::Integer(acc - acc2);
                                    }
                                    (OwnedValue::Float(acc), OwnedValue::Float(acc2)) => {
                                        state.registers[dest] = OwnedValue::Float(acc - acc2);
                                    }
                                    (OwnedValue::Integer(acc), OwnedValue::Float(acc2)) => {
                                        state.registers[dest] =
                                            OwnedValue::Float(*acc as f64 - acc2);
                                    }
                                    (OwnedValue::Float(acc), OwnedValue::Integer(acc2)) => {
                                        state.registers[dest] =
                                            OwnedValue::Float(acc - *acc2 as f64);
                                    }
                                    _ => {
                                        todo!("{:?} {:?}", acc, acc2);
                                    }
                                }
                            }
                            rest => unimplemented!("{:?}", rest),
                        },
                        (lhs, OwnedValue::Agg(aggctx)) => match lhs {
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            OwnedValue::Integer(i) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(*i as f64 - acc);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Integer(i - acc);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Float(f) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(f - acc);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Float(f - *acc as f64);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            rest => unimplemented!("{:?}", rest),
                        },
                        others => {
                            todo!("{:?}", others);
                        }
                    }
                    state.pc += 1;
                }
                Insn::Multiply { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs * rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs * rhs);
                        }
                        (OwnedValue::Integer(i), OwnedValue::Float(f))
                        | (OwnedValue::Float(f), OwnedValue::Integer(i)) => {
                            state.registers[dest] = OwnedValue::Float(*i as f64 * { *f });
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Null => {
                                    state.registers[dest] = OwnedValue::Null;
                                }
                                OwnedValue::Integer(i) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc * *i as f64);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Integer(acc * i);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Float(f) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc * f);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Float(*acc as f64 * f);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Agg(aggctx2) => {
                                    let acc = aggctx.final_value();
                                    let acc2 = aggctx2.final_value();
                                    match (acc, acc2) {
                                        (OwnedValue::Integer(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] = OwnedValue::Integer(acc * acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] = OwnedValue::Float(acc * acc2);
                                        }
                                        (OwnedValue::Integer(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(*acc as f64 * acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(acc * *acc2 as f64);
                                        }
                                        _ => {
                                            todo!("{:?} {:?}", acc, acc2);
                                        }
                                    }
                                }
                                rest => unimplemented!("{:?}", rest),
                            }
                        }
                        others => {
                            todo!("{:?}", others);
                        }
                    }
                    state.pc += 1;
                }
                Insn::Divide { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        // If the divisor is zero, the result is NULL.
                        (_, OwnedValue::Integer(0)) | (_, OwnedValue::Float(0.0)) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs / rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs / rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs / *rhs as f64);
                        }
                        (OwnedValue::Integer(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(*lhs as f64 / rhs);
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), rhs) => match rhs {
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            OwnedValue::Integer(i) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(acc / *i as f64);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Integer(acc / i);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Float(f) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(acc / f);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Float(*acc as f64 / f);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Agg(aggctx2) => {
                                let acc = aggctx.final_value();
                                let acc2 = aggctx2.final_value();
                                match (acc, acc2) {
                                    (OwnedValue::Integer(acc), OwnedValue::Integer(acc2)) => {
                                        state.registers[dest] = OwnedValue::Integer(acc / acc2);
                                    }
                                    (OwnedValue::Float(acc), OwnedValue::Float(acc2)) => {
                                        state.registers[dest] = OwnedValue::Float(acc / acc2);
                                    }
                                    (OwnedValue::Integer(acc), OwnedValue::Float(acc2)) => {
                                        state.registers[dest] =
                                            OwnedValue::Float(*acc as f64 / acc2);
                                    }
                                    (OwnedValue::Float(acc), OwnedValue::Integer(acc2)) => {
                                        state.registers[dest] =
                                            OwnedValue::Float(acc / *acc2 as f64);
                                    }
                                    _ => {
                                        todo!("{:?} {:?}", acc, acc2);
                                    }
                                }
                            }
                            rest => unimplemented!("{:?}", rest),
                        },
                        (lhs, OwnedValue::Agg(aggctx)) => match lhs {
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            OwnedValue::Integer(i) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(*i as f64 / acc);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Integer(i / acc);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Float(f) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(f / acc);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Float(f / *acc as f64);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            rest => unimplemented!("{:?}", rest),
                        },
                        others => {
                            todo!("{:?}", others);
                        }
                    }
                    state.pc += 1;
                }
                Insn::BitAnd { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        // handle 0 and null cases
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (_, OwnedValue::Integer(0))
                        | (OwnedValue::Integer(0), _)
                        | (_, OwnedValue::Float(0.0))
                        | (OwnedValue::Float(0.0), _) => {
                            state.registers[dest] = OwnedValue::Integer(0);
                        }
                        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(lh & rh);
                        }
                        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(*lh as i64 & *rh as i64);
                        }
                        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(*lh as i64 & rh);
                        }
                        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(lh & *rh as i64);
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Agg(aggctx2) => {
                                    match (aggctx.final_value(), aggctx2.final_value()) {
                                        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                                            state.registers[dest] = OwnedValue::Integer(lh & rh);
                                        }
                                        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(*lh as i64 & *rh as i64);
                                        }
                                        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(lh & *rh as i64);
                                        }
                                        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(*lh as i64 & rh);
                                        }
                                        _ => {
                                            unimplemented!(
                                                "{:?} {:?}",
                                                aggctx.final_value(),
                                                aggctx2.final_value()
                                            );
                                        }
                                    }
                                }
                                other => match (aggctx.final_value(), other) {
                                    (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                                        state.registers[dest] = OwnedValue::Null;
                                    }
                                    (_, OwnedValue::Integer(0))
                                    | (OwnedValue::Integer(0), _)
                                    | (_, OwnedValue::Float(0.0))
                                    | (OwnedValue::Float(0.0), _) => {
                                        state.registers[dest] = OwnedValue::Integer(0);
                                    }
                                    (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                                        state.registers[dest] = OwnedValue::Integer(lh & rh);
                                    }
                                    (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                                        state.registers[dest] =
                                            OwnedValue::Integer(*lh as i64 & rh);
                                    }
                                    (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                                        state.registers[dest] =
                                            OwnedValue::Integer(lh & *rh as i64);
                                    }
                                    _ => {
                                        unimplemented!("{:?} {:?}", aggctx.final_value(), other);
                                    }
                                },
                            }
                        }
                        _ => {
                            unimplemented!("{:?} {:?}", state.registers[lhs], state.registers[rhs]);
                        }
                    }
                    state.pc += 1;
                }
                Insn::BitOr { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(lh | rh);
                        }
                        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(*lh as i64 | rh);
                        }
                        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(lh | *rh as i64);
                        }
                        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(*lh as i64 | *rh as i64);
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Agg(aggctx2) => {
                                    let final_lhs = aggctx.final_value();
                                    let final_rhs = aggctx2.final_value();
                                    match (final_lhs, final_rhs) {
                                        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                                            state.registers[dest] = OwnedValue::Integer(lh | rh);
                                        }
                                        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(*lh as i64 | *rh as i64);
                                        }
                                        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(lh | *rh as i64);
                                        }
                                        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(*lh as i64 | rh);
                                        }
                                        _ => {
                                            unimplemented!("{:?} {:?}", final_lhs, final_rhs);
                                        }
                                    }
                                }
                                other => match (aggctx.final_value(), other) {
                                    (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                                        state.registers[dest] = OwnedValue::Null;
                                    }
                                    (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                                        state.registers[dest] = OwnedValue::Integer(lh | rh);
                                    }
                                    (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                                        state.registers[dest] =
                                            OwnedValue::Integer(*lh as i64 | rh);
                                    }
                                    (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                                        state.registers[dest] =
                                            OwnedValue::Integer(lh | *rh as i64);
                                    }
                                    _ => {
                                        unimplemented!("{:?} {:?}", aggctx.final_value(), other);
                                    }
                                },
                            }
                        }
                        _ => {
                            unimplemented!("{:?} {:?}", state.registers[lhs], state.registers[rhs]);
                        }
                    }
                    state.pc += 1;
                }
                Insn::BitNot { reg, dest } => {
                    let reg = *reg;
                    let dest = *dest;
                    match &state.registers[reg] {
                        OwnedValue::Integer(i) => state.registers[dest] = OwnedValue::Integer(!i),
                        OwnedValue::Float(f) => {
                            state.registers[dest] = OwnedValue::Integer(!{ *f as i64 })
                        }
                        OwnedValue::Null => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        OwnedValue::Agg(aggctx) => match aggctx.final_value() {
                            OwnedValue::Integer(i) => {
                                state.registers[dest] = OwnedValue::Integer(!i);
                            }
                            OwnedValue::Float(f) => {
                                state.registers[dest] = OwnedValue::Integer(!{ *f as i64 });
                            }
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            _ => unimplemented!("{:?}", aggctx),
                        },
                        _ => {
                            unimplemented!("{:?}", state.registers[reg]);
                        }
                    }
                    state.pc += 1;
                }
                Insn::Null { dest, dest_end } => {
                    if let Some(dest_end) = dest_end {
                        for i in *dest..=*dest_end {
                            state.registers[i] = OwnedValue::Null;
                        }
                    } else {
                        state.registers[*dest] = OwnedValue::Null;
                    }
                    state.pc += 1;
                }
                Insn::NullRow { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.set_null_flag(true);
                    state.pc += 1;
                }
                Insn::Compare {
                    start_reg_a,
                    start_reg_b,
                    count,
                } => {
                    let start_reg_a = *start_reg_a;
                    let start_reg_b = *start_reg_b;
                    let count = *count;

                    if start_reg_a + count > start_reg_b {
                        return Err(LimboError::InternalError(
                            "Compare registers overlap".to_string(),
                        ));
                    }

                    let mut cmp = None;
                    for i in 0..count {
                        let a = &state.registers[start_reg_a + i];
                        let b = &state.registers[start_reg_b + i];
                        cmp = Some(a.cmp(b));
                        if cmp != Some(std::cmp::Ordering::Equal) {
                            break;
                        }
                    }
                    state.last_compare = cmp;
                    state.pc += 1;
                }
                Insn::Jump {
                    target_pc_lt,
                    target_pc_eq,
                    target_pc_gt,
                } => {
                    let cmp = state.last_compare.take();
                    if cmp.is_none() {
                        return Err(LimboError::InternalError(
                            "Jump without compare".to_string(),
                        ));
                    }
                    let target_pc = match cmp.unwrap() {
                        std::cmp::Ordering::Less => *target_pc_lt,
                        std::cmp::Ordering::Equal => *target_pc_eq,
                        std::cmp::Ordering::Greater => *target_pc_gt,
                    };
                    assert!(target_pc >= 0);
                    state.pc = target_pc;
                }
                Insn::Move {
                    source_reg,
                    dest_reg,
                    count,
                } => {
                    let source_reg = *source_reg;
                    let dest_reg = *dest_reg;
                    let count = *count;
                    for i in 0..count {
                        state.registers[dest_reg + i] = std::mem::replace(
                            &mut state.registers[source_reg + i],
                            OwnedValue::Null,
                        );
                    }
                    state.pc += 1;
                }
                Insn::IfPos {
                    reg,
                    target_pc,
                    decrement_by,
                } => {
                    assert!(*target_pc >= 0);
                    let reg = *reg;
                    let target_pc = *target_pc;
                    match &state.registers[reg] {
                        OwnedValue::Integer(n) if *n > 0 => {
                            state.pc = target_pc;
                            state.registers[reg] = OwnedValue::Integer(*n - *decrement_by as i64);
                        }
                        OwnedValue::Integer(_) => {
                            state.pc += 1;
                        }
                        _ => {
                            return Err(LimboError::InternalError(
                                "IfPos: the value in the register is not an integer".into(),
                            ));
                        }
                    }
                }
                Insn::NotNull { reg, target_pc } => {
                    assert!(*target_pc >= 0);
                    let reg = *reg;
                    let target_pc = *target_pc;
                    match &state.registers[reg] {
                        OwnedValue::Null => {
                            state.pc += 1;
                        }
                        _ => {
                            state.pc = target_pc;
                        }
                    }
                }

                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] == state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] != state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Lt {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] < state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Le {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] <= state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Gt {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] > state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Ge {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] >= state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::If {
                    reg,
                    target_pc,
                    null_reg,
                } => {
                    assert!(*target_pc >= 0);
                    if exec_if(&state.registers[*reg], &state.registers[*null_reg], false) {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::IfNot {
                    reg,
                    target_pc,
                    null_reg,
                } => {
                    assert!(*target_pc >= 0);
                    if exec_if(&state.registers[*reg], &state.registers[*null_reg], true) {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::OpenReadAsync {
                    cursor_id,
                    root_page,
                } => {
                    let cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        *root_page,
                        self.database_header.clone(),
                    ));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::OpenReadAwait => {
                    state.pc += 1;
                }
                Insn::OpenPseudo {
                    cursor_id,
                    content_reg: _,
                    num_fields: _,
                } => {
                    let cursor = Box::new(PseudoCursor::new());
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::RewindAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    return_if_io!(cursor.rewind());
                    state.pc += 1;
                }
                Insn::LastAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    return_if_io!(cursor.last());
                    state.pc += 1;
                }
                Insn::LastAwait {
                    cursor_id,
                    pc_if_empty,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if cursor.is_empty() {
                        state.pc = *pc_if_empty;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::RewindAwait {
                    cursor_id,
                    pc_if_empty,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if cursor.is_empty() {
                        state.pc = *pc_if_empty;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Column {
                    cursor_id,
                    column,
                    dest,
                } => {
                    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seek.take() {
                        let index_cursor = cursors.get_mut(&index_cursor_id).unwrap();
                        let rowid = index_cursor.rowid()?;
                        let table_cursor = cursors.get_mut(&table_cursor_id).unwrap();
                        match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                            CursorResult::Ok(_) => {}
                            CursorResult::IO => {
                                state.deferred_seek = Some((index_cursor_id, table_cursor_id));
                                return Ok(StepResult::IO);
                            }
                        }
                    }

                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    if let Some(ref record) = *cursor.record()? {
                        let null_flag = cursor.get_null_flag();
                        state.registers[*dest] = if null_flag {
                            OwnedValue::Null
                        } else {
                            record.values[*column].clone()
                        };
                    } else {
                        state.registers[*dest] = OwnedValue::Null;
                    }
                    state.pc += 1;
                }
                Insn::MakeRecord {
                    start_reg,
                    count,
                    dest_reg,
                } => {
                    let record = make_owned_record(&state.registers, start_reg, count);
                    state.registers[*dest_reg] = OwnedValue::Record(record);
                    state.pc += 1;
                }
                Insn::ResultRow { start_reg, count } => {
                    let record = make_record(&state.registers, start_reg, count);
                    state.pc += 1;
                    return Ok(StepResult::Row(record));
                }
                Insn::NextAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.set_null_flag(false);
                    return_if_io!(cursor.next());
                    state.pc += 1;
                }
                Insn::PrevAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.set_null_flag(false);
                    return_if_io!(cursor.prev());
                    state.pc += 1;
                }
                Insn::PrevAwait {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(*pc_if_next >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if !cursor.is_empty() {
                        state.pc = *pc_if_next;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::NextAwait {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(*pc_if_next >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if !cursor.is_empty() {
                        state.pc = *pc_if_next;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Halt {
                    err_code,
                    description,
                } => {
                    match *err_code {
                        0 => {}
                        SQLITE_CONSTRAINT_PRIMARYKEY => {
                            return Err(LimboError::Constraint(format!(
                                "UNIQUE constraint failed: {} (19)",
                                description
                            )));
                        }
                        _ => {
                            return Err(LimboError::Constraint(format!(
                                "undocumented halt error code {}",
                                description
                            )));
                        }
                    }
                    log::trace!("Halt auto_commit {}", self.auto_commit);
                    if self.auto_commit {
                        return match pager.end_tx() {
                            Ok(crate::storage::wal::CheckpointStatus::IO) => Ok(StepResult::IO),
                            Ok(crate::storage::wal::CheckpointStatus::Done) => Ok(StepResult::Done),
                            Err(e) => Err(e),
                        };
                    } else {
                        return Ok(StepResult::Done);
                    }
                }
                Insn::Transaction { write } => {
                    let connection = self.connection.upgrade().unwrap();
                    if let Some(db) = connection.db.upgrade() {
                        // TODO(pere): are backpointers good ?? this looks ugly af
                        // upgrade transaction if needed
                        let new_transaction_state =
                            match (db.transaction_state.borrow().clone(), write) {
                                (crate::TransactionState::Write, true) => TransactionState::Write,
                                (crate::TransactionState::Write, false) => TransactionState::Write,
                                (crate::TransactionState::Read, true) => TransactionState::Write,
                                (crate::TransactionState::Read, false) => TransactionState::Read,
                                (crate::TransactionState::None, true) => TransactionState::Read,
                                (crate::TransactionState::None, false) => TransactionState::Read,
                            };
                        // TODO(Pere):
                        //  1. lock wal
                        //  2. lock shared
                        //  3. lock write db if write
                        db.transaction_state.replace(new_transaction_state.clone());
                        if matches!(new_transaction_state, TransactionState::Write) {
                            pager.begin_read_tx()?;
                        } else {
                            pager.begin_write_tx()?;
                        }
                    }
                    state.pc += 1;
                }
                Insn::Goto { target_pc } => {
                    assert!(*target_pc >= 0);
                    state.pc = *target_pc;
                }
                Insn::Gosub {
                    target_pc,
                    return_reg,
                } => {
                    assert!(*target_pc >= 0);
                    state.registers[*return_reg] = OwnedValue::Integer(state.pc + 1);
                    state.pc = *target_pc;
                }
                Insn::Return { return_reg } => {
                    if let OwnedValue::Integer(pc) = state.registers[*return_reg] {
                        if pc < 0 {
                            return Err(LimboError::InternalError(
                                "Return register is negative".to_string(),
                            ));
                        }
                        state.pc = pc;
                    } else {
                        return Err(LimboError::InternalError(
                            "Return register is not an integer".to_string(),
                        ));
                    }
                }
                Insn::Integer { value, dest } => {
                    state.registers[*dest] = OwnedValue::Integer(*value);
                    state.pc += 1;
                }
                Insn::Real { value, dest } => {
                    state.registers[*dest] = OwnedValue::Float(*value);
                    state.pc += 1;
                }
                Insn::RealAffinity { register } => {
                    if let OwnedValue::Integer(i) = &state.registers[*register] {
                        state.registers[*register] = OwnedValue::Float(*i as f64);
                    };
                    state.pc += 1;
                }
                Insn::String8 { value, dest } => {
                    state.registers[*dest] = OwnedValue::Text(Rc::new(value.into()));
                    state.pc += 1;
                }
                Insn::Blob { value, dest } => {
                    state.registers[*dest] = OwnedValue::Blob(Rc::new(value.clone()));
                    state.pc += 1;
                }
                Insn::RowId { cursor_id, dest } => {
                    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seek.take() {
                        let index_cursor = cursors.get_mut(&index_cursor_id).unwrap();
                        let rowid = index_cursor.rowid()?;
                        let table_cursor = cursors.get_mut(&table_cursor_id).unwrap();
                        match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                            CursorResult::Ok(_) => {}
                            CursorResult::IO => {
                                state.deferred_seek = Some((index_cursor_id, table_cursor_id));
                                return Ok(StepResult::IO);
                            }
                        }
                    }

                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    if let Some(ref rowid) = cursor.rowid()? {
                        state.registers[*dest] = OwnedValue::Integer(*rowid as i64);
                    } else {
                        state.registers[*dest] = OwnedValue::Null;
                    }
                    state.pc += 1;
                }
                Insn::SeekRowid {
                    cursor_id,
                    src_reg,
                    target_pc,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let rowid = match &state.registers[*src_reg] {
                        OwnedValue::Integer(rowid) => *rowid as u64,
                        OwnedValue::Null => {
                            state.pc = *target_pc;
                            continue;
                        }
                        other => {
                            return Err(LimboError::InternalError(
                                format!("SeekRowid: the value in the register is not an integer or NULL: {}", other)
                            ));
                        }
                    };
                    let found = return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::EQ));
                    if !found {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::DeferredSeek {
                    index_cursor_id,
                    table_cursor_id,
                } => {
                    state.deferred_seek = Some((*index_cursor_id, *table_cursor_id));
                    state.pc += 1;
                }
                Insn::SeekGE {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                    is_index,
                } => {
                    if *is_index {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let record_from_regs: OwnedRecord =
                            make_owned_record(&state.registers, start_reg, num_regs);
                        let found = return_if_io!(
                            cursor.seek(SeekKey::IndexKey(&record_from_regs), SeekOp::GE)
                        );
                        if !found {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let rowid = match &state.registers[*start_reg] {
                            OwnedValue::Null => {
                                // All integer values are greater than null so we just rewind the cursor
                                return_if_io!(cursor.rewind());
                                state.pc += 1;
                                continue;
                            }
                            OwnedValue::Integer(rowid) => *rowid as u64,
                            _ => {
                                return Err(LimboError::InternalError(
                                    "SeekGE: the value in the register is not an integer".into(),
                                ));
                            }
                        };
                        let found =
                            return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE));
                        if !found {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    }
                }
                Insn::SeekGT {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                    is_index,
                } => {
                    if *is_index {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let record_from_regs: OwnedRecord =
                            make_owned_record(&state.registers, start_reg, num_regs);
                        let found = return_if_io!(
                            cursor.seek(SeekKey::IndexKey(&record_from_regs), SeekOp::GT)
                        );
                        if !found {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let rowid = match &state.registers[*start_reg] {
                            OwnedValue::Null => {
                                // All integer values are greater than null so we just rewind the cursor
                                return_if_io!(cursor.rewind());
                                state.pc += 1;
                                continue;
                            }
                            OwnedValue::Integer(rowid) => *rowid as u64,
                            _ => {
                                return Err(LimboError::InternalError(
                                    "SeekGT: the value in the register is not an integer".into(),
                                ));
                            }
                        };
                        let found =
                            return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GT));
                        if !found {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    }
                }
                Insn::IdxGE {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record_from_regs: OwnedRecord =
                        make_owned_record(&state.registers, start_reg, num_regs);
                    if let Some(ref idx_record) = *cursor.record()? {
                        // omit the rowid from the idx_record, which is the last value
                        if idx_record.values[..idx_record.values.len() - 1]
                            >= *record_from_regs.values
                        {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        state.pc = *target_pc;
                    }
                }
                Insn::IdxGT {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record_from_regs: OwnedRecord =
                        make_owned_record(&state.registers, start_reg, num_regs);
                    if let Some(ref idx_record) = *cursor.record()? {
                        // omit the rowid from the idx_record, which is the last value
                        if idx_record.values[..idx_record.values.len() - 1]
                            > *record_from_regs.values
                        {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        state.pc = *target_pc;
                    }
                }
                Insn::DecrJumpZero { reg, target_pc } => {
                    assert!(*target_pc >= 0);
                    match state.registers[*reg] {
                        OwnedValue::Integer(n) => {
                            let n = n - 1;
                            if n == 0 {
                                state.pc = *target_pc;
                            } else {
                                state.registers[*reg] = OwnedValue::Integer(n);
                                state.pc += 1;
                            }
                        }
                        _ => unreachable!("DecrJumpZero on non-integer register"),
                    }
                }
                Insn::AggStep {
                    acc_reg,
                    col,
                    delimiter,
                    func,
                } => {
                    if let OwnedValue::Null = &state.registers[*acc_reg] {
                        state.registers[*acc_reg] = match func {
                            AggFunc::Avg => OwnedValue::Agg(Box::new(AggContext::Avg(
                                OwnedValue::Float(0.0),
                                OwnedValue::Integer(0),
                            ))),
                            AggFunc::Sum => {
                                OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Null)))
                            }
                            AggFunc::Total => {
                                // The result of total() is always a floating point value.
                                // No overflow error is ever raised if any prior input was a floating point value.
                                // Total() never throws an integer overflow.
                                OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Float(0.0))))
                            }
                            AggFunc::Count => {
                                OwnedValue::Agg(Box::new(AggContext::Count(OwnedValue::Integer(0))))
                            }
                            AggFunc::Max => {
                                let col = state.registers[*col].clone();
                                match col {
                                    OwnedValue::Integer(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Max(None)))
                                    }
                                    OwnedValue::Float(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Max(None)))
                                    }
                                    OwnedValue::Text(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Max(None)))
                                    }
                                    _ => {
                                        unreachable!();
                                    }
                                }
                            }
                            AggFunc::Min => {
                                let col = state.registers[*col].clone();
                                match col {
                                    OwnedValue::Integer(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Min(None)))
                                    }
                                    OwnedValue::Float(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Min(None)))
                                    }
                                    OwnedValue::Text(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Min(None)))
                                    }
                                    _ => {
                                        unreachable!();
                                    }
                                }
                            }
                            AggFunc::GroupConcat | AggFunc::StringAgg => OwnedValue::Agg(Box::new(
                                AggContext::GroupConcat(OwnedValue::Text(Rc::new("".to_string()))),
                            )),
                        };
                    }
                    match func {
                        AggFunc::Avg => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *acc += col;
                            *count += 1;
                        }
                        AggFunc::Sum | AggFunc::Total => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Sum(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *acc += col;
                        }
                        AggFunc::Count => {
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Count(count) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *count += 1;
                        }
                        AggFunc::Max => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Max(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };

                            match (acc.as_mut(), col) {
                                (None, value) => {
                                    *acc = Some(value);
                                }
                                (
                                    Some(OwnedValue::Integer(ref mut current_max)),
                                    OwnedValue::Integer(value),
                                ) => {
                                    if value > *current_max {
                                        *current_max = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Float(ref mut current_max)),
                                    OwnedValue::Float(value),
                                ) => {
                                    if value > *current_max {
                                        *current_max = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Text(ref mut current_max)),
                                    OwnedValue::Text(value),
                                ) => {
                                    if value > *current_max {
                                        *current_max = value;
                                    }
                                }
                                _ => {
                                    eprintln!("Unexpected types in max aggregation");
                                }
                            }
                        }
                        AggFunc::Min => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Min(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };

                            match (acc.as_mut(), col) {
                                (None, value) => {
                                    *acc.borrow_mut() = Some(value);
                                }
                                (
                                    Some(OwnedValue::Integer(ref mut current_min)),
                                    OwnedValue::Integer(value),
                                ) => {
                                    if value < *current_min {
                                        *current_min = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Float(ref mut current_min)),
                                    OwnedValue::Float(value),
                                ) => {
                                    if value < *current_min {
                                        *current_min = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Text(ref mut current_min)),
                                    OwnedValue::Text(value),
                                ) => {
                                    if value < *current_min {
                                        *current_min = value;
                                    }
                                }
                                _ => {
                                    eprintln!("Unexpected types in min aggregation");
                                }
                            }
                        }
                        AggFunc::GroupConcat | AggFunc::StringAgg => {
                            let col = state.registers[*col].clone();
                            let delimiter = state.registers[*delimiter].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            if acc.to_string().is_empty() {
                                *acc = col;
                            } else {
                                *acc += delimiter;
                                *acc += col;
                            }
                        }
                    };
                    state.pc += 1;
                }
                Insn::AggFinal { register, func } => {
                    match state.registers[*register].borrow_mut() {
                        OwnedValue::Agg(agg) => {
                            match func {
                                AggFunc::Avg => {
                                    let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                                        unreachable!();
                                    };
                                    *acc /= count.clone();
                                }
                                AggFunc::Sum | AggFunc::Total => {}
                                AggFunc::Count => {}
                                AggFunc::Max => {}
                                AggFunc::Min => {}
                                AggFunc::GroupConcat | AggFunc::StringAgg => {}
                            };
                        }
                        OwnedValue::Null => {
                            // when the set is empty
                            match func {
                                AggFunc::Total => {
                                    state.registers[*register] = OwnedValue::Float(0.0);
                                }
                                AggFunc::Count => {
                                    state.registers[*register] = OwnedValue::Integer(0);
                                }
                                _ => {}
                            }
                        }
                        _ => {
                            unreachable!();
                        }
                    };
                    state.pc += 1;
                }
                Insn::SorterOpen {
                    cursor_id,
                    columns: _,
                    order,
                } => {
                    let order = order
                        .values
                        .iter()
                        .map(|v| match v {
                            OwnedValue::Integer(i) => *i == 0,
                            _ => unreachable!(),
                        })
                        .collect();
                    let cursor = Box::new(sorter::Sorter::new(order));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::SorterData {
                    cursor_id,
                    dest_reg,
                    pseudo_cursor: sorter_cursor,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record = match *cursor.record()? {
                        Some(ref record) => record.clone(),
                        None => {
                            state.pc += 1;
                            continue;
                        }
                    };
                    state.registers[*dest_reg] = OwnedValue::Record(record.clone());
                    let sorter_cursor = cursors.get_mut(sorter_cursor).unwrap();
                    sorter_cursor.insert(&OwnedValue::Integer(0), &record, false)?; // fix key later
                    state.pc += 1;
                }
                Insn::SorterInsert {
                    cursor_id,
                    record_reg,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record = match &state.registers[*record_reg] {
                        OwnedValue::Record(record) => record,
                        _ => unreachable!("SorterInsert on non-record register"),
                    };
                    // TODO: set correct key
                    cursor.insert(&OwnedValue::Integer(0), record, false)?;
                    state.pc += 1;
                }
                Insn::SorterSort {
                    cursor_id,
                    pc_if_empty,
                } => {
                    if let Some(cursor) = cursors.get_mut(cursor_id) {
                        cursor.rewind()?;
                        state.pc += 1;
                    } else {
                        state.pc = *pc_if_empty;
                    }
                }
                Insn::SorterNext {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(*pc_if_next >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    return_if_io!(cursor.next());
                    if !cursor.is_empty() {
                        state.pc = *pc_if_next;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Function {
                    constant_mask,
                    func,
                    start_reg,
                    dest,
                } => {
                    let arg_count = func.arg_count;
                    match &func.func {
                        #[cfg(feature = "json")]
                        crate::function::Func::Json(JsonFunc::Json) => {
                            let json_value = &state.registers[*start_reg];
                            let json_str = get_json(json_value);
                            match json_str {
                                Ok(json) => state.registers[*dest] = json,
                                Err(e) => return Err(e),
                            }
                        }
                        crate::function::Func::Scalar(scalar_func) => match scalar_func {
                            ScalarFunc::Cast => {
                                assert!(arg_count == 2);
                                assert!(*start_reg + 1 < state.registers.len());
                                let reg_value_argument = state.registers[*start_reg].clone();
                                let OwnedValue::Text(reg_value_type) =
                                    state.registers[*start_reg + 1].clone()
                                else {
                                    unreachable!("Cast with non-text type");
                                };
                                let result = exec_cast(&reg_value_argument, &reg_value_type);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Char => {
                                let reg_values =
                                    state.registers[*start_reg..*start_reg + arg_count].to_vec();
                                state.registers[*dest] = exec_char(reg_values);
                            }
                            ScalarFunc::Coalesce => {}
                            ScalarFunc::Concat => {
                                let result = exec_concat(
                                    &state.registers[*start_reg..*start_reg + arg_count],
                                );
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::ConcatWs => {
                                let result = exec_concat_ws(
                                    &state.registers[*start_reg..*start_reg + arg_count],
                                );
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Glob => {
                                let pattern = &state.registers[*start_reg];
                                let text = &state.registers[*start_reg + 1];
                                let result = match (pattern, text) {
                                    (OwnedValue::Text(pattern), OwnedValue::Text(text)) => {
                                        let cache = if *constant_mask > 0 {
                                            Some(&mut state.regex_cache.glob)
                                        } else {
                                            None
                                        };
                                        OwnedValue::Integer(exec_glob(cache, pattern, text) as i64)
                                    }
                                    _ => {
                                        unreachable!("Like on non-text registers");
                                    }
                                };
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::IfNull => {}
                            ScalarFunc::Iif => {}
                            ScalarFunc::Instr => {
                                let reg_value = &state.registers[*start_reg];
                                let pattern_value = &state.registers[*start_reg + 1];
                                let result = exec_instr(reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::LastInsertRowid => {
                                if let Some(conn) = self.connection.upgrade() {
                                    state.registers[*dest] =
                                        OwnedValue::Integer(conn.last_insert_rowid() as i64);
                                } else {
                                    state.registers[*dest] = OwnedValue::Null;
                                }
                            }
                            ScalarFunc::Like => {
                                let pattern = &state.registers[*start_reg];
                                let text = &state.registers[*start_reg + 1];
                                let result = match (pattern, text) {
                                    (OwnedValue::Text(pattern), OwnedValue::Text(text)) => {
                                        let cache = if *constant_mask > 0 {
                                            Some(&mut state.regex_cache.like)
                                        } else {
                                            None
                                        };
                                        OwnedValue::Integer(exec_like(cache, pattern, text) as i64)
                                    }
                                    _ => {
                                        unreachable!("Like on non-text registers");
                                    }
                                };
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Abs
                            | ScalarFunc::Lower
                            | ScalarFunc::Upper
                            | ScalarFunc::Length
                            | ScalarFunc::OctetLength
                            | ScalarFunc::Typeof
                            | ScalarFunc::Unicode
                            | ScalarFunc::Quote
                            | ScalarFunc::RandomBlob
                            | ScalarFunc::Sign
                            | ScalarFunc::Soundex
                            | ScalarFunc::ZeroBlob => {
                                let reg_value = state.registers[*start_reg].borrow_mut();
                                let result = match scalar_func {
                                    ScalarFunc::Sign => exec_sign(reg_value),
                                    ScalarFunc::Abs => exec_abs(reg_value),
                                    ScalarFunc::Lower => exec_lower(reg_value),
                                    ScalarFunc::Upper => exec_upper(reg_value),
                                    ScalarFunc::Length => Some(exec_length(reg_value)),
                                    ScalarFunc::OctetLength => Some(exec_octet_length(reg_value)),
                                    ScalarFunc::Typeof => Some(exec_typeof(reg_value)),
                                    ScalarFunc::Unicode => Some(exec_unicode(reg_value)),
                                    ScalarFunc::Quote => Some(exec_quote(reg_value)),
                                    ScalarFunc::RandomBlob => Some(exec_randomblob(reg_value)),
                                    ScalarFunc::ZeroBlob => Some(exec_zeroblob(reg_value)),
                                    ScalarFunc::Soundex => Some(exec_soundex(reg_value)),
                                    _ => unreachable!(),
                                };
                                state.registers[*dest] = result.unwrap_or(OwnedValue::Null);
                            }
                            ScalarFunc::Hex => {
                                let reg_value = state.registers[*start_reg].borrow_mut();
                                let result = exec_hex(reg_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Unhex => {
                                let reg_value = state.registers[*start_reg].clone();
                                let ignored_chars = state.registers.get(*start_reg + 1);
                                let result = exec_unhex(&reg_value, ignored_chars);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Random => {
                                state.registers[*dest] = exec_random();
                            }
                            ScalarFunc::Trim => {
                                let reg_value = state.registers[*start_reg].clone();
                                let pattern_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_trim(&reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::LTrim => {
                                let reg_value = state.registers[*start_reg].clone();
                                let pattern_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_ltrim(&reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::RTrim => {
                                let reg_value = state.registers[*start_reg].clone();
                                let pattern_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_rtrim(&reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Round => {
                                let reg_value = state.registers[*start_reg].clone();
                                assert!(arg_count == 1 || arg_count == 2);
                                let precision_value = if arg_count > 1 {
                                    Some(state.registers[*start_reg + 1].clone())
                                } else {
                                    None
                                };
                                let result = exec_round(&reg_value, precision_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Min => {
                                let reg_values = state.registers
                                    [*start_reg..*start_reg + arg_count]
                                    .iter()
                                    .collect();
                                state.registers[*dest] = exec_min(reg_values);
                            }
                            ScalarFunc::Max => {
                                let reg_values = state.registers
                                    [*start_reg..*start_reg + arg_count]
                                    .iter()
                                    .collect();
                                state.registers[*dest] = exec_max(reg_values);
                            }
                            ScalarFunc::Nullif => {
                                let first_value = &state.registers[*start_reg];
                                let second_value = &state.registers[*start_reg + 1];
                                state.registers[*dest] = exec_nullif(first_value, second_value);
                            }
                            ScalarFunc::Substr | ScalarFunc::Substring => {
                                let str_value = &state.registers[*start_reg];
                                let start_value = &state.registers[*start_reg + 1];
                                let length_value = &state.registers[*start_reg + 2];
                                let result = exec_substring(str_value, start_value, length_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Date => {
                                let result =
                                    exec_date(&state.registers[*start_reg..*start_reg + arg_count]);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Time => {
                                let result =
                                    exec_time(&state.registers[*start_reg..*start_reg + arg_count]);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::UnixEpoch => {
                                if *start_reg == 0 {
                                    let unixepoch: String = exec_unixepoch(&OwnedValue::Text(
                                        Rc::new("now".to_string()),
                                    ))?;
                                    state.registers[*dest] = OwnedValue::Text(Rc::new(unixepoch));
                                } else {
                                    let datetime_value = &state.registers[*start_reg];
                                    let unixepoch = exec_unixepoch(datetime_value);
                                    match unixepoch {
                                        Ok(time) => {
                                            state.registers[*dest] = OwnedValue::Text(Rc::new(time))
                                        }
                                        Err(e) => {
                                            return Err(LimboError::ParseError(format!(
                                                "Error encountered while parsing datetime value: {}",
                                                e
                                            )));
                                        }
                                    }
                                }
                            }
                            ScalarFunc::SqliteVersion => {
                                let version_integer: i64 =
                                    DATABASE_VERSION.get().unwrap().parse()?;
                                let version = execute_sqlite_version(version_integer);
                                state.registers[*dest] = OwnedValue::Text(Rc::new(version));
                            }
                            ScalarFunc::Replace => {
                                assert!(arg_count == 3);
                                let source = &state.registers[*start_reg];
                                let pattern = &state.registers[*start_reg + 1];
                                let replacement = &state.registers[*start_reg + 2];
                                state.registers[*dest] = exec_replace(source, pattern, replacement);
                            }
                        },
                        crate::function::Func::Math(math_func) => match math_func.arity() {
                            MathFuncArity::Nullary => match math_func {
                                MathFunc::Pi => {
                                    state.registers[*dest] =
                                        OwnedValue::Float(std::f64::consts::PI);
                                }
                                _ => {
                                    unreachable!(
                                        "Unexpected mathematical Nullary function {:?}",
                                        math_func
                                    );
                                }
                            },

                            MathFuncArity::Unary => {
                                let reg_value = &state.registers[*start_reg];
                                let result = exec_math_unary(reg_value, math_func);
                                state.registers[*dest] = result;
                            }

                            MathFuncArity::Binary => {
                                let lhs = &state.registers[*start_reg];
                                let rhs = &state.registers[*start_reg + 1];
                                let result = exec_math_binary(lhs, rhs, math_func);
                                state.registers[*dest] = result;
                            }

                            MathFuncArity::UnaryOrBinary => match math_func {
                                MathFunc::Log => {
                                    let result = match arg_count {
                                        1 => {
                                            let arg = &state.registers[*start_reg];
                                            exec_math_log(arg, None)
                                        }
                                        2 => {
                                            let base = &state.registers[*start_reg];
                                            let arg = &state.registers[*start_reg + 1];
                                            exec_math_log(arg, Some(base))
                                        }
                                        _ => unreachable!(
                                            "{:?} function with unexpected number of arguments",
                                            math_func
                                        ),
                                    };
                                    state.registers[*dest] = result;
                                }
                                _ => unreachable!(
                                    "Unexpected mathematical UnaryOrBinary function {:?}",
                                    math_func
                                ),
                            },
                        },
                        crate::function::Func::Agg(_) => {
                            unreachable!("Aggregate functions should not be handled here")
                        }
                    }
                    state.pc += 1;
                }
                Insn::InitCoroutine {
                    yield_reg,
                    jump_on_definition,
                    start_offset,
                } => {
                    state.registers[*yield_reg] = OwnedValue::Integer(*start_offset);
                    state.pc = *jump_on_definition;
                }
                Insn::EndCoroutine { yield_reg } => {
                    if let OwnedValue::Integer(pc) = state.registers[*yield_reg] {
                        state.ended_coroutine = true;
                        state.pc = pc - 1; // yield jump is always next to yield. Here we substract 1 to go back to yield instruction
                    } else {
                        unreachable!();
                    }
                }
                Insn::Yield {
                    yield_reg,
                    end_offset,
                } => {
                    if let OwnedValue::Integer(pc) = state.registers[*yield_reg] {
                        if state.ended_coroutine {
                            state.pc = *end_offset;
                        } else {
                            // swap
                            (state.pc, state.registers[*yield_reg]) =
                                (pc, OwnedValue::Integer(state.pc + 1));
                        }
                    } else {
                        unreachable!();
                    }
                }
                Insn::InsertAsync {
                    cursor,
                    key_reg,
                    record_reg,
                    flag: _,
                } => {
                    let cursor = cursors.get_mut(cursor).unwrap();
                    let record = match &state.registers[*record_reg] {
                        OwnedValue::Record(r) => r,
                        _ => unreachable!("Not a record! Cannot insert a non record value."),
                    };
                    let key = &state.registers[*key_reg];
                    return_if_io!(cursor.insert(key, record, true));
                    state.pc += 1;
                }
                Insn::InsertAwait { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    // Only update last_insert_rowid for regular table inserts, not schema modifications
                    if cursor.root_page() != 1 {
                        if let Some(rowid) = cursor.rowid()? {
                            if let Some(conn) = self.connection.upgrade() {
                                conn.update_last_rowid(rowid);
                            }
                        }
                    }
                    state.pc += 1;
                }
                Insn::NewRowid {
                    cursor, rowid_reg, ..
                } => {
                    let cursor = cursors.get_mut(cursor).unwrap();
                    // TODO: make io handle rng
                    let rowid = return_if_io!(get_new_rowid(cursor, thread_rng()));
                    state.registers[*rowid_reg] = OwnedValue::Integer(rowid);
                    state.pc += 1;
                }
                Insn::MustBeInt { reg } => {
                    match state.registers[*reg] {
                        OwnedValue::Integer(_) => {}
                        _ => {
                            crate::bail_parse_error!(
                                "MustBeInt: the value in the register is not an integer"
                            );
                        }
                    };
                    state.pc += 1;
                }
                Insn::SoftNull { reg } => {
                    state.registers[*reg] = OwnedValue::Null;
                    state.pc += 1;
                }
                Insn::NotExists {
                    cursor,
                    rowid_reg,
                    target_pc,
                } => {
                    let cursor = cursors.get_mut(cursor).unwrap();
                    let exists = return_if_io!(cursor.exists(&state.registers[*rowid_reg]));
                    if exists {
                        state.pc += 1;
                    } else {
                        state.pc = *target_pc;
                    }
                }
                // this cursor may be reused for next insert
                // Update: tablemoveto is used to travers on not exists, on insert depending on flags if nonseek it traverses again.
                // If not there might be some optimizations obviously.
                Insn::OpenWriteAsync {
                    cursor_id,
                    root_page,
                } => {
                    let cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        *root_page,
                        self.database_header.clone(),
                    ));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::OpenWriteAwait {} => {
                    state.pc += 1;
                }
                Insn::Copy {
                    src_reg,
                    dst_reg,
                    amount,
                } => {
                    for i in 0..=*amount {
                        state.registers[*dst_reg + i] = state.registers[*src_reg + i].clone();
                    }
                    state.pc += 1;
                }
                Insn::CreateBtree { db, root, flags: _ } => {
                    if *db > 0 {
                        // TODO: implement temp datbases
                        todo!("temp databases not implemented yet");
                    }
                    let mut cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        0,
                        self.database_header.clone(),
                    ));

                    let root_page = cursor.btree_create(1);
                    state.registers[*root] = OwnedValue::Integer(root_page as i64);
                    state.pc += 1;
                }
                Insn::Close { cursor_id } => {
                    cursors.remove(cursor_id);
                    state.pc += 1;
                }
                Insn::IsNull { src, target_pc } => {
                    if matches!(state.registers[*src], OwnedValue::Null) {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::ParseSchema {
                    db: _,
                    where_clause,
                } => {
                    let conn = self.connection.upgrade();
                    let conn = conn.as_ref().unwrap();
                    let stmt = conn.prepare(format!(
                        "SELECT * FROM  sqlite_schema WHERE {}",
                        where_clause
                    ))?;
                    let rows = Rows { stmt };
                    let mut schema = RefCell::borrow_mut(&conn.schema);
                    // TODO: This function below is synchronous, make it not async
                    parse_schema_rows(Some(rows), &mut schema, conn.pager.io.clone())?;
                    state.pc += 1;
                }
            }
        }
    }
}

fn get_new_rowid<R: Rng>(cursor: &mut Box<dyn Cursor>, mut rng: R) -> Result<CursorResult<i64>> {
    match cursor.seek_to_last()? {
        CursorResult::Ok(()) => {}
        CursorResult::IO => return Ok(CursorResult::IO),
    }
    let mut rowid = cursor.rowid()?.unwrap_or(0) + 1;
    if rowid > i64::MAX.try_into().unwrap() {
        let distribution = Uniform::from(1..=i64::MAX);
        let max_attempts = 100;
        for count in 0..max_attempts {
            rowid = distribution.sample(&mut rng).try_into().unwrap();
            match cursor.seek(SeekKey::TableRowId(rowid), SeekOp::EQ)? {
                CursorResult::Ok(false) => break, // Found a non-existing rowid
                CursorResult::Ok(true) => {
                    if count == max_attempts - 1 {
                        return Err(LimboError::InternalError(
                            "Failed to generate a new rowid".to_string(),
                        ));
                    } else {
                        continue; // Try next random rowid
                    }
                }
                CursorResult::IO => return Ok(CursorResult::IO),
            }
        }
    }
    Ok(CursorResult::Ok(rowid.try_into().unwrap()))
}

fn make_record<'a>(registers: &'a [OwnedValue], start_reg: &usize, count: &usize) -> Record<'a> {
    let mut values = Vec::with_capacity(*count);
    for r in registers.iter().skip(*start_reg).take(*count) {
        values.push(crate::types::to_value(r))
    }
    Record::new(values)
}

fn make_owned_record(registers: &[OwnedValue], start_reg: &usize, count: &usize) -> OwnedRecord {
    let mut values = Vec::with_capacity(*count);
    for r in registers.iter().skip(*start_reg).take(*count) {
        values.push(r.clone())
    }
    OwnedRecord::new(values)
}

fn trace_insn(program: &Program, addr: InsnReference, insn: &Insn) {
    if !log::log_enabled!(log::Level::Trace) {
        return;
    }
    log::trace!(
        "{}",
        explain::insn_to_str(
            program,
            addr,
            insn,
            String::new(),
            program.comments.get(&(addr as BranchOffset)).copied()
        )
    );
}

fn print_insn(program: &Program, addr: InsnReference, insn: &Insn, indent: String) {
    let s = explain::insn_to_str(
        program,
        addr,
        insn,
        indent,
        program.comments.get(&(addr as BranchOffset)).copied(),
    );
    println!("{}", s);
}

fn get_indent_count(indent_count: usize, curr_insn: &Insn, prev_insn: Option<&Insn>) -> usize {
    let indent_count = if let Some(insn) = prev_insn {
        match insn {
            Insn::RewindAwait { .. }
            | Insn::LastAwait { .. }
            | Insn::SorterSort { .. }
            | Insn::SeekGE { .. }
            | Insn::SeekGT { .. } => indent_count + 1,
            _ => indent_count,
        }
    } else {
        indent_count
    };

    match curr_insn {
        Insn::NextAsync { .. } | Insn::SorterNext { .. } | Insn::PrevAsync { .. } => {
            indent_count - 1
        }
        _ => indent_count,
    }
}

fn exec_lower(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::Text(Rc::new(t.to_lowercase()))),
        t => Some(t.to_owned()),
    }
}

fn exec_length(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
            OwnedValue::Integer(reg.to_string().chars().count() as i64)
        }
        OwnedValue::Blob(blob) => OwnedValue::Integer(blob.len() as i64),
        OwnedValue::Agg(aggctx) => exec_length(aggctx.final_value()),
        _ => reg.to_owned(),
    }
}

fn exec_octet_length(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
            OwnedValue::Integer(reg.to_string().into_bytes().len() as i64)
        }
        OwnedValue::Blob(blob) => OwnedValue::Integer(blob.len() as i64),
        OwnedValue::Agg(aggctx) => exec_octet_length(aggctx.final_value()),
        _ => reg.to_owned(),
    }
}

fn exec_upper(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::Text(Rc::new(t.to_uppercase()))),
        t => Some(t.to_owned()),
    }
}

fn exec_concat(registers: &[OwnedValue]) -> OwnedValue {
    let mut result = String::new();
    for reg in registers {
        match reg {
            OwnedValue::Text(text) => result.push_str(text),
            OwnedValue::Integer(i) => result.push_str(&i.to_string()),
            OwnedValue::Float(f) => result.push_str(&f.to_string()),
            OwnedValue::Agg(aggctx) => result.push_str(&aggctx.final_value().to_string()),
            OwnedValue::Null => continue,
            OwnedValue::Blob(_) => todo!("TODO concat blob"),
            OwnedValue::Record(_) => unreachable!(),
        }
    }
    OwnedValue::Text(Rc::new(result))
}

fn exec_concat_ws(registers: &[OwnedValue]) -> OwnedValue {
    if registers.is_empty() {
        return OwnedValue::Null;
    }

    let separator = match &registers[0] {
        OwnedValue::Text(text) => text.clone(),
        OwnedValue::Integer(i) => Rc::new(i.to_string()),
        OwnedValue::Float(f) => Rc::new(f.to_string()),
        _ => return OwnedValue::Null,
    };

    let mut result = String::new();
    for (i, reg) in registers.iter().enumerate().skip(1) {
        if i > 1 {
            result.push_str(&separator);
        }
        match reg {
            OwnedValue::Text(text) => result.push_str(text),
            OwnedValue::Integer(i) => result.push_str(&i.to_string()),
            OwnedValue::Float(f) => result.push_str(&f.to_string()),
            _ => continue,
        }
    }

    OwnedValue::Text(Rc::new(result))
}

fn exec_sign(reg: &OwnedValue) -> Option<OwnedValue> {
    let num = match reg {
        OwnedValue::Integer(i) => *i as f64,
        OwnedValue::Float(f) => *f,
        OwnedValue::Text(s) => {
            if let Ok(i) = s.parse::<i64>() {
                i as f64
            } else if let Ok(f) = s.parse::<f64>() {
                f
            } else {
                return Some(OwnedValue::Null);
            }
        }
        OwnedValue::Blob(b) => match std::str::from_utf8(b) {
            Ok(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    i as f64
                } else if let Ok(f) = s.parse::<f64>() {
                    f
                } else {
                    return Some(OwnedValue::Null);
                }
            }
            Err(_) => return Some(OwnedValue::Null),
        },
        _ => return Some(OwnedValue::Null),
    };

    let sign = if num > 0.0 {
        1
    } else if num < 0.0 {
        -1
    } else {
        0
    };

    Some(OwnedValue::Integer(sign))
}

/// Generates the Soundex code for a given word
pub fn exec_soundex(reg: &OwnedValue) -> OwnedValue {
    let s = match reg {
        OwnedValue::Null => return OwnedValue::Text(Rc::new("?000".to_string())),
        OwnedValue::Text(s) => {
            // return ?000 if non ASCII alphabet character is found
            if !s.chars().all(|c| c.is_ascii_alphabetic()) {
                return OwnedValue::Text(Rc::new("?000".to_string()));
            }
            s.clone()
        }
        _ => return OwnedValue::Text(Rc::new("?000".to_string())), // For unsupported types, return NULL
    };

    // Remove numbers and spaces
    let word: String = s
        .chars()
        .filter(|c| !c.is_digit(10))
        .collect::<String>()
        .replace(" ", "");
    if word.is_empty() {
        return OwnedValue::Text(Rc::new("0000".to_string()));
    }

    let soundex_code = |c| match c {
        'b' | 'f' | 'p' | 'v' => Some('1'),
        'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
        'd' | 't' => Some('3'),
        'l' => Some('4'),
        'm' | 'n' => Some('5'),
        'r' => Some('6'),
        _ => None,
    };

    // Convert the word to lowercase for consistent lookups
    let word = word.to_lowercase();
    let first_letter = word.chars().next().unwrap();

    // Remove all occurrences of 'h' and 'w' except the first letter
    let code: String = word
        .chars()
        .skip(1)
        .filter(|&ch| ch != 'h' && ch != 'w')
        .fold(first_letter.to_string(), |mut acc, ch| {
            acc.push(ch);
            acc
        });

    // Replace consonants with digits based on Soundex mapping
    let tmp: String = code
        .chars()
        .map(|ch| match soundex_code(ch) {
            Some(code) => code.to_string(),
            None => ch.to_string(),
        })
        .collect();

    // Remove adjacent same digits
    let tmp = tmp.chars().fold(String::new(), |mut acc, ch| {
        if acc.chars().last() != Some(ch) {
            acc.push(ch);
        }
        acc
    });

    // Remove all occurrences of a, e, i, o, u, y except the first letter
    let mut result = tmp
        .chars()
        .enumerate()
        .filter(|(i, ch)| *i == 0 || !matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u' | 'y'))
        .map(|(_, ch)| ch)
        .collect::<String>();

    // If the first symbol is a digit, replace it with the saved first letter
    if let Some(first_digit) = result.chars().next() {
        if first_digit.is_digit(10) {
            result.replace_range(0..1, &first_letter.to_string());
        }
    }

    // Append zeros if the result contains less than 4 characters
    while result.len() < 4 {
        result.push('0');
    }

    // Retain the first 4 characters and convert to uppercase
    result.truncate(4);
    OwnedValue::Text(Rc::new(result.to_uppercase()))
}

fn exec_abs(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Integer(x) => {
            if x < &0 {
                Some(OwnedValue::Integer(-x))
            } else {
                Some(OwnedValue::Integer(*x))
            }
        }
        OwnedValue::Float(x) => {
            if x < &0.0 {
                Some(OwnedValue::Float(-x))
            } else {
                Some(OwnedValue::Float(*x))
            }
        }
        OwnedValue::Null => Some(OwnedValue::Null),
        _ => Some(OwnedValue::Float(0.0)),
    }
}

fn exec_random() -> OwnedValue {
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf).unwrap();
    let random_number = i64::from_ne_bytes(buf);
    OwnedValue::Integer(random_number)
}

fn exec_randomblob(reg: &OwnedValue) -> OwnedValue {
    let length = match reg {
        OwnedValue::Integer(i) => *i,
        OwnedValue::Float(f) => *f as i64,
        OwnedValue::Text(t) => t.parse().unwrap_or(1),
        _ => 1,
    }
    .max(1) as usize;

    let mut blob: Vec<u8> = vec![0; length];
    getrandom::getrandom(&mut blob).expect("Failed to generate random blob");
    OwnedValue::Blob(Rc::new(blob))
}

fn exec_quote(value: &OwnedValue) -> OwnedValue {
    match value {
        OwnedValue::Null => OwnedValue::Text(OwnedValue::Null.to_string().into()),
        OwnedValue::Integer(_) | OwnedValue::Float(_) => value.to_owned(),
        OwnedValue::Blob(_) => todo!(),
        OwnedValue::Text(s) => {
            let mut quoted = String::with_capacity(s.len() + 2);
            quoted.push('\'');
            for c in s.chars() {
                if c == '\0' {
                    break;
                } else {
                    quoted.push(c);
                }
            }
            quoted.push('\'');
            OwnedValue::Text(Rc::new(quoted))
        }
        _ => OwnedValue::Null, // For unsupported types, return NULL
    }
}

fn exec_char(values: Vec<OwnedValue>) -> OwnedValue {
    let result: String = values
        .iter()
        .filter_map(|x| {
            if let OwnedValue::Integer(i) = x {
                Some(*i as u8 as char)
            } else {
                None
            }
        })
        .collect();
    OwnedValue::Text(Rc::new(result))
}

fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::from("(?i)^");
    regex_pattern.push_str(&pattern.replace('%', ".*").replace('_', "."));
    regex_pattern.push('$');
    Regex::new(&regex_pattern).unwrap()
}

// Implements LIKE pattern matching. Caches the constructed regex if a cache is provided
fn exec_like(regex_cache: Option<&mut HashMap<String, Regex>>, pattern: &str, text: &str) -> bool {
    if let Some(cache) = regex_cache {
        match cache.get(pattern) {
            Some(re) => re.is_match(text),
            None => {
                let re = construct_like_regex(pattern);
                let res = re.is_match(text);
                cache.insert(pattern.to_string(), re);
                res
            }
        }
    } else {
        let re = construct_like_regex(pattern);
        re.is_match(text)
    }
}

fn construct_glob_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::from("^");
    regex_pattern.push_str(&pattern.replace('*', ".*").replace("?", "."));
    regex_pattern.push('$');
    Regex::new(&regex_pattern).unwrap()
}

// Implements GLOB pattern matching. Caches the constructed regex if a cache is provided
fn exec_glob(regex_cache: Option<&mut HashMap<String, Regex>>, pattern: &str, text: &str) -> bool {
    if let Some(cache) = regex_cache {
        match cache.get(pattern) {
            Some(re) => re.is_match(text),
            None => {
                let re = construct_glob_regex(pattern);
                let res = re.is_match(text);
                cache.insert(pattern.to_string(), re);
                res
            }
        }
    } else {
        let re = construct_glob_regex(pattern);
        re.is_match(text)
    }
}

fn exec_min(regs: Vec<&OwnedValue>) -> OwnedValue {
    regs.iter()
        .min()
        .map(|&v| v.to_owned())
        .unwrap_or(OwnedValue::Null)
}

fn exec_max(regs: Vec<&OwnedValue>) -> OwnedValue {
    regs.iter()
        .max()
        .map(|&v| v.to_owned())
        .unwrap_or(OwnedValue::Null)
}

fn exec_nullif(first_value: &OwnedValue, second_value: &OwnedValue) -> OwnedValue {
    if first_value != second_value {
        first_value.clone()
    } else {
        OwnedValue::Null
    }
}

fn exec_substring(
    str_value: &OwnedValue,
    start_value: &OwnedValue,
    length_value: &OwnedValue,
) -> OwnedValue {
    if let (OwnedValue::Text(str), OwnedValue::Integer(start), OwnedValue::Integer(length)) =
        (str_value, start_value, length_value)
    {
        let start = *start as usize;
        if start > str.len() {
            return OwnedValue::Text(Rc::new("".to_string()));
        }

        let start_idx = start - 1;
        let str_len = str.len();
        let end = if *length != -1 {
            start_idx + *length as usize
        } else {
            str_len
        };
        let substring = &str[start_idx..end.min(str_len)];

        OwnedValue::Text(Rc::new(substring.to_string()))
    } else if let (OwnedValue::Text(str), OwnedValue::Integer(start)) = (str_value, start_value) {
        let start = *start as usize;
        if start > str.len() {
            return OwnedValue::Text(Rc::new("".to_string()));
        }

        let start_idx = start - 1;
        let str_len = str.len();
        let substring = &str[start_idx..str_len];

        OwnedValue::Text(Rc::new(substring.to_string()))
    } else {
        OwnedValue::Null
    }
}

fn exec_instr(reg: &OwnedValue, pattern: &OwnedValue) -> OwnedValue {
    if reg == &OwnedValue::Null || pattern == &OwnedValue::Null {
        return OwnedValue::Null;
    }

    if let (OwnedValue::Blob(reg), OwnedValue::Blob(pattern)) = (reg, pattern) {
        let result = reg
            .windows(pattern.len())
            .position(|window| window == **pattern)
            .map_or(0, |i| i + 1);
        return OwnedValue::Integer(result as i64);
    }

    let reg_str;
    let reg = match reg {
        OwnedValue::Text(s) => s.as_str(),
        _ => {
            reg_str = reg.to_string();
            reg_str.as_str()
        }
    };

    let pattern_str;
    let pattern = match pattern {
        OwnedValue::Text(s) => s.as_str(),
        _ => {
            pattern_str = pattern.to_string();
            pattern_str.as_str()
        }
    };

    match reg.find(pattern) {
        Some(position) => OwnedValue::Integer(position as i64 + 1),
        None => OwnedValue::Integer(0),
    }
}

fn exec_typeof(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Null => OwnedValue::Text(Rc::new("null".to_string())),
        OwnedValue::Integer(_) => OwnedValue::Text(Rc::new("integer".to_string())),
        OwnedValue::Float(_) => OwnedValue::Text(Rc::new("real".to_string())),
        OwnedValue::Text(_) => OwnedValue::Text(Rc::new("text".to_string())),
        OwnedValue::Blob(_) => OwnedValue::Text(Rc::new("blob".to_string())),
        OwnedValue::Agg(ctx) => exec_typeof(ctx.final_value()),
        OwnedValue::Record(_) => unimplemented!(),
    }
}

fn exec_hex(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_)
        | OwnedValue::Integer(_)
        | OwnedValue::Float(_)
        | OwnedValue::Blob(_) => {
            let text = reg.to_string();
            OwnedValue::Text(Rc::new(hex::encode_upper(text)))
        }
        _ => OwnedValue::Null,
    }
}

fn exec_unhex(reg: &OwnedValue, ignored_chars: Option<&OwnedValue>) -> OwnedValue {
    match reg {
        OwnedValue::Null => OwnedValue::Null,
        _ => match ignored_chars {
            None => match hex::decode(reg.to_string()) {
                Ok(bytes) => OwnedValue::Blob(Rc::new(bytes)),
                Err(_) => OwnedValue::Null,
            },
            Some(ignore) => match ignore {
                OwnedValue::Text(_) => {
                    let pat = ignore.to_string();
                    let trimmed = reg
                        .to_string()
                        .trim_start_matches(|x| pat.contains(x))
                        .trim_end_matches(|x| pat.contains(x))
                        .to_string();
                    match hex::decode(trimmed) {
                        Ok(bytes) => OwnedValue::Blob(Rc::new(bytes)),
                        Err(_) => OwnedValue::Null,
                    }
                }
                _ => OwnedValue::Null,
            },
        },
    }
}

fn exec_unicode(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_)
        | OwnedValue::Integer(_)
        | OwnedValue::Float(_)
        | OwnedValue::Blob(_) => {
            let text = reg.to_string();
            if let Some(first_char) = text.chars().next() {
                OwnedValue::Integer(first_char as u32 as i64)
            } else {
                OwnedValue::Null
            }
        }
        _ => OwnedValue::Null,
    }
}

fn _to_float(reg: &OwnedValue) -> f64 {
    match reg {
        OwnedValue::Text(x) => x.parse().unwrap_or(0.0),
        OwnedValue::Integer(x) => *x as f64,
        OwnedValue::Float(x) => *x,
        _ => 0.0,
    }
}

fn exec_round(reg: &OwnedValue, precision: Option<OwnedValue>) -> OwnedValue {
    let precision = match precision {
        Some(OwnedValue::Text(x)) => x.parse().unwrap_or(0.0),
        Some(OwnedValue::Integer(x)) => x as f64,
        Some(OwnedValue::Float(x)) => x,
        Some(OwnedValue::Null) => return OwnedValue::Null,
        _ => 0.0,
    };

    let reg = match reg {
        OwnedValue::Agg(ctx) => _to_float(ctx.final_value()),
        _ => _to_float(reg),
    };

    let precision = if precision < 1.0 { 0.0 } else { precision };
    let multiplier = 10f64.powi(precision as i32);
    OwnedValue::Float(((reg * multiplier).round()) / multiplier)
}

// Implements TRIM pattern matching.
fn exec_trim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::Text(Rc::new(
                    reg.to_string().trim_matches(&pattern_chars[..]).to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::Text(Rc::new(t.trim().to_string())),
        (reg, _) => reg.to_owned(),
    }
}

// Implements LTRIM pattern matching.
fn exec_ltrim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::Text(Rc::new(
                    reg.to_string()
                        .trim_start_matches(&pattern_chars[..])
                        .to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::Text(Rc::new(t.trim_start().to_string())),
        (reg, _) => reg.to_owned(),
    }
}

// Implements RTRIM pattern matching.
fn exec_rtrim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::Text(Rc::new(
                    reg.to_string()
                        .trim_end_matches(&pattern_chars[..])
                        .to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::Text(Rc::new(t.trim_end().to_string())),
        (reg, _) => reg.to_owned(),
    }
}

fn exec_zeroblob(req: &OwnedValue) -> OwnedValue {
    let length: i64 = match req {
        OwnedValue::Integer(i) => *i,
        OwnedValue::Float(f) => *f as i64,
        OwnedValue::Text(s) => s.parse().unwrap_or(0),
        _ => 0,
    };
    OwnedValue::Blob(Rc::new(vec![0; length.max(0) as usize]))
}

// exec_if returns whether you should jump
fn exec_if(reg: &OwnedValue, null_reg: &OwnedValue, not: bool) -> bool {
    match reg {
        OwnedValue::Integer(0) | OwnedValue::Float(0.0) => not,
        OwnedValue::Integer(_) | OwnedValue::Float(_) => !not,
        OwnedValue::Null => match null_reg {
            OwnedValue::Integer(0) | OwnedValue::Float(0.0) => false,
            OwnedValue::Integer(_) | OwnedValue::Float(_) => true,
            _ => false,
        },
        _ => false,
    }
}

fn exec_cast(value: &OwnedValue, datatype: &str) -> OwnedValue {
    if matches!(value, OwnedValue::Null) {
        return OwnedValue::Null;
    }
    match affinity(datatype) {
        // NONE	Casting a value to a type-name with no affinity causes the value to be converted into a BLOB. Casting to a BLOB consists of first casting the value to TEXT in the encoding of the database connection, then interpreting the resulting byte sequence as a BLOB instead of as TEXT.
        // Historically called NONE, but it's the same as BLOB
        Affinity::Blob => {
            // Convert to TEXT first, then interpret as BLOB
            // TODO: handle encoding
            let text = value.to_string();
            OwnedValue::Blob(Rc::new(text.into_bytes()))
        }
        // TEXT To cast a BLOB value to TEXT, the sequence of bytes that make up the BLOB is interpreted as text encoded using the database encoding.
        // Casting an INTEGER or REAL value into TEXT renders the value as if via sqlite3_snprintf() except that the resulting TEXT uses the encoding of the database connection.
        Affinity::Text => {
            // Convert everything to text representation
            // TODO: handle encoding and whatever sqlite3_snprintf does
            OwnedValue::Text(Rc::new(value.to_string()))
        }
        Affinity::Real => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_real(&text)
            }
            OwnedValue::Text(t) => cast_text_to_real(t),
            OwnedValue::Integer(i) => OwnedValue::Float(*i as f64),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => OwnedValue::Float(0.0),
        },
        Affinity::Integer => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_integer(&text)
            }
            OwnedValue::Text(t) => cast_text_to_integer(t),
            OwnedValue::Integer(i) => OwnedValue::Integer(*i),
            // A cast of a REAL value into an INTEGER results in the integer between the REAL value and zero
            // that is closest to the REAL value. If a REAL is greater than the greatest possible signed integer (+9223372036854775807)
            // then the result is the greatest possible signed integer and if the REAL is less than the least possible signed integer (-9223372036854775808)
            // then the result is the least possible signed integer.
            OwnedValue::Float(f) => {
                let i = f.floor() as i128;
                if i > i64::MAX as i128 {
                    OwnedValue::Integer(i64::MAX)
                } else if i < i64::MIN as i128 {
                    OwnedValue::Integer(i64::MIN)
                } else {
                    OwnedValue::Integer(i as i64)
                }
            }
            _ => OwnedValue::Integer(0),
        },
        Affinity::Numeric => match value {
            OwnedValue::Blob(b) => {
                let text = String::from_utf8_lossy(b);
                cast_text_to_numeric(&text)
            }
            OwnedValue::Text(t) => cast_text_to_numeric(t),
            OwnedValue::Integer(i) => OwnedValue::Integer(*i),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => value.clone(), // TODO probably wrong
        },
    }
}

fn exec_replace(source: &OwnedValue, pattern: &OwnedValue, replacement: &OwnedValue) -> OwnedValue {
    // The replace(X,Y,Z) function returns a string formed by substituting string Z for every occurrence of
    // string Y in string X. The BINARY collating sequence is used for comparisons. If Y is an empty string
    // then return X unchanged. If Z is not initially a string, it is cast to a UTF-8 string prior to processing.

    // If any of the arguments is NULL, the result is NULL.
    if matches!(source, OwnedValue::Null)
        || matches!(pattern, OwnedValue::Null)
        || matches!(replacement, OwnedValue::Null)
    {
        return OwnedValue::Null;
    }

    let source = exec_cast(source, "TEXT");
    let pattern = exec_cast(pattern, "TEXT");
    let replacement = exec_cast(replacement, "TEXT");

    // If any of the casts failed, panic as text casting is not expected to fail.
    match (&source, &pattern, &replacement) {
        (OwnedValue::Text(source), OwnedValue::Text(pattern), OwnedValue::Text(replacement)) => {
            if pattern.is_empty() {
                return OwnedValue::Text(source.clone());
            }

            let result = source.replace(pattern.as_str(), replacement);
            OwnedValue::Text(Rc::new(result))
        }
        _ => unreachable!("text cast should never fail"),
    }
}

enum Affinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

/// For tables not declared as STRICT, the affinity of a column is determined by the declared type of the column, according to the following rules in the order shown:
/// If the declared type contains the string "INT" then it is assigned INTEGER affinity.
/// If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
/// If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
/// If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
/// Otherwise, the affinity is NUMERIC.
/// Note that the order of the rules for determining column affinity is important. A column whose declared type is "CHARINT" will match both rules 1 and 2 but the first rule takes precedence and so the column affinity will be INTEGER.
fn affinity(datatype: &str) -> Affinity {
    // Note: callers of this function must ensure that the datatype is uppercase.
    // Rule 1: INT -> INTEGER affinity
    if datatype.contains("INT") {
        return Affinity::Integer;
    }

    // Rule 2: CHAR/CLOB/TEXT -> TEXT affinity
    if datatype.contains("CHAR") || datatype.contains("CLOB") || datatype.contains("TEXT") {
        return Affinity::Text;
    }

    // Rule 3: BLOB or empty -> BLOB affinity (historically called NONE)
    if datatype.contains("BLOB") || datatype.is_empty() {
        return Affinity::Blob;
    }

    // Rule 4: REAL/FLOA/DOUB -> REAL affinity
    if datatype.contains("REAL") || datatype.contains("FLOA") || datatype.contains("DOUB") {
        return Affinity::Real;
    }

    // Rule 5: Otherwise -> NUMERIC affinity
    Affinity::Numeric
}

/// When casting a TEXT value to INTEGER, the longest possible prefix of the value that can be interpreted as an integer number
/// is extracted from the TEXT value and the remainder ignored. Any leading spaces in the TEXT value when converting from TEXT to INTEGER are ignored.
/// If there is no prefix that can be interpreted as an integer number, the result of the conversion is 0.
/// If the prefix integer is greater than +9223372036854775807 then the result of the cast is exactly +9223372036854775807.
/// Similarly, if the prefix integer is less than -9223372036854775808 then the result of the cast is exactly -9223372036854775808.
/// When casting to INTEGER, if the text looks like a floating point value with an exponent, the exponent will be ignored
/// because it is no part of the integer prefix. For example, "CAST('123e+5' AS INTEGER)" results in 123, not in 12300000.
/// The CAST operator understands decimal integers only — conversion of hexadecimal integers stops at the "x" in the "0x" prefix of the hexadecimal integer string and thus result of the CAST is always zero.
fn cast_text_to_integer(text: &str) -> OwnedValue {
    let text = text.trim();
    if let Ok(i) = text.parse::<i64>() {
        return OwnedValue::Integer(i);
    }
    // Try to find longest valid prefix that parses as an integer
    // TODO: inefficient
    let mut end_index = text.len() - 1;
    while end_index > 0 {
        if let Ok(i) = text[..=end_index].parse::<i64>() {
            return OwnedValue::Integer(i);
        }
        end_index -= 1;
    }
    OwnedValue::Integer(0)
}

/// When casting a TEXT value to REAL, the longest possible prefix of the value that can be interpreted
/// as a real number is extracted from the TEXT value and the remainder ignored. Any leading spaces in
/// the TEXT value are ignored when converging from TEXT to REAL.
/// If there is no prefix that can be interpreted as a real number, the result of the conversion is 0.0.
fn cast_text_to_real(text: &str) -> OwnedValue {
    let trimmed = text.trim_start();
    if let Ok(num) = trimmed.parse::<f64>() {
        return OwnedValue::Float(num);
    }
    // Try to find longest valid prefix that parses as a float
    // TODO: inefficient
    let mut end_index = trimmed.len() - 1;
    while end_index > 0 {
        if let Ok(num) = trimmed[..=end_index].parse::<f64>() {
            return OwnedValue::Float(num);
        }
        end_index -= 1;
    }
    OwnedValue::Float(0.0)
}

/// NUMERIC Casting a TEXT or BLOB value into NUMERIC yields either an INTEGER or a REAL result.
/// If the input text looks like an integer (there is no decimal point nor exponent) and the value
/// is small enough to fit in a 64-bit signed integer, then the result will be INTEGER.
/// Input text that looks like floating point (there is a decimal point and/or an exponent)
/// and the text describes a value that can be losslessly converted back and forth between IEEE 754
/// 64-bit float and a 51-bit signed integer, then the result is INTEGER. (In the previous sentence,
/// a 51-bit integer is specified since that is one bit less than the length of the mantissa of an
/// IEEE 754 64-bit float and thus provides a 1-bit of margin for the text-to-float conversion operation.)
/// Any text input that describes a value outside the range of a 64-bit signed integer yields a REAL result.
/// Casting a REAL or INTEGER value to NUMERIC is a no-op, even if a real value could be losslessly converted to an integer.
fn cast_text_to_numeric(text: &str) -> OwnedValue {
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        // Looks like an integer
        if let Ok(i) = text.parse::<i64>() {
            return OwnedValue::Integer(i);
        }
    }
    // Try as float
    if let Ok(f) = text.parse::<f64>() {
        // Check if can be losslessly converted to 51-bit integer
        let i = f as i64;
        if f == i as f64 && i.abs() < (1i64 << 51) {
            return OwnedValue::Integer(i);
        }
        return OwnedValue::Float(f);
    }
    OwnedValue::Integer(0)
}

fn execute_sqlite_version(version_integer: i64) -> String {
    let major = version_integer / 1_000_000;
    let minor = (version_integer % 1_000_000) / 1_000;
    let release = version_integer % 1_000;

    format!("{}.{}.{}", major, minor, release)
}

fn to_f64(reg: &OwnedValue) -> Option<f64> {
    match reg {
        OwnedValue::Integer(i) => Some(*i as f64),
        OwnedValue::Float(f) => Some(*f),
        OwnedValue::Text(t) => t.parse::<f64>().ok(),
        OwnedValue::Agg(ctx) => to_f64(ctx.final_value()),
        _ => None,
    }
}

fn exec_math_unary(reg: &OwnedValue, function: &MathFunc) -> OwnedValue {
    // In case of some functions and integer input, return the input as is
    if let OwnedValue::Integer(_) = reg {
        if matches! { function, MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc }
        {
            return reg.clone();
        }
    }

    let f = match to_f64(reg) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let result = match function {
        MathFunc::Acos => f.acos(),
        MathFunc::Acosh => f.acosh(),
        MathFunc::Asin => f.asin(),
        MathFunc::Asinh => f.asinh(),
        MathFunc::Atan => f.atan(),
        MathFunc::Atanh => f.atanh(),
        MathFunc::Ceil | MathFunc::Ceiling => f.ceil(),
        MathFunc::Cos => f.cos(),
        MathFunc::Cosh => f.cosh(),
        MathFunc::Degrees => f.to_degrees(),
        MathFunc::Exp => f.exp(),
        MathFunc::Floor => f.floor(),
        MathFunc::Ln => f.ln(),
        MathFunc::Log10 => f.log10(),
        MathFunc::Log2 => f.log2(),
        MathFunc::Radians => f.to_radians(),
        MathFunc::Sin => f.sin(),
        MathFunc::Sinh => f.sinh(),
        MathFunc::Sqrt => f.sqrt(),
        MathFunc::Tan => f.tan(),
        MathFunc::Tanh => f.tanh(),
        MathFunc::Trunc => f.trunc(),
        _ => unreachable!("Unexpected mathematical unary function {:?}", function),
    };

    if result.is_nan() {
        OwnedValue::Null
    } else {
        OwnedValue::Float(result)
    }
}

fn exec_math_binary(lhs: &OwnedValue, rhs: &OwnedValue, function: &MathFunc) -> OwnedValue {
    let lhs = match to_f64(lhs) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let rhs = match to_f64(rhs) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let result = match function {
        MathFunc::Atan2 => lhs.atan2(rhs),
        MathFunc::Mod => lhs % rhs,
        MathFunc::Pow | MathFunc::Power => lhs.powf(rhs),
        _ => unreachable!("Unexpected mathematical binary function {:?}", function),
    };

    if result.is_nan() {
        OwnedValue::Null
    } else {
        OwnedValue::Float(result)
    }
}

fn exec_math_log(arg: &OwnedValue, base: Option<&OwnedValue>) -> OwnedValue {
    let f = match to_f64(arg) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let base = match base {
        Some(base) => match to_f64(base) {
            Some(f) => f,
            None => return OwnedValue::Null,
        },
        None => 10.0,
    };

    if f <= 0.0 || base <= 0.0 || base == 1.0 {
        return OwnedValue::Null;
    }

    OwnedValue::Float(f.log(base))
}

#[cfg(test)]
mod tests {

    use crate::{
        types::{SeekKey, SeekOp},
        vdbe::exec_replace,
    };

    use super::{
        exec_abs, exec_char, exec_hex, exec_if, exec_instr, exec_length, exec_like, exec_lower,
        exec_ltrim, exec_max, exec_min, exec_nullif, exec_quote, exec_random, exec_randomblob,
        exec_round, exec_rtrim, exec_sign, exec_soundex, exec_substring, exec_trim, exec_typeof,
        exec_unhex, exec_unicode, exec_upper, exec_zeroblob, execute_sqlite_version, get_new_rowid,
        AggContext, Cursor, CursorResult, LimboError, OwnedRecord, OwnedValue, Result,
    };
    use mockall::{mock, predicate};
    use rand::{rngs::mock::StepRng, thread_rng};
    use std::{cell::Ref, collections::HashMap, rc::Rc};

    mock! {
        Cursor {
            fn seek_to_last(&mut self) -> Result<CursorResult<()>>;
            fn seek<'a>(&mut self, key: SeekKey<'a>, op: SeekOp) -> Result<CursorResult<bool>>;
            fn rowid(&self) -> Result<Option<u64>>;
            fn seek_rowid(&mut self, rowid: u64) -> Result<CursorResult<bool>>;
        }
    }

    impl Cursor for MockCursor {
        fn root_page(&self) -> usize {
            unreachable!()
        }

        fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
            self.seek_to_last()
        }

        fn rowid(&self) -> Result<Option<u64>> {
            self.rowid()
        }

        fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<CursorResult<bool>> {
            self.seek(key, op)
        }

        fn rewind(&mut self) -> Result<CursorResult<()>> {
            unimplemented!()
        }

        fn next(&mut self) -> Result<CursorResult<()>> {
            unimplemented!()
        }

        fn record(&self) -> Result<Ref<Option<OwnedRecord>>> {
            unimplemented!()
        }

        fn is_empty(&self) -> bool {
            unimplemented!()
        }

        fn set_null_flag(&mut self, _flag: bool) {
            unimplemented!()
        }

        fn get_null_flag(&self) -> bool {
            unimplemented!()
        }

        fn insert(
            &mut self,
            _key: &OwnedValue,
            _record: &OwnedRecord,
            _is_leaf: bool,
        ) -> Result<CursorResult<()>> {
            unimplemented!()
        }

        fn wait_for_completion(&mut self) -> Result<()> {
            unimplemented!()
        }

        fn exists(&mut self, _key: &OwnedValue) -> Result<CursorResult<bool>> {
            unimplemented!()
        }

        fn btree_create(&mut self, _flags: usize) -> u32 {
            unimplemented!()
        }

        fn last(&mut self) -> Result<CursorResult<()>> {
            todo!()
        }

        fn prev(&mut self) -> Result<CursorResult<()>> {
            todo!()
        }
    }

    #[test]
    fn test_get_new_rowid() -> Result<()> {
        // Test case 0: Empty table
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid().return_once(|| Ok(None));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng())?;
        assert_eq!(
            result,
            CursorResult::Ok(1),
            "For an empty table, rowid should be 1"
        );

        // Test case 1: Normal case, rowid within i64::MAX
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid().return_once(|| Ok(Some(100)));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng())?;
        assert_eq!(result, CursorResult::Ok(101));

        // Test case 2: Rowid exceeds i64::MAX, need to generate random rowid
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(i64::MAX as u64)));
        mock.expect_seek()
            .with(predicate::always(), predicate::always())
            .returning(|rowid, _| {
                if rowid == SeekKey::TableRowId(50) {
                    Ok(CursorResult::Ok(false))
                } else {
                    Ok(CursorResult::Ok(true))
                }
            });

        // Mock the random number generation
        let new_rowid =
            get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), StepRng::new(1, 1))?;
        assert_eq!(new_rowid, CursorResult::Ok(50));

        // Test case 3: IO error
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(i64::MAX as u64)));
        mock.expect_seek()
            .with(predicate::always(), predicate::always())
            .return_once(|_, _| Ok(CursorResult::IO));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng());
        assert!(matches!(result, Ok(CursorResult::IO)));

        // Test case 4: Failure to generate new rowid
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(i64::MAX as u64)));
        mock.expect_seek()
            .with(predicate::always(), predicate::always())
            .returning(|_, _| Ok(CursorResult::Ok(true)));

        // Mock the random number generation
        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), StepRng::new(1, 1));
        assert!(matches!(result, Err(LimboError::InternalError(_))));

        Ok(())
    }

    #[test]
    fn test_length() {
        let input_str = OwnedValue::Text(Rc::new(String::from("bob")));
        let expected_len = OwnedValue::Integer(3);
        assert_eq!(exec_length(&input_str), expected_len);

        let input_integer = OwnedValue::Integer(123);
        let expected_len = OwnedValue::Integer(3);
        assert_eq!(exec_length(&input_integer), expected_len);

        let input_float = OwnedValue::Float(123.456);
        let expected_len = OwnedValue::Integer(7);
        assert_eq!(exec_length(&input_float), expected_len);

        let expected_blob = OwnedValue::Blob(Rc::new("example".as_bytes().to_vec()));
        let expected_len = OwnedValue::Integer(7);
        assert_eq!(exec_length(&expected_blob), expected_len);
    }

    #[test]
    fn test_quote() {
        let input = OwnedValue::Text(Rc::new(String::from("abc\0edf")));
        let expected = OwnedValue::Text(Rc::new(String::from("'abc'")));
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected = OwnedValue::Integer(123);
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::Text(Rc::new(String::from("hello''world")));
        let expected = OwnedValue::Text(Rc::new(String::from("'hello''world'")));
        assert_eq!(exec_quote(&input), expected);
    }

    #[test]
    fn test_typeof() {
        let input = OwnedValue::Null;
        let expected: OwnedValue = OwnedValue::Text(Rc::new("null".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected: OwnedValue = OwnedValue::Text(Rc::new("integer".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Float(123.456);
        let expected: OwnedValue = OwnedValue::Text(Rc::new("real".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Text(Rc::new("hello".to_string()));
        let expected: OwnedValue = OwnedValue::Text(Rc::new("text".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Blob(Rc::new("limbo".as_bytes().to_vec()));
        let expected: OwnedValue = OwnedValue::Text(Rc::new("blob".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Integer(123))));
        let expected = OwnedValue::Text(Rc::new("integer".to_string()));
        assert_eq!(exec_typeof(&input), expected);
    }

    #[test]
    fn test_unicode() {
        assert_eq!(
            exec_unicode(&OwnedValue::Text(Rc::new("a".to_string()))),
            OwnedValue::Integer(97)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Text(Rc::new("😊".to_string()))),
            OwnedValue::Integer(128522)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Text(Rc::new("".to_string()))),
            OwnedValue::Null
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Integer(23)),
            OwnedValue::Integer(50)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Integer(0)),
            OwnedValue::Integer(48)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Float(0.0)),
            OwnedValue::Integer(48)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Float(23.45)),
            OwnedValue::Integer(50)
        );
        assert_eq!(exec_unicode(&OwnedValue::Null), OwnedValue::Null);
        assert_eq!(
            exec_unicode(&OwnedValue::Blob(Rc::new("example".as_bytes().to_vec()))),
            OwnedValue::Integer(101)
        );
    }

    #[test]
    fn test_min_max() {
        let input_int_vec = vec![&OwnedValue::Integer(-1), &OwnedValue::Integer(10)];
        assert_eq!(exec_min(input_int_vec.clone()), OwnedValue::Integer(-1));
        assert_eq!(exec_max(input_int_vec.clone()), OwnedValue::Integer(10));

        let str1 = OwnedValue::Text(Rc::new(String::from("A")));
        let str2 = OwnedValue::Text(Rc::new(String::from("z")));
        let input_str_vec = vec![&str2, &str1];
        assert_eq!(
            exec_min(input_str_vec.clone()),
            OwnedValue::Text(Rc::new(String::from("A")))
        );
        assert_eq!(
            exec_max(input_str_vec.clone()),
            OwnedValue::Text(Rc::new(String::from("z")))
        );

        let input_null_vec = vec![&OwnedValue::Null, &OwnedValue::Null];
        assert_eq!(exec_min(input_null_vec.clone()), OwnedValue::Null);
        assert_eq!(exec_max(input_null_vec.clone()), OwnedValue::Null);

        let input_mixed_vec = vec![&OwnedValue::Integer(10), &str1];
        assert_eq!(exec_min(input_mixed_vec.clone()), OwnedValue::Integer(10));
        assert_eq!(
            exec_max(input_mixed_vec.clone()),
            OwnedValue::Text(Rc::new(String::from("A")))
        );
    }

    #[test]
    fn test_trim() {
        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("Bob and Alice")));
        assert_eq!(exec_trim(&input_str, None), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("Alice")));
        assert_eq!(exec_trim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_ltrim() {
        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("Bob and Alice     ")));
        assert_eq!(exec_ltrim(&input_str, None), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("Alice     ")));
        assert_eq!(exec_ltrim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_rtrim() {
        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice")));
        assert_eq!(exec_rtrim(&input_str, None), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice")));
        assert_eq!(exec_rtrim(&input_str, Some(pattern_str)), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("and Alice")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("     Bob")));
        assert_eq!(exec_rtrim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_soundex() {
        let input_str = OwnedValue::Text(Rc::new(String::from("Pfister")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("P236")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("husobee")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("H210")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("Tymczak")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("T522")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("Ashcraft")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("A261")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("Robert")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("R163")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("Rupert")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("R163")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("Rubin")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("R150")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("Kant")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("K530")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("Knuth")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("K530")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("x")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("X000")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("闪电五连鞭")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("?000")));
        assert_eq!(exec_soundex(&input_str), expected_str);
    }

    #[test]
    fn test_upper_case() {
        let input_str = OwnedValue::Text(Rc::new(String::from("Limbo")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("LIMBO")));
        assert_eq!(exec_upper(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_upper(&input_int).unwrap(), input_int);
        assert_eq!(exec_upper(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_lower_case() {
        let input_str = OwnedValue::Text(Rc::new(String::from("Limbo")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("limbo")));
        assert_eq!(exec_lower(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_lower(&input_int).unwrap(), input_int);
        assert_eq!(exec_lower(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_hex() {
        let input_str = OwnedValue::Text(Rc::new("limbo".to_string()));
        let expected_val = OwnedValue::Text(Rc::new(String::from("6C696D626F")));
        assert_eq!(exec_hex(&input_str), expected_val);

        let input_int = OwnedValue::Integer(100);
        let expected_val = OwnedValue::Text(Rc::new(String::from("313030")));
        assert_eq!(exec_hex(&input_int), expected_val);

        let input_float = OwnedValue::Float(12.34);
        let expected_val = OwnedValue::Text(Rc::new(String::from("31322E3334")));
        assert_eq!(exec_hex(&input_float), expected_val);
    }

    #[test]
    fn test_unhex() {
        let input = OwnedValue::Text(Rc::new(String::from("6F")));
        let expected = OwnedValue::Blob(Rc::new(vec![0x6f]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Text(Rc::new(String::from("6f")));
        let expected = OwnedValue::Blob(Rc::new(vec![0x6f]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Text(Rc::new(String::from("611")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Text(Rc::new(String::from("")));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Text(Rc::new(String::from("61x")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);
    }

    #[test]
    fn test_abs() {
        let int_positive_reg = OwnedValue::Integer(10);
        let int_negative_reg = OwnedValue::Integer(-10);
        assert_eq!(exec_abs(&int_positive_reg).unwrap(), int_positive_reg);
        assert_eq!(exec_abs(&int_negative_reg).unwrap(), int_positive_reg);

        let float_positive_reg = OwnedValue::Integer(10);
        let float_negative_reg = OwnedValue::Integer(-10);
        assert_eq!(exec_abs(&float_positive_reg).unwrap(), float_positive_reg);
        assert_eq!(exec_abs(&float_negative_reg).unwrap(), float_positive_reg);

        assert_eq!(
            exec_abs(&OwnedValue::Text(Rc::new(String::from("a")))).unwrap(),
            OwnedValue::Float(0.0)
        );
        assert_eq!(exec_abs(&OwnedValue::Null).unwrap(), OwnedValue::Null);
    }

    #[test]
    fn test_char() {
        assert_eq!(
            exec_char(vec![OwnedValue::Integer(108), OwnedValue::Integer(105)]),
            OwnedValue::Text(Rc::new("li".to_string()))
        );
        assert_eq!(exec_char(vec![]), OwnedValue::Text(Rc::new("".to_string())));
        assert_eq!(
            exec_char(vec![OwnedValue::Null]),
            OwnedValue::Text(Rc::new("".to_string()))
        );
        assert_eq!(
            exec_char(vec![OwnedValue::Text(Rc::new("a".to_string()))]),
            OwnedValue::Text(Rc::new("".to_string()))
        );
    }

    #[test]
    fn test_like_no_cache() {
        assert!(exec_like(None, "a%", "aaaa"));
        assert!(exec_like(None, "%a%a", "aaaa"));
        assert!(exec_like(None, "%a.a", "aaaa"));
        assert!(exec_like(None, "a.a%", "aaaa"));
        assert!(!exec_like(None, "%a.ab", "aaaa"));
    }

    #[test]
    fn test_like_with_cache() {
        let mut cache = HashMap::new();
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.ab", "aaaa"));

        // again after values have been cached
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.ab", "aaaa"));
    }

    #[test]
    fn test_random() {
        match exec_random() {
            OwnedValue::Integer(value) => {
                // Check that the value is within the range of i64
                assert!(
                    (i64::MIN..=i64::MAX).contains(&value),
                    "Random number out of range"
                );
            }
            _ => panic!("exec_random did not return an Integer variant"),
        }
    }

    #[test]
    fn test_exec_randomblob() {
        struct TestCase {
            input: OwnedValue,
            expected_len: usize,
        }

        let test_cases = vec![
            TestCase {
                input: OwnedValue::Integer(5),
                expected_len: 5,
            },
            TestCase {
                input: OwnedValue::Integer(0),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Integer(-1),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Text(Rc::new(String::from(""))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Text(Rc::new(String::from("5"))),
                expected_len: 5,
            },
            TestCase {
                input: OwnedValue::Text(Rc::new(String::from("0"))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Text(Rc::new(String::from("-1"))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Float(2.9),
                expected_len: 2,
            },
            TestCase {
                input: OwnedValue::Float(-3.14),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Null,
                expected_len: 1,
            },
        ];

        for test_case in &test_cases {
            let result = exec_randomblob(&test_case.input);
            match result {
                OwnedValue::Blob(blob) => {
                    assert_eq!(blob.len(), test_case.expected_len);
                }
                _ => panic!("exec_randomblob did not return a Blob variant"),
            }
        }
    }

    #[test]
    fn test_exec_round() {
        let input_val = OwnedValue::Float(123.456);
        let expected_val = OwnedValue::Float(123.0);
        assert_eq!(exec_round(&input_val, None), expected_val);

        let input_val = OwnedValue::Float(123.456);
        let precision_val = OwnedValue::Integer(2);
        let expected_val = OwnedValue::Float(123.46);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Float(123.456);
        let precision_val = OwnedValue::Text(Rc::new(String::from("1")));
        let expected_val = OwnedValue::Float(123.5);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Text(Rc::new(String::from("123.456")));
        let precision_val = OwnedValue::Integer(2);
        let expected_val = OwnedValue::Float(123.46);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Integer(123);
        let precision_val = OwnedValue::Integer(1);
        let expected_val = OwnedValue::Float(123.0);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Float(100.123);
        let expected_val = OwnedValue::Float(100.0);
        assert_eq!(exec_round(&input_val, None), expected_val);

        let input_val = OwnedValue::Float(100.123);
        let expected_val = OwnedValue::Null;
        assert_eq!(exec_round(&input_val, Some(OwnedValue::Null)), expected_val);
    }

    #[test]
    fn test_exec_if() {
        let reg = OwnedValue::Integer(0);
        let null_reg = OwnedValue::Integer(0);
        assert!(!exec_if(&reg, &null_reg, false));
        assert!(exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Integer(1);
        let null_reg = OwnedValue::Integer(0);
        assert!(exec_if(&reg, &null_reg, false));
        assert!(!exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Null;
        let null_reg = OwnedValue::Integer(0);
        assert!(!exec_if(&reg, &null_reg, false));
        assert!(!exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Null;
        let null_reg = OwnedValue::Integer(1);
        assert!(exec_if(&reg, &null_reg, false));
        assert!(exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Null;
        let null_reg = OwnedValue::Null;
        assert!(!exec_if(&reg, &null_reg, false));
        assert!(!exec_if(&reg, &null_reg, true));
    }

    #[test]
    fn test_nullif() {
        assert_eq!(
            exec_nullif(&OwnedValue::Integer(1), &OwnedValue::Integer(1)),
            OwnedValue::Null
        );
        assert_eq!(
            exec_nullif(&OwnedValue::Float(1.1), &OwnedValue::Float(1.1)),
            OwnedValue::Null
        );
        assert_eq!(
            exec_nullif(
                &OwnedValue::Text(Rc::new("limbo".to_string())),
                &OwnedValue::Text(Rc::new("limbo".to_string()))
            ),
            OwnedValue::Null
        );

        assert_eq!(
            exec_nullif(&OwnedValue::Integer(1), &OwnedValue::Integer(2)),
            OwnedValue::Integer(1)
        );
        assert_eq!(
            exec_nullif(&OwnedValue::Float(1.1), &OwnedValue::Float(1.2)),
            OwnedValue::Float(1.1)
        );
        assert_eq!(
            exec_nullif(
                &OwnedValue::Text(Rc::new("limbo".to_string())),
                &OwnedValue::Text(Rc::new("limb".to_string()))
            ),
            OwnedValue::Text(Rc::new("limbo".to_string()))
        );
    }

    #[test]
    fn test_substring() {
        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::Text(Rc::new(String::from("lim")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(10);
        let expected_val = OwnedValue::Text(Rc::new(String::from("limbo")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::Text(Rc::new(String::from("")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(3);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::Text(Rc::new(String::from("mbo")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::Text(Rc::new(String::from("")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );
    }

    #[test]
    fn test_exec_instr() {
        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("im")));
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("o")));
        let expected = OwnedValue::Integer(5);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("liiiiimbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("ii")));
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("limboX")));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("")));
        let pattern = OwnedValue::Text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("")));
        let pattern = OwnedValue::Text(Rc::new(String::from("")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::Text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Integer(123);
        let pattern = OwnedValue::Integer(2);
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Integer(123);
        let pattern = OwnedValue::Integer(5);
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Float(2.3);
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Float(5.6);
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Text(Rc::new(String::from(".")));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(Rc::new(vec![1, 2, 3, 4, 5]));
        let pattern = OwnedValue::Blob(Rc::new(vec![3, 4]));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(Rc::new(vec![1, 2, 3, 4, 5]));
        let pattern = OwnedValue::Blob(Rc::new(vec![3, 2]));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(Rc::new(vec![0x61, 0x62, 0x63, 0x64, 0x65]));
        let pattern = OwnedValue::Text(Rc::new(String::from("cd")));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("abcde")));
        let pattern = OwnedValue::Blob(Rc::new(vec![0x63, 0x64]));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);
    }

    #[test]
    fn test_exec_sign() {
        let input = OwnedValue::Integer(42);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Integer(-42);
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Integer(0);
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(0.0);
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(0.1);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(42.0);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(-42.0);
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Text(Rc::new("abc".to_string()));
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Text(Rc::new("42".to_string()));
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Text(Rc::new("-42".to_string()));
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Text(Rc::new("0".to_string()));
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"abc".to_vec()));
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"42".to_vec()));
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"-42".to_vec()));
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"0".to_vec()));
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Null;
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);
    }

    #[test]
    fn test_exec_zeroblob() {
        let input = OwnedValue::Integer(0);
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Null;
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Integer(4);
        let expected = OwnedValue::Blob(Rc::new(vec![0; 4]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Integer(-1);
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Text(Rc::new("5".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![0; 5]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Text(Rc::new("-5".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Text(Rc::new("text".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Float(2.6);
        let expected = OwnedValue::Blob(Rc::new(vec![0; 2]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Blob(Rc::new(vec![1]));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);
    }

    #[test]
    fn test_execute_sqlite_version() {
        let version_integer = 3046001;
        let expected = "3.46.1";
        assert_eq!(execute_sqlite_version(version_integer), expected);
    }

    #[test]
    fn test_replace() {
        let input_str = OwnedValue::Text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("b")));
        let replace_str = OwnedValue::Text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("aoa")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::Text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("b")));
        let replace_str = OwnedValue::Text(Rc::new(String::from("")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("o")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::Text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("b")));
        let replace_str = OwnedValue::Text(Rc::new(String::from("abc")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("abcoabc")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::Text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("a")));
        let replace_str = OwnedValue::Text(Rc::new(String::from("b")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("bob")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::Text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("")));
        let replace_str = OwnedValue::Text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("bob")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::Text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::Null;
        let replace_str = OwnedValue::Text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::Null;
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::Text(Rc::new(String::from("bo5")));
        let pattern_str = OwnedValue::Integer(5);
        let replace_str = OwnedValue::Text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("boa")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::Text(Rc::new(String::from("bo5.0")));
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::Text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("boa")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::Text(Rc::new(String::from("bo5")));
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::Text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("bo5")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::Text(Rc::new(String::from("bo5.0")));
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::Float(6.0);
        let expected_str = OwnedValue::Text(Rc::new(String::from("bo6.0")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        // todo: change this test to use (0.1 + 0.2) instead of 0.3 when decimals are implemented.
        let input_str = OwnedValue::Text(Rc::new(String::from("tes3")));
        let pattern_str = OwnedValue::Integer(3);
        let replace_str = OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Float(0.3))));
        let expected_str = OwnedValue::Text(Rc::new(String::from("tes0.3")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );
    }
}
