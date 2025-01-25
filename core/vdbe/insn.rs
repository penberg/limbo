use std::num::NonZero;
use std::rc::Rc;

use super::{AggFunc, BranchOffset, CursorID, FuncCtx, PageIdx};
use crate::storage::wal::CheckpointMode;
use crate::types::{OwnedRecord, OwnedValue};
use limbo_macros::Description;

#[derive(Description, Debug)]
pub enum Insn {
    // Initialize the program state and jump to the given PC.
    Init {
        target_pc: BranchOffset,
    },
    // Write a NULL into register dest. If dest_end is Some, then also write NULL into register dest_end and every register in between dest and dest_end. If dest_end is not set, then only register dest is set to NULL.
    Null {
        dest: usize,
        dest_end: Option<usize>,
    },
    // Move the cursor P1 to a null row. Any Column operations that occur while the cursor is on the null row will always write a NULL.
    NullRow {
        cursor_id: CursorID,
    },
    // Add two registers and store the result in a third register.
    Add {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Subtract rhs from lhs and store in dest
    Subtract {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Multiply two registers and store the result in a third register.
    Multiply {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Divide lhs by rhs and store the result in a third register.
    Divide {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Compare two vectors of registers in reg(P1)..reg(P1+P3-1) (call this vector "A") and in reg(P2)..reg(P2+P3-1) ("B"). Save the result of the comparison for use by the next Jump instruct.
    Compare {
        start_reg_a: usize,
        start_reg_b: usize,
        count: usize,
    },
    // Place the result of rhs bitwise AND lhs in third register.
    BitAnd {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Place the result of rhs bitwise OR lhs in third register.
    BitOr {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Place the result of bitwise NOT register P1 in dest register.
    BitNot {
        reg: usize,
        dest: usize,
    },
    // Checkpoint the database (applying wal file content to database file).
    Checkpoint {
        database: usize,                 // checkpoint database P1
        checkpoint_mode: CheckpointMode, // P2 checkpoint mode
        dest: usize,                     // P3 checkpoint result
    },
    // Divide lhs by rhs and place the remainder in dest register.
    Remainder {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Jump to the instruction at address P1, P2, or P3 depending on whether in the most recent Compare instruction the P1 vector was less than, equal to, or greater than the P2 vector, respectively.
    Jump {
        target_pc_lt: BranchOffset,
        target_pc_eq: BranchOffset,
        target_pc_gt: BranchOffset,
    },
    // Move the P3 values in register P1..P1+P3-1 over into registers P2..P2+P3-1. Registers P1..P1+P3-1 are left holding a NULL. It is an error for register ranges P1..P1+P3-1 and P2..P2+P3-1 to overlap. It is an error for P3 to be less than 1.
    Move {
        source_reg: usize,
        dest_reg: usize,
        count: usize,
    },
    // If the given register is a positive integer, decrement it by decrement_by and jump to the given PC.
    IfPos {
        reg: usize,
        target_pc: BranchOffset,
        decrement_by: usize,
    },
    // If the given register is not NULL, jump to the given PC.
    NotNull {
        reg: usize,
        target_pc: BranchOffset,
    },
    // Compare two registers and jump to the given PC if they are equal.
    Eq {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// Jump if either of the operands is null. Used for "jump when false" logic.
        /// Eg. "SELECT * FROM users WHERE id = NULL" becomes:
        /// <JUMP TO NEXT ROW IF id != NULL>
        /// Without the jump_if_null flag it would not jump because the logical comparison "id != NULL" is never true.
        /// This flag indicates that if either is null we should still jump.
        jump_if_null: bool,
    },
    // Compare two registers and jump to the given PC if they are not equal.
    Ne {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// Jump if either of the operands is null. Used for "jump when false" logic.
        jump_if_null: bool,
    },
    // Compare two registers and jump to the given PC if the left-hand side is less than the right-hand side.
    Lt {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// Jump if either of the operands is null. Used for "jump when false" logic.
        jump_if_null: bool,
    },
    // Compare two registers and jump to the given PC if the left-hand side is less than or equal to the right-hand side.
    Le {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// Jump if either of the operands is null. Used for "jump when false" logic.
        jump_if_null: bool,
    },
    // Compare two registers and jump to the given PC if the left-hand side is greater than the right-hand side.
    Gt {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// Jump if either of the operands is null. Used for "jump when false" logic.
        jump_if_null: bool,
    },
    // Compare two registers and jump to the given PC if the left-hand side is greater than or equal to the right-hand side.
    Ge {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// Jump if either of the operands is null. Used for "jump when false" logic.
        jump_if_null: bool,
    },
    /// Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[jump_if_null\] != 0)
    If {
        reg: usize,              // P1
        target_pc: BranchOffset, // P2
        /// P3. If r\[reg\] is null, jump iff r\[jump_if_null\] != 0
        jump_if_null: bool,
    },
    /// Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[jump_if_null\] != 0)
    IfNot {
        reg: usize,              // P1
        target_pc: BranchOffset, // P2
        /// P3. If r\[reg\] is null, jump iff r\[jump_if_null\] != 0
        jump_if_null: bool,
    },
    // Open a cursor for reading.
    OpenReadAsync {
        cursor_id: CursorID,
        root_page: PageIdx,
    },

    // Await for the completion of open cursor.
    OpenReadAwait,

    // Open a cursor for a pseudo-table that contains a single row.
    OpenPseudo {
        cursor_id: CursorID,
        content_reg: usize,
        num_fields: usize,
    },

    // Rewind the cursor to the beginning of the B-Tree.
    RewindAsync {
        cursor_id: CursorID,
    },

    // Await for the completion of cursor rewind.
    RewindAwait {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    LastAsync {
        cursor_id: CursorID,
    },

    LastAwait {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    // Read a column from the current row of the cursor.
    Column {
        cursor_id: CursorID,
        column: usize,
        dest: usize,
    },

    // Make a record and write it to destination register.
    MakeRecord {
        start_reg: usize, // P1
        count: usize,     // P2
        dest_reg: usize,  // P3
    },

    // Emit a row of results.
    ResultRow {
        start_reg: usize, // P1
        count: usize,     // P2
    },

    // Advance the cursor to the next row.
    NextAsync {
        cursor_id: CursorID,
    },

    // Await for the completion of cursor advance.
    NextAwait {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    PrevAsync {
        cursor_id: CursorID,
    },

    PrevAwait {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    // Halt the program.
    Halt {
        err_code: usize,
        description: String,
    },

    // Start a transaction.
    Transaction {
        write: bool,
    },

    // Branch to the given PC.
    Goto {
        target_pc: BranchOffset,
    },

    // Stores the current program counter into register 'return_reg' then jumps to address target_pc.
    Gosub {
        target_pc: BranchOffset,
        return_reg: usize,
    },

    // Returns to the program counter stored in register 'return_reg'.
    Return {
        return_reg: usize,
    },

    // Write an integer value into a register.
    Integer {
        value: i64,
        dest: usize,
    },

    // Write a float value into a register
    Real {
        value: f64,
        dest: usize,
    },

    // If register holds an integer, transform it to a float
    RealAffinity {
        register: usize,
    },

    // Write a string value into a register.
    String8 {
        value: String,
        dest: usize,
    },

    // Write a blob value into a register.
    Blob {
        value: Vec<u8>,
        dest: usize,
    },

    // Read the rowid of the current row.
    RowId {
        cursor_id: CursorID,
        dest: usize,
    },

    // Seek to a rowid in the cursor. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekRowid {
        cursor_id: CursorID,
        src_reg: usize,
        target_pc: BranchOffset,
    },

    // P1 is an open index cursor and P3 is a cursor on the corresponding table. This opcode does a deferred seek of the P3 table cursor to the row that corresponds to the current row of P1.
    // This is a deferred seek. Nothing actually happens until the cursor is used to read a record. That way, if no reads occur, no unnecessary I/O happens.
    DeferredSeek {
        index_cursor_id: CursorID,
        table_cursor_id: CursorID,
    },

    // If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    // If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    // Seek to the first index entry that is greater than or equal to the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
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
    SeekGT {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    // The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    // If the P1 index entry is greater or equal than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxGE {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    // The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    // If the P1 index entry is greater than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxGT {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    // Decrement the given register and jump to the given PC if the result is zero.
    DecrJumpZero {
        reg: usize,
        target_pc: BranchOffset,
    },

    AggStep {
        acc_reg: usize,
        col: usize,
        delimiter: usize,
        func: AggFunc,
    },

    AggFinal {
        register: usize,
        func: AggFunc,
    },

    // Open a sorter.
    SorterOpen {
        cursor_id: CursorID, // P1
        columns: usize,      // P2
        order: OwnedRecord,  // P4. 0 if ASC and 1 if DESC
    },

    // Insert a row into the sorter.
    SorterInsert {
        cursor_id: CursorID,
        record_reg: usize,
    },

    // Sort the rows in the sorter.
    SorterSort {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    // Retrieve the next row from the sorter.
    SorterData {
        cursor_id: CursorID,  // P1
        dest_reg: usize,      // P2
        pseudo_cursor: usize, // P3
    },

    // Advance to the next row in the sorter.
    SorterNext {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    // Function
    Function {
        constant_mask: i32, // P1
        start_reg: usize,   // P2, start of argument registers
        dest: usize,        // P3
        func: FuncCtx,      // P4
    },

    InitCoroutine {
        yield_reg: usize,
        jump_on_definition: BranchOffset,
        start_offset: BranchOffset,
    },

    EndCoroutine {
        yield_reg: usize,
    },

    Yield {
        yield_reg: usize,
        end_offset: BranchOffset,
    },

    InsertAsync {
        cursor: CursorID,
        key_reg: usize,    // Must be int.
        record_reg: usize, // Blob of record data.
        flag: usize,       // Flags used by insert, for now not used.
    },

    InsertAwait {
        cursor_id: usize,
    },

    DeleteAsync {
        cursor_id: CursorID,
    },

    DeleteAwait {
        cursor_id: CursorID,
    },

    NewRowid {
        cursor: CursorID,        // P1
        rowid_reg: usize,        // P2  Destination register to store the new rowid
        prev_largest_reg: usize, // P3 Previous largest rowid in the table (Not used for now)
    },

    MustBeInt {
        reg: usize,
    },

    SoftNull {
        reg: usize,
    },

    NotExists {
        cursor: CursorID,
        rowid_reg: usize,
        target_pc: BranchOffset,
    },

    OpenWriteAsync {
        cursor_id: CursorID,
        root_page: PageIdx,
    },

    OpenWriteAwait {},

    Copy {
        src_reg: usize,
        dst_reg: usize,
        amount: usize, // 0 amount means we include src_reg, dst_reg..=dst_reg+amount = src_reg..=src_reg+amount
    },

    /// Allocate a new b-tree.
    CreateBtree {
        /// Allocate b-tree in main database if zero or in temp database if non-zero (P1).
        db: usize,
        /// The root page of the new b-tree (P2).
        root: usize,
        /// Flags (P3).
        flags: usize,
    },

    /// Close a cursor.
    Close {
        cursor_id: CursorID,
    },

    /// Check if the register is null.
    IsNull {
        /// Source register (P1).
        src: usize,

        /// Jump to this PC if the register is null (P2).
        target_pc: BranchOffset,
    },
    ParseSchema {
        db: usize,
        where_clause: String,
    },

    // Place the result of lhs >> rhs in dest register.
    ShiftRight {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },

    // Place the result of lhs << rhs in dest register.
    ShiftLeft {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },

    /// Get parameter variable.
    Variable {
        index: NonZero<usize>,
        dest: usize,
    },
    /// If either register is null put null else put 0
    ZeroOrNull {
        /// Source register (P1).
        rg1: usize,
        rg2: usize,
        dest: usize,
    },
    /// Interpret the value in reg as boolean and store its compliment in destination
    Not {
        reg: usize,
        dest: usize,
    },
    /// Concatenates the `rhs` and `lhs` values and stores the result in the third register.
    Concat {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Take the logical AND of the values in registers P1 and P2 and write the result into register P3.
    And {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Take the logical OR of the values in register P1 and P2 and store the answer in register P3.
    Or {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
}

fn cast_text_to_numerical(value: &str) -> OwnedValue {
    if let Ok(x) = value.parse::<i64>() {
        OwnedValue::Integer(x)
    } else if let Ok(x) = value.parse::<f64>() {
        OwnedValue::Float(x)
    } else {
        OwnedValue::Integer(0)
    }
}

pub fn exec_add(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }
    match (lhs, rhs) {
        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
            let result = lhs.overflowing_add(*rhs);
            if result.1 {
                OwnedValue::Float(*lhs as f64 + *rhs as f64)
            } else {
                OwnedValue::Integer(result.0)
            }
        }
        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => OwnedValue::Float(lhs + rhs),
        (OwnedValue::Float(f), OwnedValue::Integer(i))
        | (OwnedValue::Integer(i), OwnedValue::Float(f)) => OwnedValue::Float(*f + *i as f64),
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_add(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) | (other, OwnedValue::Text(text)) => {
            exec_add(&cast_text_to_numerical(&text.value), other)
        }
        _ => todo!(),
    }
}

pub fn exec_subtract(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }
    match (lhs, rhs) {
        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
            let result = lhs.overflowing_sub(*rhs);
            if result.1 {
                OwnedValue::Float(*lhs as f64 - *rhs as f64)
            } else {
                OwnedValue::Integer(result.0)
            }
        }
        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => OwnedValue::Float(lhs - rhs),
        (OwnedValue::Float(lhs), OwnedValue::Integer(rhs)) => OwnedValue::Float(lhs - *rhs as f64),
        (OwnedValue::Integer(lhs), OwnedValue::Float(rhs)) => OwnedValue::Float(*lhs as f64 - rhs),
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_subtract(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) => {
            exec_subtract(&cast_text_to_numerical(&text.value), other)
        }
        (other, OwnedValue::Text(text)) => {
            exec_subtract(other, &cast_text_to_numerical(&text.value))
        }
        _ => todo!(),
    }
}
pub fn exec_multiply(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }
    match (lhs, rhs) {
        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
            let result = lhs.overflowing_mul(*rhs);
            if result.1 {
                OwnedValue::Float(*lhs as f64 * *rhs as f64)
            } else {
                OwnedValue::Integer(result.0)
            }
        }
        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => OwnedValue::Float(lhs * rhs),
        (OwnedValue::Integer(i), OwnedValue::Float(f))
        | (OwnedValue::Float(f), OwnedValue::Integer(i)) => OwnedValue::Float(*i as f64 * { *f }),
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_multiply(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) | (other, OwnedValue::Text(text)) => {
            exec_multiply(&cast_text_to_numerical(&text.value), other)
        }

        _ => todo!(),
    }
}

pub fn exec_divide(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }
    match (lhs, rhs) {
        (_, OwnedValue::Integer(0)) | (_, OwnedValue::Float(0.0)) => OwnedValue::Null,
        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
            let result = lhs.overflowing_div(*rhs);
            if result.1 {
                OwnedValue::Float(*lhs as f64 / *rhs as f64)
            } else {
                OwnedValue::Integer(result.0)
            }
        }
        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => OwnedValue::Float(lhs / rhs),
        (OwnedValue::Float(lhs), OwnedValue::Integer(rhs)) => OwnedValue::Float(lhs / *rhs as f64),
        (OwnedValue::Integer(lhs), OwnedValue::Float(rhs)) => OwnedValue::Float(*lhs as f64 / rhs),
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_divide(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) => exec_divide(&cast_text_to_numerical(&text.value), other),
        (other, OwnedValue::Text(text)) => exec_divide(other, &cast_text_to_numerical(&text.value)),
        _ => todo!(),
    }
}

pub fn exec_bit_and(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }
    match (lhs, rhs) {
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (_, OwnedValue::Integer(0))
        | (OwnedValue::Integer(0), _)
        | (_, OwnedValue::Float(0.0))
        | (OwnedValue::Float(0.0), _) => OwnedValue::Integer(0),
        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => OwnedValue::Integer(lh & rh),
        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
            OwnedValue::Integer(*lh as i64 & *rh as i64)
        }
        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => OwnedValue::Integer(*lh as i64 & rh),
        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => OwnedValue::Integer(lh & *rh as i64),
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_bit_and(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) | (other, OwnedValue::Text(text)) => {
            exec_bit_and(&cast_text_to_numerical(&text.value), other)
        }
        _ => todo!(),
    }
}

pub fn exec_bit_or(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }
    match (lhs, rhs) {
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => OwnedValue::Integer(lh | rh),
        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => OwnedValue::Integer(*lh as i64 | rh),
        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => OwnedValue::Integer(lh | *rh as i64),
        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
            OwnedValue::Integer(*lh as i64 | *rh as i64)
        }
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_bit_or(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) | (other, OwnedValue::Text(text)) => {
            exec_bit_or(&cast_text_to_numerical(&text.value), other)
        }
        _ => todo!(),
    }
}

pub fn exec_remainder(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }
    match (lhs, rhs) {
        (OwnedValue::Null, _)
        | (_, OwnedValue::Null)
        | (_, OwnedValue::Integer(0))
        | (_, OwnedValue::Float(0.0)) => OwnedValue::Null,
        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => OwnedValue::Integer(lhs % rhs),
        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
            OwnedValue::Float(((*lhs as i64) % (*rhs as i64)) as f64)
        }
        (OwnedValue::Float(lhs), OwnedValue::Integer(rhs)) => {
            OwnedValue::Float(((*lhs as i64) % rhs) as f64)
        }
        (OwnedValue::Integer(lhs), OwnedValue::Float(rhs)) => {
            OwnedValue::Float((lhs % *rhs as i64) as f64)
        }
        _ => todo!(),
    }
}

pub fn exec_bit_not(mut reg: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = reg {
        reg = agg.final_value();
    }
    match reg {
        OwnedValue::Null => OwnedValue::Null,
        OwnedValue::Integer(i) => OwnedValue::Integer(!i),
        OwnedValue::Float(f) => OwnedValue::Integer(!(*f as i64)),
        OwnedValue::Text(text) => exec_bit_not(&cast_text_to_numerical(&text.value)),
        _ => todo!(),
    }
}

pub fn exec_shift_left(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }
    match (lhs, rhs) {
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
            OwnedValue::Integer(compute_shl(*lh, *rh))
        }
        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
            OwnedValue::Integer(compute_shl(*lh as i64, *rh))
        }
        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
            OwnedValue::Integer(compute_shl(*lh, *rh as i64))
        }
        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
            OwnedValue::Integer(compute_shl(*lh as i64, *rh as i64))
        }
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_shift_left(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) => {
            exec_shift_left(&cast_text_to_numerical(&text.value), other)
        }
        (other, OwnedValue::Text(text)) => {
            exec_shift_left(other, &cast_text_to_numerical(&text.value))
        }
        _ => todo!(),
    }
}

fn compute_shl(lhs: i64, rhs: i64) -> i64 {
    if rhs == 0 {
        lhs
    } else if rhs >= 64 || rhs <= -64 {
        0
    } else if rhs < 0 {
        // if negative do right shift
        lhs >> (-rhs)
    } else {
        lhs << rhs
    }
}

pub fn exec_shift_right(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }
    match (lhs, rhs) {
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
            OwnedValue::Integer(compute_shr(*lh, *rh))
        }
        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
            OwnedValue::Integer(compute_shr(*lh as i64, *rh))
        }
        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
            OwnedValue::Integer(compute_shr(*lh, *rh as i64))
        }
        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
            OwnedValue::Integer(compute_shr(*lh as i64, *rh as i64))
        }
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_shift_right(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) => {
            exec_shift_right(&cast_text_to_numerical(&text.value), other)
        }
        (other, OwnedValue::Text(text)) => {
            exec_shift_right(other, &cast_text_to_numerical(&text.value))
        }
        _ => todo!(),
    }
}

fn compute_shr(lhs: i64, rhs: i64) -> i64 {
    if rhs == 0 {
        lhs
    } else if rhs >= 64 || rhs <= -64 {
        0
    } else if rhs < 0 {
        // if negative do left shift
        lhs << (-rhs)
    } else {
        lhs >> rhs
    }
}

pub fn exec_boolean_not(mut reg: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = reg {
        reg = agg.final_value();
    }
    match reg {
        OwnedValue::Null => OwnedValue::Null,
        OwnedValue::Integer(i) => OwnedValue::Integer((*i == 0) as i64),
        OwnedValue::Float(f) => OwnedValue::Integer((*f == 0.0) as i64),
        OwnedValue::Text(text) => exec_boolean_not(&cast_text_to_numerical(&text.value)),
        _ => todo!(),
    }
}

pub fn exec_concat(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    match (lhs, rhs) {
        (OwnedValue::Text(lhs_text), OwnedValue::Text(rhs_text)) => {
            OwnedValue::build_text(Rc::new(lhs_text.value.as_ref().clone() + &rhs_text.value))
        }
        (OwnedValue::Text(lhs_text), OwnedValue::Integer(rhs_int)) => OwnedValue::build_text(
            Rc::new(lhs_text.value.as_ref().clone() + &rhs_int.to_string()),
        ),
        (OwnedValue::Text(lhs_text), OwnedValue::Float(rhs_float)) => OwnedValue::build_text(
            Rc::new(lhs_text.value.as_ref().clone() + &rhs_float.to_string()),
        ),
        (OwnedValue::Text(lhs_text), OwnedValue::Agg(rhs_agg)) => OwnedValue::build_text(Rc::new(
            lhs_text.value.as_ref().clone() + &rhs_agg.final_value().to_string(),
        )),

        (OwnedValue::Integer(lhs_int), OwnedValue::Text(rhs_text)) => {
            OwnedValue::build_text(Rc::new(lhs_int.to_string() + &rhs_text.value))
        }
        (OwnedValue::Integer(lhs_int), OwnedValue::Integer(rhs_int)) => {
            OwnedValue::build_text(Rc::new(lhs_int.to_string() + &rhs_int.to_string()))
        }
        (OwnedValue::Integer(lhs_int), OwnedValue::Float(rhs_float)) => {
            OwnedValue::build_text(Rc::new(lhs_int.to_string() + &rhs_float.to_string()))
        }
        (OwnedValue::Integer(lhs_int), OwnedValue::Agg(rhs_agg)) => OwnedValue::build_text(
            Rc::new(lhs_int.to_string() + &rhs_agg.final_value().to_string()),
        ),

        (OwnedValue::Float(lhs_float), OwnedValue::Text(rhs_text)) => {
            OwnedValue::build_text(Rc::new(lhs_float.to_string() + &rhs_text.value))
        }
        (OwnedValue::Float(lhs_float), OwnedValue::Integer(rhs_int)) => {
            OwnedValue::build_text(Rc::new(lhs_float.to_string() + &rhs_int.to_string()))
        }
        (OwnedValue::Float(lhs_float), OwnedValue::Float(rhs_float)) => {
            OwnedValue::build_text(Rc::new(lhs_float.to_string() + &rhs_float.to_string()))
        }
        (OwnedValue::Float(lhs_float), OwnedValue::Agg(rhs_agg)) => OwnedValue::build_text(
            Rc::new(lhs_float.to_string() + &rhs_agg.final_value().to_string()),
        ),

        (OwnedValue::Agg(lhs_agg), OwnedValue::Text(rhs_text)) => {
            OwnedValue::build_text(Rc::new(lhs_agg.final_value().to_string() + &rhs_text.value))
        }
        (OwnedValue::Agg(lhs_agg), OwnedValue::Integer(rhs_int)) => OwnedValue::build_text(
            Rc::new(lhs_agg.final_value().to_string() + &rhs_int.to_string()),
        ),
        (OwnedValue::Agg(lhs_agg), OwnedValue::Float(rhs_float)) => OwnedValue::build_text(
            Rc::new(lhs_agg.final_value().to_string() + &rhs_float.to_string()),
        ),
        (OwnedValue::Agg(lhs_agg), OwnedValue::Agg(rhs_agg)) => OwnedValue::build_text(Rc::new(
            lhs_agg.final_value().to_string() + &rhs_agg.final_value().to_string(),
        )),

        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Blob(_), _) | (_, OwnedValue::Blob(_)) => {
            todo!("TODO: Handle Blob conversion to String")
        }
        (OwnedValue::Record(_), _) | (_, OwnedValue::Record(_)) => unreachable!(),
    }
}

pub fn exec_and(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }

    match (lhs, rhs) {
        (_, OwnedValue::Integer(0))
        | (OwnedValue::Integer(0), _)
        | (_, OwnedValue::Float(0.0))
        | (OwnedValue::Float(0.0), _) => OwnedValue::Integer(0),
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_and(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) | (other, OwnedValue::Text(text)) => {
            exec_and(&cast_text_to_numerical(&text.value), other)
        }
        _ => OwnedValue::Integer(1),
    }
}

pub fn exec_or(mut lhs: &OwnedValue, mut rhs: &OwnedValue) -> OwnedValue {
    if let OwnedValue::Agg(agg) = lhs {
        lhs = agg.final_value();
    }
    if let OwnedValue::Agg(agg) = rhs {
        rhs = agg.final_value();
    }

    match (lhs, rhs) {
        (OwnedValue::Null, OwnedValue::Null)
        | (OwnedValue::Null, OwnedValue::Float(0.0))
        | (OwnedValue::Float(0.0), OwnedValue::Null)
        | (OwnedValue::Null, OwnedValue::Integer(0))
        | (OwnedValue::Integer(0), OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Float(0.0), OwnedValue::Integer(0))
        | (OwnedValue::Integer(0), OwnedValue::Float(0.0))
        | (OwnedValue::Float(0.0), OwnedValue::Float(0.0))
        | (OwnedValue::Integer(0), OwnedValue::Integer(0)) => OwnedValue::Integer(0),
        (OwnedValue::Text(lhs), OwnedValue::Text(rhs)) => exec_or(
            &cast_text_to_numerical(&lhs.value),
            &cast_text_to_numerical(&rhs.value),
        ),
        (OwnedValue::Text(text), other) | (other, OwnedValue::Text(text)) => {
            exec_or(&cast_text_to_numerical(&text.value), other)
        }
        _ => OwnedValue::Integer(1),
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::{
        types::{LimboText, OwnedValue},
        vdbe::insn::exec_or,
    };

    use super::exec_and;

    #[test]
    fn test_exec_and() {
        let inputs = vec![
            (OwnedValue::Integer(0), OwnedValue::Null),
            (OwnedValue::Null, OwnedValue::Integer(1)),
            (OwnedValue::Null, OwnedValue::Null),
            (OwnedValue::Float(0.0), OwnedValue::Null),
            (OwnedValue::Integer(1), OwnedValue::Float(2.2)),
            (
                OwnedValue::Integer(0),
                OwnedValue::Text(LimboText::new(Rc::new("string".to_string()))),
            ),
            (
                OwnedValue::Integer(0),
                OwnedValue::Text(LimboText::new(Rc::new("1".to_string()))),
            ),
            (
                OwnedValue::Integer(1),
                OwnedValue::Text(LimboText::new(Rc::new("1".to_string()))),
            ),
        ];
        let outpus = [
            OwnedValue::Integer(0),
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Integer(0),
            OwnedValue::Integer(1),
            OwnedValue::Integer(0),
            OwnedValue::Integer(0),
            OwnedValue::Integer(1),
        ];

        assert_eq!(
            inputs.len(),
            outpus.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                exec_and(lhs, rhs),
                outpus[i],
                "Wrong AND for lhs: {}, rhs: {}",
                lhs,
                rhs
            );
        }
    }

    #[test]
    fn test_exec_or() {
        let inputs = vec![
            (OwnedValue::Integer(0), OwnedValue::Null),
            (OwnedValue::Null, OwnedValue::Integer(1)),
            (OwnedValue::Null, OwnedValue::Null),
            (OwnedValue::Float(0.0), OwnedValue::Null),
            (OwnedValue::Integer(1), OwnedValue::Float(2.2)),
            (OwnedValue::Float(0.0), OwnedValue::Integer(0)),
            (
                OwnedValue::Integer(0),
                OwnedValue::Text(LimboText::new(Rc::new("string".to_string()))),
            ),
            (
                OwnedValue::Integer(0),
                OwnedValue::Text(LimboText::new(Rc::new("1".to_string()))),
            ),
            (
                OwnedValue::Integer(0),
                OwnedValue::Text(LimboText::new(Rc::new("".to_string()))),
            ),
        ];
        let outpus = [
            OwnedValue::Null,
            OwnedValue::Integer(1),
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Integer(1),
            OwnedValue::Integer(0),
            OwnedValue::Integer(0),
            OwnedValue::Integer(1),
            OwnedValue::Integer(0),
        ];

        assert_eq!(
            inputs.len(),
            outpus.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                exec_or(lhs, rhs),
                outpus[i],
                "Wrong OR for lhs: {}, rhs: {}",
                lhs,
                rhs
            );
        }
    }
}
