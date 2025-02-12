/*
 * Copyright (c) 2007 David Crawshaw <david@zentus.com>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package tech.turso.core;

/** Sqlite error codes. */
public class SqliteCode {
  /** Successful result */
  public static final int SQLITE_OK = 0;

  /** SQL error or missing database */
  public static final int SQLITE_ERROR = 1;

  /** An internal logic error in SQLite */
  public static final int SQLITE_INTERNAL = 2;

  /** Access permission denied */
  public static final int SQLITE_PERM = 3;

  /** Callback routine requested an abort */
  public static final int SQLITE_ABORT = 4;

  /** The database file is locked */
  public static final int SQLITE_BUSY = 5;

  /** A table in the database is locked */
  public static final int SQLITE_LOCKED = 6;

  /** A malloc() failed */
  public static final int SQLITE_NOMEM = 7;

  /** Attempt to write a readonly database */
  public static final int SQLITE_READONLY = 8;

  /** Operation terminated by sqlite_interrupt() */
  public static final int SQLITE_INTERRUPT = 9;

  /** Some kind of disk I/O error occurred */
  public static final int SQLITE_IOERR = 10;

  /** The database disk image is malformed */
  public static final int SQLITE_CORRUPT = 11;

  /** (Internal Only) Table or record not found */
  public static final int SQLITE_NOTFOUND = 12;

  /** Insertion failed because database is full */
  public static final int SQLITE_FULL = 13;

  /** Unable to open the database file */
  public static final int SQLITE_CANTOPEN = 14;

  /** Database lock protocol error */
  public static final int SQLITE_PROTOCOL = 15;

  /** (Internal Only) Database table is empty */
  public static final int SQLITE_EMPTY = 16;

  /** The database schema changed */
  public static final int SQLITE_SCHEMA = 17;

  /** Too much data for one row of a table */
  public static final int SQLITE_TOOBIG = 18;

  /** Abort due to constraint violation */
  public static final int SQLITE_CONSTRAINT = 19;

  /** Data type mismatch */
  public static final int SQLITE_MISMATCH = 20;

  /** Library used incorrectly */
  public static final int SQLITE_MISUSE = 21;

  /** Uses OS features not supported on host */
  public static final int SQLITE_NOLFS = 22;

  /** Authorization denied */
  public static final int SQLITE_AUTH = 23;

  /** sqlite_step() has another row ready */
  public static final int SQLITE_ROW = 100;

  /** sqlite_step() has finished executing */
  public static final int SQLITE_DONE = 101;

  // types returned by sqlite3_column_type()

  public static final int SQLITE_INTEGER = 1;
  public static final int SQLITE_FLOAT = 2;
  public static final int SQLITE_TEXT = 3;
  public static final int SQLITE_BLOB = 4;
  public static final int SQLITE_NULL = 5;
}
