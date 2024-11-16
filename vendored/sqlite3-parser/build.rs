use std::env;
use std::fs::File;
use std::io::{BufWriter, Result, Write};
use std::path::Path;
use std::process::Command;

use cc::Build;
use uncased::UncasedStr;

fn main() -> Result<()> {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir);
    let rlemon = out_path.join("rlemon");

    let lemon_src_dir = Path::new("third_party").join("lemon");
    let rlemon_src = lemon_src_dir.join("lemon.c");

    // compile rlemon:
    {
        assert!(Build::new()
            .target(&env::var("HOST").unwrap())
            .get_compiler()
            .to_command()
            .arg("-o")
            .arg(rlemon.clone())
            .arg(rlemon_src)
            .status()?
            .success());
    }

    let sql_parser = "src/parser/parse.y";
    // run rlemon / generate parser:
    {
        assert!(Command::new(rlemon)
            .arg("-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT")
            .arg("-Tthird_party/lemon/lempar.rs")
            .arg(format!("-d{out_dir}"))
            .arg(sql_parser)
            .status()?
            .success());
        // TODO ./rlemon -m -Tthird_party/lemon/lempar.rs examples/simple.y
    }

    let keywords = out_path.join("keywords.rs");
    let mut keywords = BufWriter::new(File::create(keywords)?);
    write!(
        &mut keywords,
        "static KEYWORDS: ::phf::Map<&'static UncasedStr, TokenType> = \n{};",
        phf_codegen::Map::new()
            .entry(UncasedStr::new("ABORT"), "TokenType::TK_ABORT")
            .entry(UncasedStr::new("ACTION"), "TokenType::TK_ACTION")
            .entry(UncasedStr::new("ADD"), "TokenType::TK_ADD")
            .entry(UncasedStr::new("AFTER"), "TokenType::TK_AFTER")
            .entry(UncasedStr::new("ALL"), "TokenType::TK_ALL")
            .entry(UncasedStr::new("ALTER"), "TokenType::TK_ALTER")
            .entry(UncasedStr::new("ALWAYS"), "TokenType::TK_ALWAYS")
            .entry(UncasedStr::new("ANALYZE"), "TokenType::TK_ANALYZE")
            .entry(UncasedStr::new("AND"), "TokenType::TK_AND")
            .entry(UncasedStr::new("AS"), "TokenType::TK_AS")
            .entry(UncasedStr::new("ASC"), "TokenType::TK_ASC")
            .entry(UncasedStr::new("ATTACH"), "TokenType::TK_ATTACH")
            .entry(UncasedStr::new("AUTOINCREMENT"), "TokenType::TK_AUTOINCR")
            .entry(UncasedStr::new("BEFORE"), "TokenType::TK_BEFORE")
            .entry(UncasedStr::new("BEGIN"), "TokenType::TK_BEGIN")
            .entry(UncasedStr::new("BETWEEN"), "TokenType::TK_BETWEEN")
            .entry(UncasedStr::new("BY"), "TokenType::TK_BY")
            .entry(UncasedStr::new("CASCADE"), "TokenType::TK_CASCADE")
            .entry(UncasedStr::new("CASE"), "TokenType::TK_CASE")
            .entry(UncasedStr::new("CAST"), "TokenType::TK_CAST")
            .entry(UncasedStr::new("CHECK"), "TokenType::TK_CHECK")
            .entry(UncasedStr::new("COLLATE"), "TokenType::TK_COLLATE")
            .entry(UncasedStr::new("COLUMN"), "TokenType::TK_COLUMNKW")
            .entry(UncasedStr::new("COMMIT"), "TokenType::TK_COMMIT")
            .entry(UncasedStr::new("CONFLICT"), "TokenType::TK_CONFLICT")
            .entry(UncasedStr::new("CONSTRAINT"), "TokenType::TK_CONSTRAINT")
            .entry(UncasedStr::new("CREATE"), "TokenType::TK_CREATE")
            .entry(UncasedStr::new("CROSS"), "TokenType::TK_JOIN_KW")
            .entry(UncasedStr::new("CURRENT"), "TokenType::TK_CURRENT")
            .entry(UncasedStr::new("CURRENT_DATE"), "TokenType::TK_CTIME_KW")
            .entry(UncasedStr::new("CURRENT_TIME"), "TokenType::TK_CTIME_KW")
            .entry(
                UncasedStr::new("CURRENT_TIMESTAMP"),
                "TokenType::TK_CTIME_KW"
            )
            .entry(UncasedStr::new("DATABASE"), "TokenType::TK_DATABASE")
            .entry(UncasedStr::new("DEFAULT"), "TokenType::TK_DEFAULT")
            .entry(UncasedStr::new("DEFERRABLE"), "TokenType::TK_DEFERRABLE")
            .entry(UncasedStr::new("DEFERRED"), "TokenType::TK_DEFERRED")
            .entry(UncasedStr::new("DELETE"), "TokenType::TK_DELETE")
            .entry(UncasedStr::new("DESC"), "TokenType::TK_DESC")
            .entry(UncasedStr::new("DETACH"), "TokenType::TK_DETACH")
            .entry(UncasedStr::new("DISTINCT"), "TokenType::TK_DISTINCT")
            .entry(UncasedStr::new("DO"), "TokenType::TK_DO")
            .entry(UncasedStr::new("DROP"), "TokenType::TK_DROP")
            .entry(UncasedStr::new("EACH"), "TokenType::TK_EACH")
            .entry(UncasedStr::new("ELSE"), "TokenType::TK_ELSE")
            .entry(UncasedStr::new("END"), "TokenType::TK_END")
            .entry(UncasedStr::new("ESCAPE"), "TokenType::TK_ESCAPE")
            .entry(UncasedStr::new("EXCEPT"), "TokenType::TK_EXCEPT")
            .entry(UncasedStr::new("EXCLUDE"), "TokenType::TK_EXCLUDE")
            .entry(UncasedStr::new("EXCLUSIVE"), "TokenType::TK_EXCLUSIVE")
            .entry(UncasedStr::new("EXISTS"), "TokenType::TK_EXISTS")
            .entry(UncasedStr::new("EXPLAIN"), "TokenType::TK_EXPLAIN")
            .entry(UncasedStr::new("FAIL"), "TokenType::TK_FAIL")
            .entry(UncasedStr::new("FILTER"), "TokenType::TK_FILTER")
            .entry(UncasedStr::new("FIRST"), "TokenType::TK_FIRST")
            .entry(UncasedStr::new("FOLLOWING"), "TokenType::TK_FOLLOWING")
            .entry(UncasedStr::new("FOR"), "TokenType::TK_FOR")
            .entry(UncasedStr::new("FOREIGN"), "TokenType::TK_FOREIGN")
            .entry(UncasedStr::new("FROM"), "TokenType::TK_FROM")
            .entry(UncasedStr::new("FULL"), "TokenType::TK_JOIN_KW")
            .entry(UncasedStr::new("GENERATED"), "TokenType::TK_GENERATED")
            .entry(UncasedStr::new("GLOB"), "TokenType::TK_LIKE_KW")
            .entry(UncasedStr::new("GROUP"), "TokenType::TK_GROUP")
            .entry(UncasedStr::new("GROUPS"), "TokenType::TK_GROUPS")
            .entry(UncasedStr::new("HAVING"), "TokenType::TK_HAVING")
            .entry(UncasedStr::new("IF"), "TokenType::TK_IF")
            .entry(UncasedStr::new("IGNORE"), "TokenType::TK_IGNORE")
            .entry(UncasedStr::new("IMMEDIATE"), "TokenType::TK_IMMEDIATE")
            .entry(UncasedStr::new("IN"), "TokenType::TK_IN")
            .entry(UncasedStr::new("INDEX"), "TokenType::TK_INDEX")
            .entry(UncasedStr::new("INDEXED"), "TokenType::TK_INDEXED")
            .entry(UncasedStr::new("INITIALLY"), "TokenType::TK_INITIALLY")
            .entry(UncasedStr::new("INNER"), "TokenType::TK_JOIN_KW")
            .entry(UncasedStr::new("INSERT"), "TokenType::TK_INSERT")
            .entry(UncasedStr::new("INSTEAD"), "TokenType::TK_INSTEAD")
            .entry(UncasedStr::new("INTERSECT"), "TokenType::TK_INTERSECT")
            .entry(UncasedStr::new("INTO"), "TokenType::TK_INTO")
            .entry(UncasedStr::new("IS"), "TokenType::TK_IS")
            .entry(UncasedStr::new("ISNULL"), "TokenType::TK_ISNULL")
            .entry(UncasedStr::new("JOIN"), "TokenType::TK_JOIN")
            .entry(UncasedStr::new("KEY"), "TokenType::TK_KEY")
            .entry(UncasedStr::new("LAST"), "TokenType::TK_LAST")
            .entry(UncasedStr::new("LEFT"), "TokenType::TK_JOIN_KW")
            .entry(UncasedStr::new("LIKE"), "TokenType::TK_LIKE_KW")
            .entry(UncasedStr::new("LIMIT"), "TokenType::TK_LIMIT")
            .entry(UncasedStr::new("MATCH"), "TokenType::TK_MATCH")
            .entry(
                UncasedStr::new("MATERIALIZED"),
                "TokenType::TK_MATERIALIZED"
            )
            .entry(UncasedStr::new("NATURAL"), "TokenType::TK_JOIN_KW")
            .entry(UncasedStr::new("NO"), "TokenType::TK_NO")
            .entry(UncasedStr::new("NOT"), "TokenType::TK_NOT")
            .entry(UncasedStr::new("NOTHING"), "TokenType::TK_NOTHING")
            .entry(UncasedStr::new("NOTNULL"), "TokenType::TK_NOTNULL")
            .entry(UncasedStr::new("NULL"), "TokenType::TK_NULL")
            .entry(UncasedStr::new("NULLS"), "TokenType::TK_NULLS")
            .entry(UncasedStr::new("OF"), "TokenType::TK_OF")
            .entry(UncasedStr::new("OFFSET"), "TokenType::TK_OFFSET")
            .entry(UncasedStr::new("ON"), "TokenType::TK_ON")
            .entry(UncasedStr::new("OR"), "TokenType::TK_OR")
            .entry(UncasedStr::new("ORDER"), "TokenType::TK_ORDER")
            .entry(UncasedStr::new("OTHERS"), "TokenType::TK_OTHERS")
            .entry(UncasedStr::new("OUTER"), "TokenType::TK_JOIN_KW")
            .entry(UncasedStr::new("OVER"), "TokenType::TK_OVER")
            .entry(UncasedStr::new("PARTITION"), "TokenType::TK_PARTITION")
            .entry(UncasedStr::new("PLAN"), "TokenType::TK_PLAN")
            .entry(UncasedStr::new("PRAGMA"), "TokenType::TK_PRAGMA")
            .entry(UncasedStr::new("PRECEDING"), "TokenType::TK_PRECEDING")
            .entry(UncasedStr::new("PRIMARY"), "TokenType::TK_PRIMARY")
            .entry(UncasedStr::new("QUERY"), "TokenType::TK_QUERY")
            .entry(UncasedStr::new("RAISE"), "TokenType::TK_RAISE")
            .entry(UncasedStr::new("RANGE"), "TokenType::TK_RANGE")
            .entry(UncasedStr::new("RECURSIVE"), "TokenType::TK_RECURSIVE")
            .entry(UncasedStr::new("REFERENCES"), "TokenType::TK_REFERENCES")
            .entry(UncasedStr::new("REGEXP"), "TokenType::TK_LIKE_KW")
            .entry(UncasedStr::new("REINDEX"), "TokenType::TK_REINDEX")
            .entry(UncasedStr::new("RELEASE"), "TokenType::TK_RELEASE")
            .entry(UncasedStr::new("RENAME"), "TokenType::TK_RENAME")
            .entry(UncasedStr::new("REPLACE"), "TokenType::TK_REPLACE")
            .entry(UncasedStr::new("RETURNING"), "TokenType::TK_RETURNING")
            .entry(UncasedStr::new("RESTRICT"), "TokenType::TK_RESTRICT")
            .entry(UncasedStr::new("RIGHT"), "TokenType::TK_JOIN_KW")
            .entry(UncasedStr::new("ROLLBACK"), "TokenType::TK_ROLLBACK")
            .entry(UncasedStr::new("ROW"), "TokenType::TK_ROW")
            .entry(UncasedStr::new("ROWS"), "TokenType::TK_ROWS")
            .entry(UncasedStr::new("SAVEPOINT"), "TokenType::TK_SAVEPOINT")
            .entry(UncasedStr::new("SELECT"), "TokenType::TK_SELECT")
            .entry(UncasedStr::new("SET"), "TokenType::TK_SET")
            .entry(UncasedStr::new("TABLE"), "TokenType::TK_TABLE")
            .entry(UncasedStr::new("TEMP"), "TokenType::TK_TEMP")
            .entry(UncasedStr::new("TEMPORARY"), "TokenType::TK_TEMP")
            .entry(UncasedStr::new("THEN"), "TokenType::TK_THEN")
            .entry(UncasedStr::new("TIES"), "TokenType::TK_TIES")
            .entry(UncasedStr::new("TO"), "TokenType::TK_TO")
            .entry(UncasedStr::new("TRANSACTION"), "TokenType::TK_TRANSACTION")
            .entry(UncasedStr::new("TRIGGER"), "TokenType::TK_TRIGGER")
            .entry(UncasedStr::new("UNBOUNDED"), "TokenType::TK_UNBOUNDED")
            .entry(UncasedStr::new("UNION"), "TokenType::TK_UNION")
            .entry(UncasedStr::new("UNIQUE"), "TokenType::TK_UNIQUE")
            .entry(UncasedStr::new("UPDATE"), "TokenType::TK_UPDATE")
            .entry(UncasedStr::new("USING"), "TokenType::TK_USING")
            .entry(UncasedStr::new("VACUUM"), "TokenType::TK_VACUUM")
            .entry(UncasedStr::new("VALUES"), "TokenType::TK_VALUES")
            .entry(UncasedStr::new("VIEW"), "TokenType::TK_VIEW")
            .entry(UncasedStr::new("VIRTUAL"), "TokenType::TK_VIRTUAL")
            .entry(UncasedStr::new("WHEN"), "TokenType::TK_WHEN")
            .entry(UncasedStr::new("WHERE"), "TokenType::TK_WHERE")
            .entry(UncasedStr::new("WINDOW"), "TokenType::TK_WINDOW")
            .entry(UncasedStr::new("WITH"), "TokenType::TK_WITH")
            .entry(UncasedStr::new("WITHOUT"), "TokenType::TK_WITHOUT")
            .build()
    )?;

    println!("cargo:rerun-if-changed=third_party/lemon/lemon.c");
    println!("cargo:rerun-if-changed=third_party/lemon/lempar.rs");
    println!("cargo:rerun-if-changed=src/parser/parse.y");
    // TODO examples/simple.y if test
    Ok(())
}
