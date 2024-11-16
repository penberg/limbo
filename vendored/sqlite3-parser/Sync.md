When some changes happen in the official SQLite repository,
they can be applied locally:
 - $SQLITE/tool/lemon.c => $RLEMON/third_party/lemon.c
 - $SQLITE/tool/lempar.c => $RLEMON/third_party/lempar.rs
 - $SQLITE/tool/mkkeywordhash.c => $RLEMON/src/dialect/mod.rs
 - $SQLITE/src/tokenize.c => $RLEMON/src/lexer/sql/mod.rs
 - $SQLITE/src/parse.y => $RLEMON/src/parser/parse.y (and $RLEMON/src/dialect/token.rs, $RLEMON/src/dialect/mod.rs)
