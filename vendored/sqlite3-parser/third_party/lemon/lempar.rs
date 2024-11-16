/*
** 2000-05-29
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Driver template for the LEMON parser generator.
**
** The "lemon" program processes an LALR(1) input grammar file, then uses
** this template to construct a parser.  The "lemon" program inserts text
** at each "%%" line.  Also, any "P-a-r-s-e" identifier prefix (without the
** interstitial "-" characters) contained in this template is changed into
** the value of the %name directive from the grammar.  Otherwise, the content
** of this template is copied straight through into the generate parser
** source file.
**
** The following is the concatenation of all %include directives from the
** input grammar file:
*/
/************ Begin %include sections from the grammar ************************/
%%
/**************** End of %include directives **********************************/
/* These constants specify the various numeric values for terminal symbols.
***************** Begin token definitions *************************************/
%%
/**************** End token definitions ***************************************/

/* The next sections is a series of control #defines.
** various aspects of the generated parser.
**    YYCODETYPE         is the data type used to store the integer codes
**                       that represent terminal and non-terminal symbols.
**                       "unsigned char" is used if there are fewer than
**                       256 symbols.  Larger types otherwise.
**    YYNOCODE           is a number of type YYCODETYPE that is not used for
**                       any terminal or nonterminal symbol.
**    YYFALLBACK         If defined, this indicates that one or more tokens
**                       (also known as: "terminal symbols") have fall-back
**                       values which should be used if the original symbol
**                       would not parse.  This permits keywords to sometimes
**                       be used as identifiers, for example.
**    YYACTIONTYPE       is the data type used for "action codes" - numbers
**                       that indicate what to do in response to the next
**                       token.
**    ParseTOKENTYPE     is the data type used for minor type for terminal
**                       symbols.  Background: A "minor type" is a semantic
**                       value associated with a terminal or non-terminal
**                       symbols.  For example, for an "ID" terminal symbol,
**                       the minor type might be the name of the identifier.
**                       Each non-terminal can have a different minor type.
**                       Terminal symbols all have the same minor type, though.
**                       This macros defines the minor type for terminal
**                       symbols.
**    YYMINORTYPE        is the data type used for all minor types.
**                       This is typically a union of many types, one of
**                       which is ParseTOKENTYPE.  The entry in the union
**                       for terminal symbols is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    YYERRORSYMBOL      is the code number of the error symbol.  If not
**                       defined, then do no error processing.
**    YYNSTATE           the combined number of states.
**    YYNRULE            the number of rules in the grammar
**    YYNTOKEN           Number of terminal symbols
**    YY_MAX_SHIFT       Maximum value for shift actions
**    YY_MIN_SHIFTREDUCE Minimum value for shift-reduce actions
**    YY_MAX_SHIFTREDUCE Maximum value for shift-reduce actions
**    YY_ERROR_ACTION    The yy_action[] code for syntax error
**    YY_ACCEPT_ACTION   The yy_action[] code for accept
**    YY_NO_ACTION       The yy_action[] code for no-op
**    YY_MIN_REDUCE      Minimum value for reduce actions
**    YY_MAX_REDUCE      Maximum value for reduce actions
*/
/************* Begin control #defines *****************************************/
%%
/************* End control #defines *******************************************/

/* Next are the tables used to determine what action to take based on the
** current state and lookahead token.  These tables are used to implement
** functions that take a state number and lookahead value and return an
** action integer.
**
** Suppose the action integer is N.  Then the action is determined as
** follows
**
**   0 <= N <= YY_MAX_SHIFT             Shift N.  That is, push the lookahead
**                                      token onto the stack and goto state N.
**
**   N between YY_MIN_SHIFTREDUCE       Shift to an arbitrary state then
**     and YY_MAX_SHIFTREDUCE           reduce by rule N-YY_MIN_SHIFTREDUCE.
**
**   N == YY_ERROR_ACTION               A syntax error has occurred.
**
**   N == YY_ACCEPT_ACTION              The parser accepts its input.
**
**   N == YY_NO_ACTION                  No such action.  Denotes unused
**                                      slots in the yy_action[] table.
**
**   N between YY_MIN_REDUCE            Reduce by rule N-YY_MIN_REDUCE
**     and YY_MAX_REDUCE
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as either:
**
**    (A)   N = yy_action[ yy_shift_ofst[S] + X ]
**    (B)   N = yy_default[S]
**
** The (A) formula is preferred.  The B formula is used instead if
** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X.
**
** The formulas above are for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array.
**
** The following are the tables generated in this section:
**
**  yy_action[]        A single table containing all actions.
**  yy_lookahead[]     A table containing the lookahead for each entry in
**                     yy_action.  Used to detect hash collisions.
**  yy_shift_ofst[]    For each state, the offset into yy_action for
**                     shifting terminals.
**  yy_reduce_ofst[]   For each state, the offset into yy_action for
**                     shifting non-terminals after a reduce.
**  yy_default[]       Default action for each state.
**
*********** Begin parsing tables **********************************************/
%%
/********** End of lemon-generated parsing tables *****************************/

/* The next table maps tokens (terminal symbols) into fallback tokens.
** If a construct like the following:
**
**      %fallback ID X Y Z.
**
** appears in the grammar, then ID becomes a fallback token for X, Y,
** and Z.  Whenever one of the tokens X, Y, or Z is input to the parser
** but it does not parse, the type of the token is changed to ID and
** the parse is retried before an error is thrown.
**
** This feature can be used, for example, to cause some keywords in a language
** to revert to identifiers if they keyword does not apply in the context where
** it appears.
*/
%%

/* The following structure represents a single element of the
** parser's stack.  Information stored includes:
**
**   +  The state number for the parser at this level of the stack.
**
**   +  The value of the token stored at this level of the stack.
**      (In other words, the "major" token.)
**
**   +  The semantic value stored at this level of the stack.  This is
**      the information used by the action routines in the grammar.
**      It is sometimes called the "minor" token.
**
** After the "shift" half of a SHIFTREDUCE action, the stateno field
** actually contains the reduce action for the second half of the
** SHIFTREDUCE.
*/
#[expect(non_camel_case_types)]
#[derive(Default)]
pub struct yyStackEntry<'i> {
    stateno: YYACTIONTYPE, /* The state-number, or reduce action in SHIFTREDUCE */
    major: YYCODETYPE,     /* The major token value.  This is the code
                            ** number for the token at this stack level */
    minor: YYMINORTYPE<'i>, /* The user-supplied minor token value.  This
                         ** is the value of the token  */
}

/* The state of the parser is completely contained in an instance of
** the following structure */
#[expect(non_camel_case_types)]
pub struct yyParser<'input> {
    yyidx: usize, /* Index to top element of the stack */
    #[cfg(feature = "YYTRACKMAXSTACKDEPTH")]
    yyhwm: usize, /* High-water mark of the stack */
    //#[cfg(not(feature = "YYNOERRORRECOVERY"))]
    yyerrcnt: i32, /* Shifts left before out of the error */
%%                               /* A place to hold %extra_context */
    yystack: Vec<yyStackEntry<'input>>, /* The parser's stack */
}

use std::cmp::Ordering;
use std::ops::Neg;
impl<'input> yyParser<'input> {
    fn shift(&self, shift: i8) -> usize {
        assert!(shift <= 1);
        match shift.cmp(&0) {
            Ordering::Equal => self.yyidx,
            Ordering::Greater => self.yyidx + shift as usize,
            Ordering::Less => self.yyidx.checked_sub(shift.neg() as usize).unwrap(),
        }
    }

    fn yyidx_shift(&mut self, shift: i8) {
        match shift.cmp(&0) {
            Ordering::Greater => self.yyidx += shift as usize,
            Ordering::Less => self.yyidx = self.yyidx.checked_sub(shift.neg() as usize).unwrap(),
            Ordering::Equal => {}
        }
    }

    fn yy_move(&mut self, shift: i8) -> yyStackEntry<'input> {
        use std::mem::take;
        let idx = self.shift(shift);
        take(&mut self.yystack[idx])
    }

    fn push(&mut self, entry: yyStackEntry<'input>) {
        if self.yyidx == self.yystack.len() {
            self.yystack.push(entry);
        } else {
            self.yystack[self.yyidx] = entry;
        }
    }
}

use std::ops::{Index, IndexMut};
impl<'input> Index<i8> for yyParser<'input> {
    type Output = yyStackEntry<'input>;

    fn index(&self, shift: i8) -> &yyStackEntry<'input> {
        let idx = self.shift(shift);
        &self.yystack[idx]
    }
}
impl<'input> IndexMut<i8> for yyParser<'input> {
    fn index_mut(&mut self, shift: i8) -> &mut yyStackEntry<'input> {
        let idx = self.shift(shift);
        &mut self.yystack[idx]
    }
}

#[cfg(not(feature = "NDEBUG"))]
use log::{debug, log_enabled, Level::Debug};
static TARGET: &str = "Parse";

/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
#[cfg(any(feature = "YYCOVERAGE", not(feature = "NDEBUG")))]
%%

/* For tracing reduce actions, the names of all rules are required.
*/
#[cfg(not(feature = "NDEBUG"))]
#[rustfmt::skip]
#[expect(non_upper_case_globals)]
static yyRuleName: [&str; YYNRULE] = [
%%
];

/*
** Try to increase the size of the parser stack.  Return the number
** of errors.  Return 0 on success.
*/
impl yyParser<'_> {
    fn yy_grow_stack_if_needed(&mut self) -> bool {
        false
    }
    fn yy_grow_stack_for_push(&mut self) -> bool {
        // yystack is not prefilled with zero value like in C.
        if self.yyidx == self.yystack.len() {
            self.yystack.push(yyStackEntry::default());
        } else if self.yyidx + 1 == self.yystack.len() {
            self.yystack.push(yyStackEntry::default());
        }
        false
    }
}

/* Initialize a new parser.
*/
impl yyParser<'_> {
    pub fn new(
%%               /* Optional %extra_context parameter */
    ) -> yyParser {
        let mut p = yyParser {
            yyidx: 0,
            #[cfg(feature = "YYTRACKMAXSTACKDEPTH")]
            yyhwm: 0,
            yystack: Vec::new(),
            //#[cfg(not(feature = "YYNOERRORRECOVERY"))]
            yyerrcnt: -1,
%%               /* Optional %extra_context store */
        };
        p.push(yyStackEntry::default());
        p
    }
}

/*
** Pop the parser's stack once.
*/
impl yyParser<'_> {
    fn yy_pop_parser_stack(&mut self) {
        use std::mem::take;
        let _yytos = take(&mut self.yystack[self.yyidx]);
        self.yyidx = self.yyidx.checked_sub(1).unwrap();
        //assert_eq!(self.yyidx+1, self.yystack.len());
        #[cfg(not(feature = "NDEBUG"))]
        {
            debug!(
                target: TARGET,
                "Popping {}", yyTokenName[_yytos.major as usize]
            );
        }
    }
}

/*
** Clear all secondary memory allocations from the parser
*/
impl yyParser<'_> {
    #[expect(non_snake_case)]
    pub fn ParseFinalize(&mut self) {
        while self.yyidx > 0 {
            self.yy_pop_parser_stack();
        }
        // TODO check all elements remaining in yystack are yyinit()
    }
}

/*
** Return the peak depth of the stack for a parser.
*/
#[cfg(feature = "YYTRACKMAXSTACKDEPTH")]
impl yyParser<'_> {
    #[expect(non_snake_case)]
    pub fn ParseStackPeak(&self) -> usize {
        self.yyhwm
    }
    fn yyhwm_incr(&mut self) {
        if self.yyidx > self.yyhwm {
            self.yyhwm += 1;
            assert_eq!(self.yyhwm, self.yyidx);
        }
    }
}
#[cfg(not(feature = "YYTRACKMAXSTACKDEPTH"))]
impl yyParser<'_> {
    #[inline]
    fn yyhwm_incr(&mut self) {}
}

/* This array of booleans keeps track of the parser statement
** coverage.  The element yycoverage[X][Y] is set when the parser
** is in state X and has a lookahead token Y.  In a well-tested
** systems, every element of this matrix should end up being set.
*/
#[cfg(feature = "YYCOVERAGE")]
static yycoverage: [[bool; YYNTOKEN]; YYNSTATE] = [];

/*
** Write into out a description of every state/lookahead combination that
**
**   (1)  has not been used by the parser, and
**   (2)  is not a syntax error.
**
** Return the number of missed state/lookahead combinations.
*/
#[cfg(feature = "YYCOVERAGE")]
fn ParseCoverage(/*FILE *out*/) -> i32 {
    //int stateno, iLookAhead, i;
    let mut nMissed = 0;
    /*for(stateno=0; stateno<YYNSTATE; stateno++){
      i = yy_shift_ofst[stateno];
      for(iLookAhead=0; iLookAhead<YYNTOKEN; iLookAhead++){
        if( yy_lookahead[i+iLookAhead]!=iLookAhead ) continue;
        if( yycoverage[stateno][iLookAhead]==0 ) nMissed++;
        if( out ){
          fprintf(out,"State %d lookahead %s %s\n", stateno,
                  yyTokenName[iLookAhead],
                  yycoverage[stateno][iLookAhead] ? "ok" : "missed");
        }
      }
    }*/
    return nMissed;
}

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
*/
#[expect(non_snake_case)]
fn yy_find_shift_action(
    mut iLookAhead: YYCODETYPE, /* The look-ahead token */
    stateno: YYACTIONTYPE,      /* Current state number */
) -> YYACTIONTYPE {
    if stateno > YY_MAX_SHIFT {
        return stateno;
    }
    assert!(stateno <= YY_SHIFT_COUNT);
    #[cfg(feature = "YYCOVERAGE")]
    {
        //yycoverage[stateno][iLookAhead] = true;
    }
    loop {
        let mut i = yy_shift_ofst[stateno as usize] as usize;
        assert!(i <= YY_ACTTAB_COUNT!());
        assert!(i + usize::from(YYNTOKEN) <= yy_lookahead.len());
        assert_ne!(iLookAhead, YYNOCODE);
        assert!((iLookAhead as YYACTIONTYPE) < YYNTOKEN);
        i += iLookAhead as usize;
        if yy_lookahead[i] != iLookAhead {
            if YYFALLBACK {
                let iFallback = yyFallback[iLookAhead as usize]; /* Fallback token */
                if iFallback != 0 {
                    #[cfg(not(feature = "NDEBUG"))]
                    {
                        debug!(
                            target: TARGET,
                            "FALLBACK {} => {}",
                            yyTokenName[iLookAhead as usize],
                            yyTokenName[iFallback as usize]
                        );
                    }
                    assert_eq!(yyFallback[iFallback as usize], 0); /* Fallback loop must terminate */
                    iLookAhead = iFallback;
                    continue;
                }
            }
            if YYWILDCARD > 0 {
                let j = i - iLookAhead as usize + YYWILDCARD as usize;
                if yy_lookahead[j] == YYWILDCARD && iLookAhead > 0 {
                    #[cfg(not(feature = "NDEBUG"))]
                    {
                        debug!(
                            target: TARGET,
                            "WILDCARD {} => {}",
                            yyTokenName[iLookAhead as usize],
                            yyTokenName[YYWILDCARD as usize]
                        );
                    }
                    return yy_action[j];
                }
            } /* YYWILDCARD */
            return yy_default[stateno as usize];
        } else {
            return yy_action[i];
        }
    }
}

/*
** Find the appropriate action for a parser given the non-terminal
** look-ahead token iLookAhead.
*/
#[expect(non_snake_case)]
fn yy_find_reduce_action(
    stateno: YYACTIONTYPE,  /* Current state number */
    iLookAhead: YYCODETYPE, /* The look-ahead token */
) -> YYACTIONTYPE {
    if YYERRORSYMBOL > 0 {
        if stateno > YY_REDUCE_COUNT {
            return yy_default[stateno as usize];
        }
    } else {
        assert!(stateno <= YY_REDUCE_COUNT);
    }
    let mut i: i32 = yy_reduce_ofst[stateno as usize].into();
    assert_ne!(iLookAhead, YYNOCODE);
    i += i32::from(iLookAhead);
    if YYERRORSYMBOL > 0 {
        if !(0..YY_ACTTAB_COUNT!()).contains(&i) || yy_lookahead[i as usize] != iLookAhead {
            return yy_default[stateno as usize];
        }
    } else {
        assert!((0..YY_ACTTAB_COUNT!()).contains(&i));
        assert_eq!(yy_lookahead[i as usize], iLookAhead);
    }
    yy_action[i as usize]
}

        /******** Begin %stack_overflow code ******************************************
%%
        ******** End %stack_overflow code ********************************************/

/*
** Print tracing information for a SHIFT action
*/
impl yyParser<'_> {
    #[expect(non_snake_case)]
    #[cfg(feature = "NDEBUG")]
    fn yyTraceShift(&self, _: YYACTIONTYPE, _: &str) {
    }
    #[expect(non_snake_case)]
    #[cfg(not(feature = "NDEBUG"))]
    fn yyTraceShift(&self, yyNewState: YYACTIONTYPE, zTag: &str) {
        let yytos = &self[0];
        if yyNewState < YYNSTATE {
            debug!(
                target: TARGET,
                "{} '{}', go to state {}", zTag, yyTokenName[yytos.major as usize], yyNewState
            );
        } else {
            debug!(
                target: TARGET,
                "{} '{}', pending reduce {:?}",
                zTag,
                yyTokenName[yytos.major as usize],
                yyNewState.checked_sub(YY_MIN_REDUCE)
            );
        }
    }
}

/*
** Perform a shift action.
*/
impl<'input> yyParser<'input> {
    #[expect(non_snake_case)]
    fn yy_shift(
        &mut self,
        mut yyNewState: YYACTIONTYPE,    /* The new state to shift in */
        yyMajor: YYCODETYPE,             /* The major token to shift in */
        yyMinor: ParseTOKENTYPE<'input>, /* The minor token to shift in */
    ) {
        self.yyidx_shift(1);
        self.yyhwm_incr();
        if self.yy_grow_stack_if_needed() {
            return;
        }
        if yyNewState > YY_MAX_SHIFT {
            yyNewState += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
        }
        let yytos = yyStackEntry {
            stateno: yyNewState,
            major: yyMajor,
            minor: YYMINORTYPE::yy0(yyMinor),
        };
        self.push(yytos);
        self.yyTraceShift(yyNewState, "Shift");
    }
}

/* For rule J, yyRuleInfoLhs[J] contains the symbol on the left-hand side
** of that rule */
#[expect(non_upper_case_globals)]
static yyRuleInfoLhs: [YYCODETYPE; YYNRULE] = [
%%
];

/* For rule J, yyRuleInfoNRhs[J] contains the negative of the number
** of symbols on the right-hand side of that rule. */
#[expect(non_upper_case_globals)]
static yyRuleInfoNRhs: [i8; YYNRULE] = [
%%
];

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
**
** The yyLookahead and yyLookaheadToken parameters provide reduce actions
** access to the lookahead token (if any).  The yyLookahead will be YYNOCODE
** if the lookahead token has already been consumed.  As this procedure is
** only called from one place, optimizing compilers will in-line it, which
** means that the extra parameters have no performance impact.
*/
impl yyParser<'_> {
    fn yy_reduce(
        &mut self,
        yyruleno: YYACTIONTYPE,    /* Number of the rule by which to reduce */
        yy_look_ahead: YYCODETYPE, /* Lookahead token, or YYNOCODE if none */
        yy_lookahead_token: &ParseTOKENTYPE, /* Value of the lookahead token */
    ) -> Result<YYACTIONTYPE, ParseError> {
        let _ = yy_look_ahead;
        let _ = yy_lookahead_token;

        let yylhsminor: YYMINORTYPE<'_>;
        match yyruleno {
  /* Beginning here are the reduction cases.  A typical example
  ** follows:
  **   case 0:
  **  #line <lineno> <grammarfile>
  **     { ... }           // User supplied code
  **  #line <lineno> <thisfile>
  **     break;
  */
/********** Begin reduce actions **********************************************/
%%
/********** End reduce actions ************************************************/
        };
        let yygoto: YYCODETYPE = yyRuleInfoLhs[yyruleno as usize]; /* The next state */
        let yysize: i8 = yyRuleInfoNRhs[yyruleno as usize];  /* Amount to pop the stack */
        let yyact: YYACTIONTYPE = yy_find_reduce_action(self[yysize].stateno, yygoto); /* The next action */

        /* There are no SHIFTREDUCE actions on nonterminals because the table
         ** generator has simplified them to pure REDUCE actions. */
        assert!(!(yyact > YY_MAX_SHIFT && yyact <= YY_MAX_SHIFTREDUCE));

        /* It is not possible for a REDUCE to be followed by an error */
        assert_ne!(yyact, YY_ERROR_ACTION);

        self.yyidx_shift(yysize + 1);
        {
            let yymsp = &mut self[0];
            yymsp.stateno = yyact;
            yymsp.major = yygoto;
        }
        self.yyTraceShift(yyact, "... then shift");
        Ok(yyact)
    }
}

/*
** The following code executes when the parse fails
*/
impl yyParser<'_> {
    #[cfg(not(feature = "YYNOERRORRECOVERY"))]
    fn yy_parse_failed(&mut self) {
        #[cfg(not(feature = "NDEBUG"))]
        {
            error!(target: TARGET, "Fail!");
        }
        while self.yyidx > 0 {
            self.yy_pop_parser_stack();
        }
        /* Here code is inserted which will be executed whenever the
         ** parser fails */
        /************ Begin %parse_failure code ***************************************/
%%
        /************ End %parse_failure code *****************************************/
    }
    #[cfg(feature = "YYNOERRORRECOVERY")]
    fn yy_parse_failed(&mut self) {}
}

/*
** The following code executes when a syntax error first occurs.
*/
impl yyParser<'_> {
    fn yy_syntax_error(
        &mut self,
        yymajor: YYCODETYPE,              /* The major type of the error token */
        yyminor: &ParseTOKENTYPE, /* The minor type of the error token */
    ) {
        /************ Begin %syntax_error code ****************************************/
%%
        /************ End %syntax_error code ******************************************/
    }
}

/*
** The following is executed when the parser accepts
*/
impl yyParser<'_> {
    fn yy_accept(&mut self) {
        #[cfg(not(feature = "NDEBUG"))]
        {
            debug!(target: TARGET, "Accept!");
        }
        if cfg!(not(feature = "YYNOERRORRECOVERY")) {
            self.yyerrcnt = -1;
        }
        assert_eq!(self.yyidx, 0);
        /* Here code is inserted which will be executed whenever the
         ** parser accepts */
        /*********** Begin %parse_accept code *****************************************/
%%
        /*********** End %parse_accept code *******************************************/
    }
}

/* The main parser program.
** The first argument is a pointer to a structure obtained from
** "ParseAlloc" which describes the current state of the parser.
** The second argument is the major token number.  The third is
** the minor token.  The fourth optional argument is whatever the
** user wants (and specified in the grammar) and is available for
** use by the action routines.
**
** Inputs:
** <ul>
** <li> A pointer to the parser (an opaque structure.)
** <li> The major token number.
** <li> The minor token number.
** <li> An option argument of a grammar-specified type.
** </ul>
**
** Outputs:
** None.
*/
impl<'input> yyParser<'input> {
    #[expect(non_snake_case)]
    pub fn Parse(
        &mut self,
        yymajor: TokenType,                  /* The major token code number */
        yyminor: ParseTOKENTYPE<'input>, /* The value for the token */
    ) -> Result<(), ParseError> {
        let mut yymajor = yymajor as YYCODETYPE;
        //#[cfg(all(not(feature = "YYERRORSYMBOL"), not(feature = "YYNOERRORRECOVERY")))]
        let mut yyendofinput: bool = false; /* True if we are at the end of input */
        //#[cfg(feature = "YYERRORSYMBOL")]
        let mut yyerrorhit: bool = false; /* True if yymajor has invoked an error */

        //assert_ne!( self[0], null );
        if YYERRORSYMBOL == 0 && cfg!(not(feature = "YYNOERRORRECOVERY")) {
            yyendofinput = yymajor == 0;
        }

        let mut yyact: YYACTIONTYPE = self[0].stateno; /* The parser action. */
        #[cfg(not(feature = "NDEBUG"))]
        {
            if yyact < YY_MIN_REDUCE {
                debug!(
                    target: TARGET,
                    "Input '{}' in state {}", yyTokenName[yymajor as usize], yyact
                );
            } else {
                debug!(
                    target: TARGET,
                    "Input '{}' with pending reduce {}",
                    yyTokenName[yymajor as usize],
                    yyact - YY_MIN_REDUCE
                );
            }
        }

        loop {
            assert_eq!(yyact, self[0].stateno);
            yyact = yy_find_shift_action(yymajor, yyact);
            if yyact >= YY_MIN_REDUCE {
                let yyruleno = yyact - YY_MIN_REDUCE; /* Reduce by this rule */
                #[cfg(not(feature = "NDEBUG"))]
                    {
                        assert!((yyruleno as usize) < yyRuleName.len());
                        let yysize = yyRuleInfoNRhs[yyruleno as usize];
                        let action = if yyruleno < YYNRULE_WITH_ACTION {
                            ""
                        } else {
                            " without external action"
                        };
                        if yysize != 0 {
                            debug!(
                                target: TARGET,
                                "Reduce {} [{}]{}, pop back to state {}.",
                                yyruleno,
                                yyRuleName[yyruleno as usize],
                                action,
                                self[yysize].stateno
                            );
                        } else {
                            debug!(
                                target: TARGET,
                                "Reduce {} [{}]{}.", yyruleno, yyRuleName[yyruleno as usize], action
                            );
                        }
                    }
                /* Check that the stack is large enough to grow by a single entry
                 ** if the RHS of the rule is empty.  This ensures that there is room
                 ** enough on the stack to push the LHS value */
                if yyRuleInfoNRhs[yyruleno as usize] == 0 {
                    self.yyhwm_incr();
                    if self.yy_grow_stack_for_push() {
                        break;
                    }
                }
                yyact = self.yy_reduce(yyruleno, yymajor, &yyminor)?;
            } else if yyact <= YY_MAX_SHIFTREDUCE {
                self.yy_shift(yyact, yymajor, yyminor);
                if cfg!(not(feature = "YYNOERRORRECOVERY")) {
                    self.yyerrcnt -= 1;
                }
                break;
            } else if yyact == YY_ACCEPT_ACTION {
                self.yyidx_shift(-1);
                self.yy_accept();
                return Ok(());
            } else {
                assert_eq!(yyact, YY_ERROR_ACTION);
                #[cfg(not(feature = "NDEBUG"))]
                {
                    debug!(target: TARGET, "Syntax Error!");
                }
                if YYERRORSYMBOL > 0 {
                    /* A syntax error has occurred.
                     ** The response to an error depends upon whether or not the
                     ** grammar defines an error token "ERROR".
                     **
                     ** This is what we do if the grammar does define ERROR:
                     **
                     **  * Call the %syntax_error function.
                     **
                     **  * Begin popping the stack until we enter a state where
                     **    it is legal to shift the error symbol, then shift
                     **    the error symbol.
                     **
                     **  * Set the error count to three.
                     **
                     **  * Begin accepting and shifting new tokens.  No new error
                     **    processing will occur until three tokens have been
                     **    shifted successfully.
                     **
                     */
                    if self.yyerrcnt < 0 {
                        self.yy_syntax_error(yymajor, &yyminor);
                    }
                    let yymx = self[0].major;
                    if yymx == YYERRORSYMBOL || yyerrorhit {
                        #[cfg(not(feature = "NDEBUG"))]
                        {
                            debug!(
                                target: TARGET,
                                "Discard input token {}", yyTokenName[yymajor as usize]
                            );
                        }
                        yymajor = YYNOCODE;
                    } else {
                        while self.yyidx > 0 {
                            yyact = yy_find_reduce_action(self[0].stateno, YYERRORSYMBOL);
                            if yyact <= YY_MAX_SHIFTREDUCE {
                                break;
                            }
                            self.yy_pop_parser_stack();
                        }
                        if self.yyidx <= 0 || yymajor == 0 {
                            self.yy_parse_failed();
                            if cfg!(not(feature = "YYNOERRORRECOVERY")) {
                                self.yyerrcnt = -1;
                            }
                            yymajor = YYNOCODE;
                        } else if yymx != YYERRORSYMBOL {
                            self.yy_shift(yyact, YYERRORSYMBOL, yyminor);
                        }
                    }
                    self.yyerrcnt = 3;
                    yyerrorhit = true;
                    if yymajor == YYNOCODE {
                        break;
                    }
                    yyact = self[0].stateno;
                } else if cfg!(feature = "YYNOERRORRECOVERY") {
                    /* If the YYNOERRORRECOVERY macro is defined, then do not attempt to
                     ** do any kind of error recovery.  Instead, simply invoke the syntax
                     ** error routine and continue going as if nothing had happened.
                     **
                     ** Applications can set this macro (for example inside %include) if
                     ** they intend to abandon the parse upon the first syntax error seen.
                     */
                    self.yy_syntax_error(yymajor, &yyminor);
                    break;
                } else {
                    /* YYERRORSYMBOL is not defined */
                    /* This is what we do if the grammar does not define ERROR:
                     **
                     **  * Report an error message, and throw away the input token.
                     **
                     **  * If the input token is $, then fail the parse.
                     **
                     ** As before, subsequent error messages are suppressed until
                     ** three input tokens have been successfully shifted.
                     */
                    if self.yyerrcnt <= 0 {
                        self.yy_syntax_error(yymajor, &yyminor);
                    }
                    self.yyerrcnt = 3;
                    if yyendofinput {
                        self.yy_parse_failed();
                        if cfg!(not(feature = "YYNOERRORRECOVERY")) {
                            self.yyerrcnt = -1;
                        }
                    }
                    break;
                }
            }
            if self.yyidx <= 0 {
                break;
            }
        }
        #[cfg(not(feature = "NDEBUG"))]
        {
            if log_enabled!(target: TARGET, Debug) {
                let msg = self.yystack[1..=self.yyidx]
                    .iter()
                    .map(|entry| yyTokenName[entry.major as usize])
                    .collect::<Vec<&str>>()
                    .join(" ");
                debug!(target: TARGET, "Return. Stack=[{}]", msg);
            }
        }
        return Ok(());
    }

    /*
     ** Return the fallback token corresponding to canonical token iToken, or
     ** 0 if iToken has no fallback.
     */
    pub fn parse_fallback(i_token: YYCODETYPE) -> YYCODETYPE {
        if YYFALLBACK {
            return yyFallback[i_token as usize];
        }
        0
    }
}
