#![doc = include_str!("../README.md")]

use sqlparser::ast::{
    Assignment, AssignmentTarget, ConflictTarget, Delete, Distinct, DoUpdate, Expr, GroupByExpr,
    Ident, Insert, JoinConstraint, JoinOperator, LimitClause, ObjectName, ObjectNamePart, Offset,
    OnConflict, OnConflictAction, OnInsert, OrderBy, OrderByKind, Query, SelectItem, SetExpr,
    Statement, TableAliasColumnDef, TableFactor, Value, ValueWithSpan, VisitMut, VisitorMut,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;
use std::collections::HashMap;
use std::ops::ControlFlow;

/// Fingerprint a single SQL string.
///
/// Unparsable SQL is returned as-is.
///
/// # Example
/// ```
/// use sql_fingerprint::fingerprint_one;
///
/// let result = fingerprint_one("SELECT a, b FROM c ORDER BY b", None);
/// assert_eq!(result, "SELECT ... FROM c ORDER BY ...");
/// ```
pub fn fingerprint_one(input: &str, dialect: Option<&dyn Dialect>) -> String {
    fingerprint_many(vec![input], dialect).join(" ")
}

/// Fingerprint multiple SQL strings.
/// Doing so for a batch of strings allows sharing some state, such as savepoint ID aliases.
///
/// Unparsable SQL is returned as-is.
///
/// # Example
/// ```
/// use sql_fingerprint::fingerprint_many;
///
/// let result = fingerprint_many(vec!["SELECT a, b FROM c", "SELECT b, c FROM d"], None);
/// assert_eq!(result, vec!["SELECT ... FROM c", "SELECT ... FROM d"]);
/// ```
pub fn fingerprint_many(input: Vec<&str>, dialect: Option<&dyn Dialect>) -> Vec<String> {
    let dialect = dialect.unwrap_or(&GenericDialect {});

    let mut visitor = FingerprintingVisitor::new();

    input
        .iter()
        .map(|sql| match Parser::parse_sql(dialect, sql) {
            Ok(mut ast) => {
                for stmt in &mut ast {
                    let _ = stmt.visit(&mut visitor);
                }

                ast.into_iter()
                    .map(|stmt| stmt.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            }
            Err(_) => sql.to_string(),
        })
        .collect()
}

struct FingerprintingVisitor {
    savepoint_ids: HashMap<String, String>,
}

impl FingerprintingVisitor {
    fn new() -> Self {
        FingerprintingVisitor {
            savepoint_ids: HashMap::new(),
        }
    }

    fn visit_select(&mut self, select: &mut sqlparser::ast::Select) {
        if !select.projection.is_empty() {
            if let Some(item) = select.projection.first_mut() {
                match item {
                    SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => {
                        *item = SelectItem::UnnamedExpr(placeholder_value());
                    }
                    _ => {}
                }
            }
            select.projection.truncate(1);
        }

        if let Some(Distinct::On(exprs)) = &mut select.distinct {
            if !exprs.is_empty() {
                *exprs = vec![placeholder_value()];
            }
        };

        for table_with_joins in &mut select.from {
            for join in &mut table_with_joins.joins {
                match &mut join.join_operator {
                    JoinOperator::Join(constraint)
                    | JoinOperator::Inner(constraint)
                    | JoinOperator::Left(constraint)
                    | JoinOperator::LeftOuter(constraint)
                    | JoinOperator::Right(constraint)
                    | JoinOperator::RightOuter(constraint)
                    | JoinOperator::FullOuter(constraint)
                    | JoinOperator::Semi(constraint)
                    | JoinOperator::LeftSemi(constraint)
                    | JoinOperator::RightSemi(constraint)
                    | JoinOperator::Anti(constraint)
                    | JoinOperator::LeftAnti(constraint)
                    | JoinOperator::RightAnti(constraint) => {
                        if let JoinConstraint::On(expr) = constraint {
                            *expr = placeholder_value();
                        }
                    }
                    _ => {}
                }
            }
        }

        if let Some(selection) = &mut select.selection {
            *selection = placeholder_value();
        }

        if let GroupByExpr::Expressions(col_names, ..) = &mut select.group_by {
            if !col_names.is_empty() {
                *col_names = vec![placeholder_value()];
            }
        }
    }
}

impl VisitorMut for FingerprintingVisitor {
    type Break = ();

    fn pre_visit_statement(&mut self, stmt: &mut Statement) -> ControlFlow<Self::Break> {
        match stmt {
            Statement::Savepoint { name } => {
                let savepoint_id = format!("s{}", self.savepoint_ids.len() + 1);
                self.savepoint_ids
                    .insert(name.value.clone(), savepoint_id.clone());
                *name = Ident::new(savepoint_id);
            }
            Statement::ReleaseSavepoint { name } => {
                if let Some(savepoint_id) = self.savepoint_ids.get(&name.value).cloned() {
                    *name = Ident::new(savepoint_id);
                }
            }
            Statement::Rollback {
                savepoint: Some(name),
                ..
            } => {
                if let Some(savepoint_id) = self.savepoint_ids.get(&name.value).cloned() {
                    *name = Ident::new(savepoint_id);
                }
            }
            Statement::Declare { stmts } => {
                for stmt in stmts {
                    if !stmt.names.is_empty() {
                        stmt.names = vec![Ident::new("...")];
                    }
                }
            }
            Statement::Insert(Insert {
                columns,
                source,
                on,
                returning,
                ..
            }) => {
                if !columns.is_empty() {
                    *columns = vec![Ident::new("...")];
                }
                if let Some(source) = source {
                    if let SetExpr::Values(values) = source.as_mut().body.as_mut() {
                        values.rows = vec![vec![placeholder_value()]];
                    }
                }
                if let Some(OnInsert::OnConflict(OnConflict {
                    conflict_target,
                    action,
                })) = on
                {
                    if let Some(ConflictTarget::Columns(columns)) = conflict_target {
                        if !columns.is_empty() {
                            *columns = vec![Ident::new("...")];
                        }
                    }
                    if let OnConflictAction::DoUpdate(DoUpdate {
                        assignments,
                        selection,
                    }) = action
                    {
                        if !assignments.is_empty() {
                            *assignments = vec![Assignment {
                                target: AssignmentTarget::ColumnName(ObjectName(vec![
                                    ObjectNamePart::Identifier(Ident::new("...")),
                                ])),
                                value: placeholder_value(),
                            }];
                        }
                        if let Some(selection) = selection {
                            *selection = placeholder_value();
                        }
                    }
                }
                if let Some(returning) = returning {
                    if !returning.is_empty() {
                        *returning = vec![SelectItem::UnnamedExpr(placeholder_value())];
                    }
                }
            }
            Statement::Update {
                assignments,
                selection,
                returning,
                ..
            } => {
                if !assignments.is_empty() {
                    *assignments = vec![sqlparser::ast::Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName(vec![
                            ObjectNamePart::Identifier(Ident::new("...")),
                        ])),
                        value: placeholder_value(),
                    }];
                }
                if let Some(selection) = selection {
                    *selection = placeholder_value();
                }
                if let Some(returning) = returning {
                    if !returning.is_empty() {
                        *returning = vec![SelectItem::UnnamedExpr(placeholder_value())];
                    }
                }
            }
            Statement::Delete(Delete {
                selection,
                returning,
                ..
            }) => {
                if let Some(selection) = selection {
                    *selection = placeholder_value();
                }
                if let Some(returning) = returning {
                    if !returning.is_empty() {
                        *returning = vec![SelectItem::UnnamedExpr(placeholder_value())];
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        match query.body.as_mut() {
            SetExpr::Select(select) => {
                self.visit_select(select);
            }
            SetExpr::SetOperation { left, right, .. } => {
                // push left and right into a double-ended queue to visit them,
                // expnading left and right as required.
                let mut stack = vec![left.as_mut(), right.as_mut()];
                while let Some(set_expr) = stack.pop() {
                    match set_expr {
                        SetExpr::Select(select) => {
                            self.visit_select(select);
                        }
                        SetExpr::SetOperation { left, right, .. } => {
                            // Push left and right onto the stack for further processing.
                            stack.push(left.as_mut());
                            stack.push(right.as_mut());
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        if let Some(order_by) = &mut query.order_by {
            let OrderBy { kind, .. } = order_by;
            if let OrderByKind::Expressions(expressions) = kind {
                if !expressions.is_empty() {
                    if let Some(expr) = expressions.first_mut() {
                        expr.expr = placeholder_value();
                    }
                    expressions.truncate(1);
                }
            }
        }
        if let Some(limit_clause) = &mut query.limit_clause {
            match limit_clause {
                LimitClause::LimitOffset {
                    limit,
                    offset,
                    limit_by,
                } => {
                    if let Some(limit_value) = limit {
                        *limit_value = placeholder_value();
                    }
                    if let Some(Offset { value, .. }) = offset {
                        *value = placeholder_value();
                    }
                    if !limit_by.is_empty() {
                        *limit_by = vec![placeholder_value()];
                    }
                }
                // MySQL specific, needs testing!Ó
                LimitClause::OffsetCommaLimit { offset, limit } => {
                    *offset = placeholder_value();
                    *limit = placeholder_value();
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, _relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        for part in _relation.0.iter_mut() {
            if let ObjectNamePart::Identifier(ident) = part {
                maybe_unquote_ident(ident);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::UNNEST {
            alias, array_exprs, ..
        } = table_factor
        {
            if let Some(alias) = alias {
                if !alias.columns.is_empty() {
                    alias.columns = vec![TableAliasColumnDef {
                        name: Ident::new("..."),
                        data_type: None,
                    }];
                }
            }
            if !array_exprs.is_empty() {
                *array_exprs = vec![placeholder_value()];
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, _expr: &mut Expr) -> ControlFlow<Self::Break> {
        match _expr {
            Expr::Identifier(ident) => {
                maybe_unquote_ident(ident);
            }
            Expr::CompoundIdentifier(idents) => {
                for ident in idents {
                    maybe_unquote_ident(ident);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn placeholder_value() -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Placeholder("...".to_string()),
        span: Span::empty(),
    })
}

fn maybe_unquote_ident(ident: &mut Ident) {
    let Ident {
        value, quote_style, ..
    } = ident;

    if value.chars().all(|c| c.is_alphanumeric() || c == '_') {
        *quote_style = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_one() {
        let result = fingerprint_one("SELECT 123", None);
        assert_eq!(result, "SELECT ...");
    }

    #[test]
    fn test_empty() {
        let result = fingerprint_many(vec![""], None);
        assert_eq!(result, vec![""]);
    }

    #[test]
    fn test_unparsable() {
        let result = fingerprint_many(vec!["SELECT  SELECT  SELECT  SELECT"], None);
        assert_eq!(result, vec!["SELECT  SELECT  SELECT  SELECT"]);
    }

    #[test]
    fn test_comments_dropped() {
        let result = fingerprint_many(vec!["SELECT 123 /* magic value */"], None);
        assert_eq!(result, vec!["SELECT ..."]);
    }

    #[test]
    fn test_savepoint() {
        let result = fingerprint_many(vec!["SAVEPOINT \"s1234\""], None);
        assert_eq!(result, vec!["SAVEPOINT s1"]);
    }

    #[test]
    fn test_multiple_savepoints() {
        let result = fingerprint_many(vec!["SAVEPOINT \"s1234\"", "SAVEPOINT \"s3456\""], None);
        assert_eq!(result, vec!["SAVEPOINT s1", "SAVEPOINT s2"]);
    }

    #[test]
    fn test_duplicate_savepoints() {
        let result = fingerprint_many(vec!["SAVEPOINT \"s1234\"", "SAVEPOINT \"s1234\""], None);
        assert_eq!(result, vec!["SAVEPOINT s1", "SAVEPOINT s2"]);
    }

    #[test]
    fn test_release_savepoints() {
        let result = fingerprint_many(
            vec![
                "SAVEPOINT \"s1234\"",
                "RELEASE SAVEPOINT \"s1234\"",
                "SAVEPOINT \"s2345\"",
                "RELEASE SAVEPOINT \"s2345\"",
            ],
            None,
        );
        assert_eq!(
            result,
            vec![
                "SAVEPOINT s1",
                "RELEASE SAVEPOINT s1",
                "SAVEPOINT s2",
                "RELEASE SAVEPOINT s2"
            ]
        );
    }

    #[test]
    fn test_rollback_savepoint() {
        let result = fingerprint_many(
            vec!["SAVEPOINT \"s1234\"", "ROLLBACK TO SAVEPOINT \"s1234\""],
            None,
        );
        assert_eq!(result, vec!["SAVEPOINT s1", "ROLLBACK TO SAVEPOINT s1"]);
    }

    #[test]
    fn test_select() {
        let result = fingerprint_many(vec!["SELECT a, b FROM c WHERE a = b"], None);
        assert_eq!(result, vec!["SELECT ... FROM c WHERE ..."]);
    }

    #[test]
    fn test_select_single_value() {
        let result = fingerprint_many(vec!["SELECT 1"], None);
        assert_eq!(result, vec!["SELECT ..."]);
    }

    #[test]
    fn test_select_distinct_on() {
        let result = fingerprint_many(vec!["SELECT DISTINCT ON (a, b) c FROM d"], None);
        assert_eq!(result, vec!["SELECT DISTINCT ON (...) ... FROM d"]);
    }

    #[test]
    fn test_select_with_from_quoted() {
        let result = fingerprint_many(vec!["SELECT a, b FROM \"c\".\"d\""], None);
        assert_eq!(result, vec!["SELECT ... FROM c.d"]);
    }

    #[test]
    fn test_select_with_from_join() {
        let result = fingerprint_many(vec!["SELECT a, b FROM c JOIN d"], None);
        assert_eq!(result, vec!["SELECT ... FROM c JOIN d"]);
    }

    #[test]
    fn test_select_with_from_inner_join_quoted() {
        let result = fingerprint_many(
            vec!["SELECT a, b FROM c INNER JOIN d ON (\"d\".\"a\" = \"c\".\"a\")"],
            None,
        );
        assert_eq!(result, vec!["SELECT ... FROM c INNER JOIN d ON ..."]);
    }

    #[test]
    fn test_select_with_from_left_outer_join_quoted() {
        let result = fingerprint_many(
            vec!["SELECT a, b FROM c LEFT OUTER JOIN d ON (\"d\".\"a\" = \"c\".\"a\")"],
            None,
        );
        assert_eq!(result, vec!["SELECT ... FROM c LEFT OUTER JOIN d ON ..."]);
    }

    #[test]
    fn test_select_with_group_by() {
        let result = fingerprint_many(vec!["SELECT a, b FROM c GROUP BY a, b"], None);
        assert_eq!(result, vec!["SELECT ... FROM c GROUP BY ..."]);
    }

    #[test]
    fn test_select_with_order_by() {
        let result = fingerprint_many(vec!["SELECT a, b FROM c ORDER BY a, b DESC"], None);
        assert_eq!(result, vec!["SELECT ... FROM c ORDER BY ..."]);
    }

    #[test]
    fn test_select_with_order_by_more() {
        let result = fingerprint_many(vec!["SELECT a, b FROM c ORDER BY a ASC, b DESC"], None);
        assert_eq!(result, vec!["SELECT ... FROM c ORDER BY ... ASC"]);
    }

    #[test]
    fn test_select_with_limit_offset() {
        let result = fingerprint_many(vec!["SELECT a FROM b LIMIT 21 OFFSET 101 ROWS"], None);
        assert_eq!(result, vec!["SELECT ... FROM b LIMIT ... OFFSET ... ROWS"]);
    }

    #[test]
    fn test_clickhouse_select_with_limit_by() {
        let result = fingerprint_many(vec!["SELECT a FROM b LIMIT 21 BY c"], None);
        assert_eq!(result, vec!["SELECT ... FROM b LIMIT ... BY ..."]);
    }

    #[test]
    fn test_mysql_select_with_limit_comma() {
        let result = fingerprint_many(vec!["SELECT a FROM b LIMIT 21, 101"], None);
        assert_eq!(result, vec!["SELECT ... FROM b LIMIT ..., ..."]);
    }

    #[test]
    fn test_select_union() {
        let result = fingerprint_many(vec!["SELECT a, b FROM c UNION SELECT a, b FROM d"], None);
        assert_eq!(result, vec!["SELECT ... FROM c UNION SELECT ... FROM d"]);
    }

    #[test]
    fn test_select_union_parenthesized() {
        let result = fingerprint_many(
            vec!["(SELECT a, b FROM c) UNION (SELECT a, b FROM d)"],
            None,
        );
        assert_eq!(
            result,
            vec!["(SELECT ... FROM c) UNION (SELECT ... FROM d)"]
        );
    }

    #[test]
    fn test_select_union_all() {
        let result = fingerprint_many(
            vec!["SELECT a, b FROM c UNION ALL SELECT a, b FROM d"],
            None,
        );
        assert_eq!(
            result,
            vec!["SELECT ... FROM c UNION ALL SELECT ... FROM d"]
        );
    }

    #[test]
    fn test_select_union_all_parenthesized() {
        let result = fingerprint_many(
            vec!["(SELECT a, b FROM c) UNION ALL (SELECT a, b FROM d)"],
            None,
        );
        assert_eq!(
            result,
            vec!["(SELECT ... FROM c) UNION ALL (SELECT ... FROM d)"]
        );
    }

    #[test]
    fn test_select_except() {
        let result = fingerprint_many(vec!["SELECT a, b FROM c EXCEPT SELECT a, b FROM d"], None);
        assert_eq!(result, vec!["SELECT ... FROM c EXCEPT SELECT ... FROM d"]);
    }

    #[test]
    fn test_select_except_parenthesized() {
        let result = fingerprint_many(
            vec!["(SELECT a, b FROM c) EXCEPT (SELECT a, b FROM d)"],
            None,
        );
        assert_eq!(
            result,
            vec!["(SELECT ... FROM c) EXCEPT (SELECT ... FROM d)"]
        );
    }

    #[test]
    fn test_select_intersect() {
        let result = fingerprint_many(
            vec!["SELECT a, b FROM c INTERSECT SELECT a, b FROM d"],
            None,
        );
        assert_eq!(
            result,
            vec!["SELECT ... FROM c INTERSECT SELECT ... FROM d"]
        );
    }

    #[test]
    fn test_select_intersect_parenthesized() {
        let result = fingerprint_many(
            vec!["(SELECT a, b FROM c) INTERSECT (SELECT a, b FROM d)"],
            None,
        );
        assert_eq!(
            result,
            vec!["(SELECT ... FROM c) INTERSECT (SELECT ... FROM d)"]
        );
    }

    #[test]
    fn test_select_union_triple() {
        let result = fingerprint_many(
            vec!["SELECT a, b FROM c UNION SELECT a, b FROM d UNION SELECT a, b FROM e"],
            None,
        );
        assert_eq!(
            result,
            vec!["SELECT ... FROM c UNION SELECT ... FROM d UNION SELECT ... FROM e"]
        );
    }

    #[test]
    fn test_select_union_triple_parenthesized() {
        let result = fingerprint_many(
            vec!["(SELECT a, b FROM c) UNION (SELECT a, b FROM d) UNION (SELECT a, b FROM e)"],
            None,
        );
        assert_eq!(
            result,
            vec!["(SELECT ... FROM c) UNION (SELECT ... FROM d) UNION (SELECT ... FROM e)"]
        );
    }

    #[test]
    fn test_with_recursive_select() {
        let result = fingerprint_many(
            vec!["WITH RECURSIVE t AS (SELECT a, b FROM c WHERE d = 12345) SELECT * FROM t"],
            None,
        );
        assert_eq!(
            result,
            vec!["WITH RECURSIVE t AS (SELECT ... FROM c WHERE ...) SELECT * FROM t"]
        );
    }

    #[test]
    fn test_with_recursive_select_union() {
        let result = fingerprint_many(
            vec!["WITH RECURSIVE t AS (SELECT a FROM b UNION SELECT a FROM c) SELECT * FROM t"],
            None,
        );

        assert_eq!(
            result,
            vec!["WITH RECURSIVE t AS (SELECT ... FROM b UNION SELECT ... FROM c) SELECT * FROM t"],
        );
    }

    #[test]
    fn test_declare_cursor() {
        let result = fingerprint_many(vec!["DECLARE c CURSOR FOR SELECT a, b FROM c join d"], None);
        assert_eq!(
            result,
            vec!["DECLARE ... CURSOR FOR SELECT ... FROM c JOIN d"]
        );
    }

    #[test]
    fn test_insert() {
        let result = fingerprint_many(
            vec!["INSERT INTO c (a, b) VALUES (1, 2), (3, 4) RETURNING d"],
            None,
        );
        assert_eq!(
            result,
            vec!["INSERT INTO c (...) VALUES (...) RETURNING ..."]
        );
    }

    #[test]
    fn test_insert_select() {
        let result = fingerprint_many(vec!["INSERT INTO a (b, c) SELECT d FROM e"], None);
        assert_eq!(result, vec!["INSERT INTO a (...) SELECT ... FROM e"]);
    }

    #[test]
    fn test_insert_on_conflict() {
        let result = fingerprint_many(
            vec![
                "INSERT INTO a (b, c) VALUES (1, 2) ON CONFLICT(\"a\", \"b\") DO UPDATE SET \"d\" = EXCLUDED.d WHERE e = f RETURNING b, c",
            ],
            None,
        );
        assert_eq!(
            result,
            vec![
                "INSERT INTO a (...) VALUES (...) ON CONFLICT(...) DO UPDATE SET ... = ... WHERE ... RETURNING ..."
            ]
        );
    }

    #[test]
    fn test_update() {
        let result = fingerprint_many(
            vec!["UPDATE a SET b = 1, c = 2 WHERE d = 3 RETURNING e"],
            None,
        );
        assert_eq!(
            result,
            vec!["UPDATE a SET ... = ... WHERE ... RETURNING ..."]
        );
    }

    #[test]
    fn test_delete() {
        let result = fingerprint_many(vec!["DELETE FROM a WHERE b = 1 RETURNING c"], None);
        assert_eq!(result, vec!["DELETE FROM a WHERE ... RETURNING ..."]);
    }

    #[test]
    fn test_insert_select_unnest() {
        let result = fingerprint_many(
            vec![
                "INSERT INTO my_table (col1, col2) SELECT * FROM UNNEST(ARRAY[1,2,3,4,5]) ON CONFLICT(col1) DO UPDATE SET col2 = EXCLUDED.col2",
            ],
            None,
        );
        assert_eq!(
            result,
            vec![
                "INSERT INTO my_table (...) SELECT * FROM UNNEST(...) ON CONFLICT(...) DO UPDATE SET ... = ..."
            ]
        );
    }

    #[test]
    fn test_select_from_unnest() {
        let result = fingerprint_many(vec!["SELECT * FROM UNNEST(ARRAY[1,2,3,4,5])"], None);
        assert_eq!(result, vec!["SELECT * FROM UNNEST(...)"]);
    }

    #[test]
    fn test_select_from_unnest_with_alias() {
        let result = fingerprint_many(
            vec!["SELECT * FROM UNNEST(ARRAY[1,2,3,4,5]) AS t (value)"],
            None,
        );
        assert_eq!(result, vec!["SELECT * FROM UNNEST(...) AS t (...)"]);
    }
}
