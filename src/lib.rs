#![doc = include_str!("../README.md")]

use sqlparser::ast::{
    Assignment, AssignmentTarget, ConflictTarget, Delete, DoUpdate, Expr, GroupByExpr, Ident,
    Insert, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, Offset, OnConflict,
    OnConflictAction, OnInsert, OrderBy, OrderByKind, Query, SelectItem, SetExpr, Statement, Value,
    ValueWithSpan, VisitMut, VisitorMut,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;
use std::collections::HashMap;
use std::ops::ControlFlow;

/// Fingerprint a single SQL string.
///
/// Unparseable SQL is returned as-is.
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
/// Unparseable SQL is returned as-is.
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

    let mut savepoint_visitor = SavepointVisitor::new();

    input
        .iter()
        .map(|sql| match Parser::parse_sql(dialect, sql) {
            Ok(mut ast) => {
                for stmt in &mut ast {
                    stmt.visit(&mut savepoint_visitor);
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

struct SavepointVisitor {
    savepoint_ids: HashMap<String, String>,
}

impl SavepointVisitor {
    fn new() -> Self {
        SavepointVisitor {
            savepoint_ids: HashMap::new(),
        }
    }
}

impl VisitorMut for SavepointVisitor {
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
                    if stmt.names.len() > 0 {
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
                if columns.len() > 0 {
                    *columns = vec![Ident::new("...")];
                }
                if let Some(source) = source {
                    if let SetExpr::Values(values) = source.as_mut().body.as_mut() {
                        values.rows = vec![vec![placeholder_value()]];
                    }
                }
                if let Some(on) = on {
                    match on {
                        OnInsert::OnConflict(OnConflict {
                            conflict_target,
                            action,
                        }) => {
                            if let Some(conflict_target) = conflict_target {
                                match conflict_target {
                                    ConflictTarget::Columns(columns) => {
                                        if columns.len() > 0 {
                                            *columns = vec![Ident::new("...")];
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            if let OnConflictAction::DoUpdate(DoUpdate {
                                assignments,
                                selection,
                            }) = action
                            {
                                if assignments.len() > 0 {
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
                        _ => {}
                    }
                }
                if let Some(returning) = returning {
                    if returning.len() > 0 {
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
                if assignments.len() > 0 {
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
                    if returning.len() > 0 {
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
                    if returning.len() > 0 {
                        *returning = vec![SelectItem::UnnamedExpr(placeholder_value())];
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            if select.projection.len() > 0 {
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
                        | JoinOperator::RightAnti(constraint) => match constraint {
                            JoinConstraint::On(expr) => {
                                *expr = placeholder_value();
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }
            }

            if let Some(selection) = &mut select.selection {
                *selection = placeholder_value();
            }

            match &mut select.group_by {
                GroupByExpr::Expressions(col_names, ..) => {
                    if col_names.len() > 0 {
                        *col_names = vec![placeholder_value()];
                    }
                }
                _ => {}
            }
        }
        if let Some(order_by) = &mut query.order_by {
            let OrderBy { kind, .. } = order_by;
            if let OrderByKind::Expressions(expressions) = kind {
                if expressions.len() > 0 {
                    if let Some(expr) = expressions.first_mut() {
                        expr.expr = placeholder_value();
                    }
                    expressions.truncate(1);
                }
            }
        }
        if let Some(limit) = &mut query.limit {
            *limit = placeholder_value();
        }
        if let Some(Offset { value, .. }) = &mut query.offset {
            *value = placeholder_value();
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, _relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        for part in _relation.0.iter_mut() {
            match part {
                ObjectNamePart::Identifier(ident) => {
                    maybe_unquote_ident(ident);
                }
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

fn maybe_unquote_ident(ident: &mut Ident) -> () {
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
    fn test_unparseable() {
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
    fn test_select_union() {
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
    fn test_select_except() {
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
            vec!["(SELECT a, b FROM c) INTERSECT (SELECT a, b FROM d)"],
            None,
        );
        assert_eq!(
            result,
            vec!["(SELECT ... FROM c) INTERSECT (SELECT ... FROM d)"]
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
            vec!["INSERT INTO a (b, c) VALUES (1, 2) ON CONFLICT(\"a\", \"b\") DO UPDATE SET \"d\" = EXCLUDED.d WHERE e = f RETURNING b, c"],
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
}
