#![doc = include_str!("../README.md")]

use sqlparser::ast::{
    Expr, Ident, OrderBy, OrderByKind, Query, SelectItem, SetExpr, Statement, Value, ValueWithSpan,
    VisitMut, VisitorMut,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;
use std::collections::HashMap;
use std::ops::ControlFlow;

/// Fingerprint a single SQL string.
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
        .map(|sql| {
            let mut ast = Parser::parse_sql(dialect, sql).unwrap();

            for stmt in &mut ast {
                stmt.visit(&mut savepoint_visitor);
            }

            ast.into_iter()
                .map(|stmt| stmt.to_string())
                .collect::<Vec<_>>()
                .join(" ")
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
            Statement::Insert(insert) => {
                if insert.columns.len() > 0 {
                    insert.columns = vec![Ident::new("...")];
                }
                if let Some(source) = &mut insert.source {
                    if let SetExpr::Values(values) = source.as_mut().body.as_mut() {
                        values.rows = vec![vec![Expr::Value(ValueWithSpan {
                            value: Value::Placeholder("...".to_string()),
                            span: Span::empty(),
                        })]];
                    }
                }
                if let Some(returning) = &mut insert.returning {
                    if returning.len() > 0 {
                        *returning = vec![SelectItem::UnnamedExpr(Expr::Value(ValueWithSpan {
                            value: Value::Placeholder("...".to_string()),
                            span: Span::empty(),
                        }))];
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
                            *item = SelectItem::UnnamedExpr(Expr::Value(ValueWithSpan {
                                value: Value::Placeholder("...".to_string()),
                                span: Span::empty(),
                            }));
                        }
                        _ => {}
                    }
                }
                select.projection.truncate(1);
            }
        }
        if let Some(order_by) = &mut query.order_by {
            let OrderBy { kind, .. } = order_by;
            if let OrderByKind::Expressions(expressions) = kind {
                if expressions.len() > 0 {
                    if let Some(expr) = expressions.first_mut() {
                        expr.expr = Expr::Value(ValueWithSpan {
                            value: Value::Placeholder("...".to_string()),
                            span: Span::empty(),
                        });
                    }
                    expressions.truncate(1);
                }
            }
        }
        ControlFlow::Continue(())
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
        let result = fingerprint_many(vec!["SELECT 1"], None);
        assert_eq!(result, vec!["SELECT ..."]);
    }

    #[test]
    fn test_select_with_from() {
        let result = fingerprint_many(vec!["SELECT a, b FROM c"], None);
        assert_eq!(result, vec!["SELECT ... FROM c"]);
    }

    #[test]
    fn test_select_with_from_join() {
        let result = fingerprint_many(vec!["SELECT a, b FROM c JOIN d"], None);
        assert_eq!(result, vec!["SELECT ... FROM c JOIN d"]);
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
}
