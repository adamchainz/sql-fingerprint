use sqlparser::ast::{
    Expr, Ident, OrderBy, OrderByKind, Query, SelectItem, SetExpr, Statement, Value, ValueWithSpan,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;
use std::collections::HashMap;

pub fn fingerprint(input: Vec<&str>, dialect: Option<&dyn Dialect>) -> Vec<String> {
    let dialect = dialect.unwrap_or(&GenericDialect {});
    let mut savepoint_simple_ids: HashMap<String, String> = HashMap::new();
    input
        .iter()
        .map(|sql| {
            let ast = Parser::parse_sql(dialect, sql).unwrap();
            ast.into_iter()
                .map(|mut stmt| simplify_statement(&mut stmt, &mut savepoint_simple_ids))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

fn simplify_statement(
    stmt: &mut Statement,
    savepoint_simple_ids: &mut HashMap<String, String>,
) -> String {
    match stmt {
        Statement::Savepoint { name } => {
            let savepoint_id = format!("s{}", savepoint_simple_ids.len() + 1);
            savepoint_simple_ids.insert(name.value.clone(), savepoint_id.clone());
            *name = Ident::new(savepoint_id);
        }
        Statement::ReleaseSavepoint { name } => {
            let savepoint_id = savepoint_simple_ids.get(&name.value).unwrap().clone();
            *name = Ident::new(savepoint_id);
        }
        Statement::Rollback {
            savepoint: Some(name),
            ..
        } => {
            let savepoint_id = savepoint_simple_ids.get(&name.value).unwrap().clone();
            *name = Ident::new(savepoint_id);
        }
        Statement::Query(query) => {
            simplify_query(query);
        }
        Statement::Declare { stmts } => {
            for stmt in stmts {
                if stmt.names.len() > 0 {
                    stmt.names = vec![Ident::new("...")];
                }
                if let Some(for_query) = &mut stmt.for_query {
                    simplify_query(for_query);
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
    stmt.to_string()
}

fn simplify_query(query: &mut Query) {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comments_dropped() {
        let result = fingerprint(vec!["SELECT 123 /* magic value */"], None);
        assert_eq!(result, vec!["SELECT ..."]);
    }

    #[test]
    fn test_savepoint() {
        let result = fingerprint(vec!["SAVEPOINT \"s1234\""], None);
        assert_eq!(result, vec!["SAVEPOINT s1"]);
    }

    #[test]
    fn test_multiple_savepoints() {
        let result = fingerprint(vec!["SAVEPOINT \"s1234\"", "SAVEPOINT \"s3456\""], None);
        assert_eq!(result, vec!["SAVEPOINT s1", "SAVEPOINT s2"]);
    }

    #[test]
    fn test_duplicate_savepoints() {
        let result = fingerprint(vec!["SAVEPOINT \"s1234\"", "SAVEPOINT \"s1234\""], None);
        assert_eq!(result, vec!["SAVEPOINT s1", "SAVEPOINT s2"]);
    }

    #[test]
    fn test_release_savepoints() {
        let result = fingerprint(
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
        let result = fingerprint(
            vec!["SAVEPOINT \"s1234\"", "ROLLBACK TO SAVEPOINT \"s1234\""],
            None,
        );
        assert_eq!(result, vec!["SAVEPOINT s1", "ROLLBACK TO SAVEPOINT s1"]);
    }

    #[test]
    fn test_select() {
        let result = fingerprint(vec!["SELECT 1"], None);
        assert_eq!(result, vec!["SELECT ..."]);
    }

    #[test]
    fn test_select_with_from() {
        let result = fingerprint(vec!["SELECT a, b FROM c"], None);
        assert_eq!(result, vec!["SELECT ... FROM c"]);
    }

    #[test]
    fn test_select_with_from_join() {
        let result = fingerprint(vec!["SELECT a, b FROM c JOIN d"], None);
        assert_eq!(result, vec!["SELECT ... FROM c JOIN d"]);
    }

    #[test]
    fn test_select_with_order_by() {
        let result = fingerprint(vec!["SELECT a, b FROM c ORDER BY a, b DESC"], None);
        assert_eq!(result, vec!["SELECT ... FROM c ORDER BY ..."]);
    }

    #[test]
    fn test_select_with_order_by_more() {
        let result = fingerprint(vec!["SELECT a, b FROM c ORDER BY a ASC, b DESC"], None);
        assert_eq!(result, vec!["SELECT ... FROM c ORDER BY ... ASC"]);
    }

    #[test]
    fn test_declare_cursor() {
        let result = fingerprint(vec!["DECLARE c CURSOR FOR SELECT a, b FROM c join d"], None);
        assert_eq!(
            result,
            vec!["DECLARE ... CURSOR FOR SELECT ... FROM c JOIN d"]
        );
    }

    #[test]
    fn test_insert() {
        let result = fingerprint(
            vec!["INSERT INTO c (a, b) VALUES (1, 2), (3, 4) RETURNING d"],
            None,
        );
        assert_eq!(
            result,
            vec!["INSERT INTO c (...) VALUES (...) RETURNING ..."]
        );
    }
}
