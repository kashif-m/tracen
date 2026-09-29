//! Compile-time scalar checks for event acceptance rules, derives, metrics and event alerts.
use crate::TrackerAst;
use std::collections::{BTreeMap, BTreeSet};
use tracen_ir::error::{ErrorCode, TrackerError, TrackerResult};
use tracen_ir::{
    AggregationFunc, BinaryOperator, ComparisonOperator, Condition, Expression, FieldType,
    GroupByDimension,
};

#[derive(Clone, Copy, PartialEq)]
enum Type {
    Number,
    Text,
    Bool,
    Null,
    Object,
    Dynamic,
}

#[derive(Clone, Copy, PartialEq)]
enum Scope {
    Intake,
    Derive,
    Metric,
    Alert,
}

fn field_types(ast: &TrackerAst) -> BTreeMap<String, Type> {
    ast.fields
        .iter()
        .flat_map(|field| {
            let ty = match field.field_type {
                FieldType::Text | FieldType::Enum(_) => Type::Text,
                FieldType::Bool => Type::Bool,
                _ => Type::Number,
            };
            [
                (field.name.clone(), ty),
                (format!("payload.{}", field.name), ty),
            ]
        })
        .collect()
}

pub(crate) fn validate(ast: &TrackerAst) -> TrackerResult<()> {
    let fields = field_types(ast);
    let mut names = BTreeSet::new();
    for rule in &ast.validations {
        if !names.insert(&rule.name) {
            return Err(error(format!("duplicate validation '{}'", rule.name)));
        }
        condition_type(&rule.condition, &fields, Scope::Intake)
            .map_err(|message| error(format!("validation '{}': {message}", rule.name)))?;
    }
    Ok(())
}

pub(crate) fn validate_computations(ast: &TrackerAst) -> TrackerResult<()> {
    let mut fields = field_types(ast);
    fields.insert("event.id".into(), Type::Text);
    fields.insert("event.tracker_id".into(), Type::Text);
    fields.insert("event.ts".into(), Type::Number);
    for derive in &ast.derives {
        let ty = expression_type(&derive.expr, &fields, Scope::Derive)
            .map_err(|message| error(format!("derive '{}': {message}", derive.name)))?;
        // The engine exposes earlier derives by bare name until preparation ends.
        fields.insert(derive.name.clone(), ty);
    }
    for derive in &ast.derives {
        fields.insert(format!("payload.{}", derive.name), fields[&derive.name]);
    }
    for metric in &ast.metrics {
        let check = || -> Result<(), String> {
            match (&metric.aggregation.func, &metric.aggregation.target) {
                (AggregationFunc::Count, None) => {}
                (AggregationFunc::Count, Some(_)) => {
                    return Err("count() does not accept a target".into())
                }
                (AggregationFunc::DistinctCount, Some(target)) => {
                    if expression_type(target, &fields, Scope::Metric)? == Type::Object {
                        return Err("distinct_count requires a scalar target".into());
                    }
                }
                (_, None) => return Err("aggregation requires a target".into()),
                (_, Some(target)) => {
                    let ty = expression_type(target, &fields, Scope::Metric)?;
                    if !numeric(ty) && ty != Type::Bool {
                        return Err("aggregation target must be numeric or boolean".into());
                    }
                }
            }
            match (&metric.aggregation.func, &metric.aggregation.weight) {
                (AggregationFunc::WeightedAvg, Some(weight)) => {
                    if !numeric(expression_type(weight, &fields, Scope::Metric)?) {
                        return Err("weighted_avg weight must be numeric".into());
                    }
                }
                (AggregationFunc::WeightedAvg, None) => {
                    return Err("weighted_avg requires a weight".into())
                }
                (_, Some(_)) => return Err("only weighted_avg accepts a weight".into()),
                (_, None) => {}
            }
            for group in &metric.aggregation.group_by {
                if let GroupByDimension::Field(path) = group {
                    let ty =
                        expression_type(&Expression::Field(path.clone()), &fields, Scope::Metric)?;
                    if ty == Type::Object {
                        return Err("grouping requires a scalar field".into());
                    }
                }
            }
            Ok(())
        };
        check().map_err(|message| error(format!("metric '{}': {message}", metric.name)))?;
    }
    let mut alert_names = BTreeSet::new();
    for alert in &ast.alerts {
        if !alert_names.insert(&alert.name) {
            return Err(error(format!("duplicate alert '{}'", alert.name)));
        }
        expression_type(&alert.expr, &fields, Scope::Alert).map_err(|message| {
            error(format!(
                "event alert '{}': {message}; aggregate metrics are not in event scope",
                alert.name
            ))
        })?;
    }
    Ok(())
}

fn error(message: String) -> TrackerError {
    TrackerError::new_simple(ErrorCode::DslInvalidExpression, message)
}

fn condition_type(
    condition: &Condition,
    fields: &BTreeMap<String, Type>,
    scope: Scope,
) -> Result<(), String> {
    match condition {
        Condition::True | Condition::False => Ok(()),
        Condition::Not(inner) => condition_type(inner, fields, scope),
        Condition::And(parts) | Condition::Or(parts) => parts
            .iter()
            .try_for_each(|part| condition_type(part, fields, scope)),
        Condition::Comparison { op, left, right } => {
            let left = expression_type(left, fields, scope)?;
            let right = expression_type(right, fields, scope)?;
            let valid = match op {
                ComparisonOperator::Eq | ComparisonOperator::Neq => {
                    left == right
                        || left == Type::Null
                        || right == Type::Null
                        || left == Type::Dynamic
                        || right == Type::Dynamic
                }
                _ => numeric(left) && numeric(right),
            };
            if valid {
                Ok(())
            } else {
                Err("incompatible comparison types".into())
            }
        }
    }
}

fn expression_type(
    expr: &Expression,
    fields: &BTreeMap<String, Type>,
    scope: Scope,
) -> Result<Type, String> {
    match expr {
        Expression::Number(_) | Expression::Int(_) => Ok(Type::Number),
        Expression::Text(_) => Ok(Type::Text),
        Expression::Bool(_) => Ok(Type::Bool),
        Expression::Null => Ok(Type::Null),
        Expression::Field(path) => fields
            .get(path)
            .copied()
            .or_else(|| {
                (scope != Scope::Intake
                    && path.starts_with("meta.")
                    && path.split('.').all(|part| !part.is_empty()))
                .then_some(Type::Dynamic)
            })
            .ok_or_else(|| format!("field '{path}' is not available in this expression")),
        Expression::Binary { op, left, right } => {
            if scope == Scope::Metric && matches!(op, BinaryOperator::Mod) {
                return Err("mod operator is not supported in metric expressions".into());
            }
            if numeric(expression_type(left, fields, scope)?)
                && numeric(expression_type(right, fields, scope)?)
            {
                Ok(Type::Number)
            } else {
                Err("arithmetic requires numeric fields".into())
            }
        }
        Expression::Conditional {
            condition,
            then_expr,
            else_expr,
        } => {
            condition_type(condition, fields, scope)?;
            let then_type = expression_type(then_expr, fields, scope)?;
            let else_type = expression_type(else_expr, fields, scope)?;
            if then_type == Type::Dynamic || else_type == Type::Dynamic {
                Ok(Type::Dynamic)
            } else if then_type == else_type || else_type == Type::Null {
                Ok(then_type)
            } else if then_type == Type::Null {
                Ok(else_type)
            } else {
                Err("incompatible conditional result types".into())
            }
        }
        Expression::Function { name, args }
            if matches!(scope, Scope::Derive | Scope::Alert) && name == "signal" =>
        {
            if !(1..=2).contains(&args.len()) {
                return Err("signal requires a name and optional payload".into());
            }
            let name_type = expression_type(&args[0], fields, scope)?;
            if !matches!(name_type, Type::Text | Type::Dynamic) {
                return Err("signal name must be text".into());
            }
            if let Some(payload) = args.get(1) {
                expression_type(payload, fields, scope)?;
            }
            Ok(Type::Object)
        }
        Expression::Function { name, .. } => Err(format!(
            "function '{name}' is not supported in this expression"
        )),
    }
}

fn numeric(ty: Type) -> bool {
    matches!(ty, Type::Number | Type::Dynamic)
}
