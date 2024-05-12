use query_lang::query::ast::{BetweenOperator, EqOperator, ExistsOperator, GteOperator, GtOperator, IsEmptyOperator, LteOperator, LtOperator, NeOperator, Operator, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Condition{
    GreaterThan(Value),
    GreaterThanOrEqual(Value),
    Equal(Value),
    NotEqual(Value),
    Empty,
    Exists,
    LowerThan(Value),
    LowerThanOrEqual(Value),
    Between { from: Value, to: Value },
}


impl From<Operator> for Condition {
    fn from(value: Operator) -> Self {
        match value {
            Operator::Eq(EqOperator(value)) => Condition::Equal(value),
            Operator::Gt(GtOperator(value)) => Condition::GreaterThan(value),
            Operator::Gte(GteOperator(value)) => Condition::GreaterThan(value),
            Operator::Lt(LtOperator(value)) => Condition::LowerThan(value),
            Operator::Lte(LteOperator(value)) => Condition::GreaterThan(value),
            Operator::Ne(NeOperator(value)) => Condition::NotEqual(value),
            Operator::Between(BetweenOperator(from ,to)) => Condition::Between { from, to },
            Operator::Exists(ExistsOperator(_)) => Condition::Exists,
            Operator::IsEmpty(IsEmptyOperator(_)) => Condition::Empty,
            _ => unreachable!()
        }
    }
}