use anyhow::anyhow;
use solabi::abi::EventDescriptor;
use solabi::Value;
use std::collections::HashMap;

/// An event is represented in the database in several tables.
///
/// All tables have some columns that are unrelated to the event's fields. See
/// `FIXED_COLUMNS`. The first table contains all fields that exist once per
/// event which means they do not show up in arrays. The other tables contain
/// fields that are part of arrays. Those tables additionally have the column
/// `ARRAY_COLUMN`.
///
/// The order of tables and fields is given by the `event_visitor` module.
pub struct PreparedEvent<I, R> {
    pub descriptor: EventDescriptor,
    pub insert_statements: Vec<I>,
    /// Prepared statements for removing rows starting at some block number.
    /// Every statement takes a block number as parameter.
    pub remove_statements: Vec<R>,
}

pub fn check_descriptor<I, R>(
    fields: &[Value],
    event: &PreparedEvent<I, R>,
) -> Result<(), anyhow::Error> {
    for (i, (value, kind)) in fields.iter().zip(&event.descriptor.inputs).enumerate() {
        if value.kind() != kind.field.kind {
            return Err(anyhow!("event field {i} doesn't match event descriptor"));
        }
    }
    Ok(())
}

// TODO:
// - Check that either no table exists or all tables exist and with the right
//   types.
// - Maybe have `CHECK` clauses to enforce things like address and integers
//   having expected length.
// - Maybe store serialized event descriptor in the database so we can load and
//   check it.
pub fn event_exists_with_same_signature<I, R>(
    events: &HashMap<String, PreparedEvent<I, R>>,
    name: &str,
    event: &EventDescriptor,
) -> Result<bool, anyhow::Error> {
    if let Some(existing) = events.get(name) {
        if event != &existing.descriptor {
            return Err(anyhow!(
                "event {} (database name {name}) already exists with different signature",
                event.name
            ));
        }
        return Ok(true);
    }
    Ok(false)
}

pub fn push_sql_value<T>(
    sql_values: &mut [(Option<usize>, Vec<T>)],
    in_array: bool,
    sql_value: T,
) {
    let target = if in_array {
        sql_values.last_mut()
    } else {
        sql_values.first_mut()
    };
    target
        .expect("Expected at least one value set in sql_values")
        .1
        .push(sql_value);
}

