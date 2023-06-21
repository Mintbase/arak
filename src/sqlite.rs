use crate::{
    database::{self, Database, IndexedBlock, Log},
    event_visitor::{VisitKind, VisitValue},
};
use anyhow::{anyhow, Context, Result};
use rusqlite::{
    types::{ToSqlOutput, Type as SqlType, Value as SqlValue, ValueRef as SqlValueRef},
    Connection, OptionalExtension,
};
use solabi::{
    abi::EventDescriptor,
    value::{Value as AbiValue, ValueKind as AbiKind},
};
use std::{collections::HashMap, fmt::Write, path::Path};

pub struct Sqlite {
    connection: Connection,
    inner: SqliteInner,
}

impl Sqlite {
    pub fn new(connection: Connection) -> Result<Self> {
        let inner = SqliteInner::new(&connection)?;
        Ok(Self { connection, inner })
    }

    pub fn open(path: &Path) -> Result<Self> {
        Self::new(Connection::open(path)?)
    }

    #[cfg(test)]
    /// Create a temporary in memory database for tests.
    pub fn new_for_test() -> Self {
        Self::new(Connection::open_in_memory().unwrap()).unwrap()
    }
}

impl Database for Sqlite {
    fn prepare_event(&mut self, name: &str, event: &EventDescriptor) -> Result<()> {
        self.inner.prepare_event(&self.connection, name, event)
    }

    fn event_block(&mut self, name: &str) -> Result<Option<u64>> {
        self.inner.event_block(&self.connection, name)
    }

    fn update(&mut self, blocks: &[IndexedBlock], logs: &[database::Log]) -> Result<()> {
        let transaction = self.connection.transaction().context("transaction")?;
        self.inner.update(&transaction, blocks, logs)?;
        transaction.commit().context("commit")
    }
}

/// Columns that every event table has.
/// 1. block number
/// 2. log index
/// 3. transaction index
/// 4. address
const FIXED_COLUMNS: usize = 4;
const FIXED_COLUMNS_SQL: &str = "block_number INTEGER NOT NULL, log_index INTEGER NOT NULL, transaction_index INTEGER NOT NULL, address BLOB NOT NULL";
const FIXED_PRIMARY_KEY: &str = "PRIMARY KEY(block_number ASC, log_index ASC)";

const GET_EVENT_BLOCK_SQL: &str = "SELECT block FROM event_block WHERE event = ?1;";
const SET_EVENT_BLOCK_SQL: &str ="INSERT INTO event_block (event, block) VALUES(?1, ?2) ON CONFLICT(event) DO UPDATE SET block = ?2;";

// Separate type because of lifetime issues when creating transactions. Outer struct only stores the connection itself.
struct SqliteInner {
    /// Invariant: Events in the map have corresponding tables in the database.
    events: HashMap<String, PreparedEvent>,
}

struct PreparedEvent {
    descriptor: EventDescriptor,
    insert_statements: Vec<PreparedStatement>,
}

/// Prepared statements for inserting into event tables. Tables and columns are ordered by `event_visitor`.
///
/// Parameters:
/// - 1: block number
/// - 2: log index
/// - 3: array index if this is an array table (all tables after the first)
/// - 3 + n: n-th event field/column
struct PreparedStatement {
    sql: String,
    /// Number of event fields that map to SQL columns. Does not count FIXED_COLUMNS and array index.
    fields: usize,
}

impl SqliteInner {
    fn new(connection: &Connection) -> Result<Self> {
        let query = "CREATE TABLE IF NOT EXISTS event_block(event TEXT PRIMARY KEY NOT NULL , block INTEGER NOT NULL) STRICT;";
        connection
            .execute(query, ())
            .context("create block table")?;

        connection
            .prepare_cached(SET_EVENT_BLOCK_SQL)
            .context("prepare get_event_block")?;
        connection
            .prepare_cached(GET_EVENT_BLOCK_SQL)
            .context("prepare set_event_block")?;

        Ok(Self {
            events: Default::default(),
        })
    }

    /// Sanitize event name so it will work as a SQL table name.
    fn internal_event_name(name: &str) -> String {
        name.chars().filter(|c| c.is_ascii_alphanumeric()).collect()
    }

    fn _read_event(
        &self,
        _c: &Connection,
        _name: &str,
        _block_number: u64,
        _log_index: u64,
    ) -> Result<Vec<AbiValue>> {
        todo!()
    }

    fn event_block(&self, connection: &Connection, name: &str) -> Result<Option<u64>> {
        let mut statement = connection
            .prepare_cached(GET_EVENT_BLOCK_SQL)
            .context("prepare_cached")?;
        let block: Option<i64> = statement
            .query_row((name,), |row| row.get(0))
            .optional()
            .context("query_row")?;
        block
            .map(u64::try_from)
            .transpose()
            .context("negative block number")
    }

    fn set_event_blocks(&self, connection: &Connection, blocks: &[IndexedBlock]) -> Result<()> {
        let mut statement = connection
            .prepare_cached(SET_EVENT_BLOCK_SQL)
            .context("prepare_cached")?;
        for block in blocks {
            if !self.events.contains_key(block.event) {
                return Err(anyhow!("event {} wasn't prepared", block.event));
            }
            let block_number: i64 = block
                .number
                .try_into()
                .context("block number doesn't fit in i64")?;
            statement
                .execute((block.event, block_number))
                .context("execute")?;
        }
        Ok(())
    }

    fn prepare_event(
        &mut self,
        connection: &Connection,
        name: &str,
        event: &EventDescriptor,
    ) -> Result<()> {
        let name = Self::internal_event_name(name);

        if let Some(existing) = self.events.get(&name) {
            if event != &existing.descriptor {
                return Err(anyhow!(
                    "event {name} already exists with different signature"
                ));
            }
            return Ok(());
        }

        // TODO:
        // - Check that either no table exists or all tables exist and with the right types.
        // - Maybe have `CHECK` clauses to enforce things like address and integers having expected length.

        let tables = event_to_tables(event).context("unsupported event")?;
        let mut sql = String::new();
        writeln!(&mut sql, "BEGIN;").unwrap();
        for (i, table) in tables.iter().enumerate() {
            write!(&mut sql, "CREATE TABLE IF NOT EXISTS {name}_{i} (").unwrap();
            write!(&mut sql, "{FIXED_COLUMNS_SQL}, ").unwrap();
            if i != 0 {
                // This is an Array table.
                write!(&mut sql, "array_index INTEGER NOT NULL, ").unwrap();
            }
            for (i, column) in table.0.iter().enumerate() {
                let type_ = match column.0 {
                    SqlType::Null => unreachable!(),
                    SqlType::Integer => "INTEGER",
                    SqlType::Real => "REAL",
                    SqlType::Text => "TEXT",
                    SqlType::Blob => "BLOB",
                };
                write!(&mut sql, "c{i} {type_}, ").unwrap();
            }
            writeln!(&mut sql, "{FIXED_PRIMARY_KEY}) STRICT;").unwrap();
        }
        write!(&mut sql, "COMMIT;").unwrap();
        tracing::debug!("creating table:\n{}", sql);

        let insert_statements: Vec<PreparedStatement> = tables
            .iter()
            .enumerate()
            .map(|(i, table)| {
                let is_array = i != 0;
                let mut sql = String::new();
                write!(&mut sql, "INSERT INTO {name}_{i} VALUES(").unwrap();
                for i in 0..table.0.len() + FIXED_COLUMNS + is_array as usize {
                    write!(&mut sql, "?{},", i + 1).unwrap();
                }
                assert_eq!(sql.pop(), Some(','));
                write!(&mut sql, ");").unwrap();
                tracing::debug!("creating insert statement:\n{}", sql);
                PreparedStatement {
                    sql,
                    fields: table.0.len(),
                }
            })
            .collect();

        connection.execute_batch(&sql).context("table creation")?;

        // Check that prepared statements are valid. Unfortunately we can't distinguish the statement being wrong from other Sqlite errors like being unable to access the database file on disk.
        for statement in &insert_statements {
            connection
                .prepare_cached(&statement.sql)
                .context("invalid prepared statement")?;
        }

        self.events.insert(
            name,
            PreparedEvent {
                descriptor: event.clone(),
                insert_statements,
            },
        );

        Ok(())
    }

    fn store_event<'a>(
        &self,
        connection: &Connection,
        Log {
            event,
            block_number,
            log_index,
            transaction_index,
            address,
            fields,
        }: &'a Log,
    ) -> Result<()> {
        // TODO:
        // - Check that the abi values matches the stored EventDescriptor.

        let name = Self::internal_event_name(event);
        let event = self.events.get(&name).context("unknown event")?;

        // Outer vec maps to tables. Inner vec maps to columns.
        let mut sql_values: Vec<Vec<ToSqlOutput<'a>>> = vec![vec![]];
        let mut in_array: bool = false;
        let mut visitor = |value: VisitValue<'a>| {
            let sql_value = match value {
                VisitValue::ArrayStart => {
                    sql_values.push(Vec::new());
                    in_array = true;
                    return;
                }
                VisitValue::ArrayEnd => {
                    in_array = false;
                    return;
                }
                VisitValue::Value(AbiValue::Int(v)) => {
                    ToSqlOutput::Owned(SqlValue::Blob(v.to_be_bytes().to_vec()))
                }
                VisitValue::Value(AbiValue::Uint(v)) => {
                    ToSqlOutput::Owned(SqlValue::Blob(v.to_be_bytes().to_vec()))
                }
                VisitValue::Value(AbiValue::Address(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(&v.0))
                }
                VisitValue::Value(AbiValue::Bool(v)) => {
                    ToSqlOutput::Owned(SqlValue::Integer(*v as i64))
                }
                VisitValue::Value(AbiValue::FixedBytes(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(v))
                }
                VisitValue::Value(AbiValue::Function(v)) => ToSqlOutput::Owned(SqlValue::Blob(
                    v.address
                        .0
                        .iter()
                        .copied()
                        .chain(v.selector.0.iter().copied())
                        .collect(),
                )),
                VisitValue::Value(AbiValue::Bytes(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(v))
                }
                VisitValue::Value(AbiValue::String(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(v.as_bytes()))
                }
                _ => unreachable!(),
            };
            (if in_array {
                <[_]>::last_mut
            } else {
                <[_]>::first_mut
            })(&mut sql_values)
            .unwrap()
            .push(sql_value);
        };
        for value in fields {
            crate::event_visitor::visit_value(value, &mut visitor)
        }

        let block_number =
            ToSqlOutput::Owned(SqlValue::Integer((*block_number).try_into().unwrap()));
        let log_index = ToSqlOutput::Owned(SqlValue::Integer((*log_index).try_into().unwrap()));
        let transaction_index =
            ToSqlOutput::Owned(SqlValue::Integer((*transaction_index).try_into().unwrap()));
        let address = ToSqlOutput::Borrowed(SqlValueRef::Blob(&address.0));
        for (i, (statement, values)) in event.insert_statements.iter().zip(sql_values).enumerate() {
            assert!(values.len() % statement.fields == 0);
            let is_array = i != 0;
            let mut statement_ = connection
                .prepare_cached(&statement.sql)
                .context("prepare_cached")?;
            for (i, row) in values.chunks_exact(statement.fields).enumerate() {
                let array_index = if is_array {
                    Some(ToSqlOutput::Owned(SqlValue::Integer(i.try_into().unwrap())))
                } else {
                    None
                };
                let params = rusqlite::params_from_iter(
                    [&block_number, &log_index, &transaction_index, &address]
                        .into_iter()
                        .chain(array_index.as_ref())
                        .chain(row),
                );
                statement_.insert(params).context("insert")?;
            }
        }

        Ok(())
    }

    fn update(
        &self,
        connection: &Connection,
        blocks: &[database::IndexedBlock],
        logs: &[database::Log],
    ) -> Result<()> {
        self.set_event_blocks(connection, blocks)
            .context("set_event_blocks")?;
        for log in logs {
            self.store_event(connection, log).context("store_event")?;
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Table(Vec<Column>);

#[derive(Debug, Eq, PartialEq)]
struct Column(SqlType);

fn event_to_tables(event: &EventDescriptor) -> Result<Vec<Table>> {
    // TODO:
    // - Handle indexed fields.
    // - Make use of field names and potentially tuple names.

    let values = event.inputs.iter().map(|input| &input.field.kind);

    // Nested dynamic arrays are rare and hard to handle. The recursive visiting code and SQL schema becomes more complicated. Handle this properly later.
    for value in values.clone() {
        if has_nested_dynamic_arrays(value) {
            return Err(anyhow!("nested dynamic arrays"));
        }
    }

    let mut tables = vec![Table(vec![])];
    for value in values {
        map_value(&mut tables, value);
    }

    Ok(tables)
}

fn has_nested_dynamic_arrays(value: &AbiKind) -> bool {
    let mut level: u32 = 0;
    let mut max_level: u32 = 0;
    let mut visitor = |visit: VisitKind| match visit {
        VisitKind::ArrayStart => {
            level += 1;
            max_level = std::cmp::max(max_level, level);
        }
        VisitKind::ArrayEnd => level -= 1,
        VisitKind::Value(_) => (),
    };
    crate::event_visitor::visit_kind(value, &mut visitor);
    max_level > 1
}

fn map_value(tables: &mut Vec<Table>, value: &AbiKind) {
    assert!(!tables.is_empty());
    let mut table_index = 0;
    let mut visitor = move |value: VisitKind| {
        let type_ = match value {
            VisitKind::Value(&AbiKind::Int(_)) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Uint(_)) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Address) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Bool) => SqlType::Integer,
            VisitKind::Value(&AbiKind::FixedBytes(_)) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Function) => SqlType::Blob,
            VisitKind::Value(&AbiKind::Bytes) => SqlType::Blob,
            VisitKind::Value(&AbiKind::String) => SqlType::Blob,
            VisitKind::ArrayStart => {
                table_index = tables.len();
                tables.push(Table(vec![]));
                return;
            }
            VisitKind::ArrayEnd => {
                table_index = 0;
                return;
            }
            _ => unreachable!(),
        };
        tables[table_index].0.push(Column(type_));
    };
    crate::event_visitor::visit_kind(value, &mut visitor);
}

#[cfg(test)]
mod tests {
    use solabi::{
        abi::{EventField, Field},
        ethprim::Address,
        function::{ExternalFunction, Selector},
        value::{BitWidth, ByteLength, FixedBytes, Int, Uint},
    };

    use super::*;

    #[test]
    fn new_for_test() {
        Sqlite::new_for_test();
    }

    fn event_descriptor(values: Vec<AbiKind>) -> EventDescriptor {
        EventDescriptor {
            name: Default::default(),
            inputs: values
                .into_iter()
                .map(|value| EventField {
                    field: Field {
                        name: Default::default(),
                        kind: value,
                        components: Default::default(),
                        internal_type: Default::default(),
                    },
                    indexed: Default::default(),
                })
                .collect(),
            anonymous: Default::default(),
        }
    }

    #[test]
    fn map_value_simple() {
        let values = vec![AbiKind::Bytes, AbiKind::Bool];
        let schema = event_to_tables(&event_descriptor(values)).unwrap();
        let expected = vec![Table(vec![Column(SqlType::Blob), Column(SqlType::Integer)])];
        assert_eq!(schema, expected);
    }

    #[test]
    fn map_value_complex_flat() {
        let values = vec![
            AbiKind::Bool,
            AbiKind::Tuple(vec![AbiKind::Bytes, AbiKind::Bool]),
            AbiKind::Bool,
            AbiKind::FixedArray(2, Box::new(AbiKind::Bytes)),
            AbiKind::Bool,
            AbiKind::Tuple(vec![AbiKind::Tuple(vec![AbiKind::FixedArray(
                2,
                Box::new(AbiKind::Bytes),
            )])]),
            AbiKind::Bool,
            AbiKind::FixedArray(
                2,
                Box::new(AbiKind::FixedArray(2, Box::new(AbiKind::Bytes))),
            ),
        ];
        let schema = event_to_tables(&event_descriptor(values)).unwrap();
        let expected = vec![Table(vec![
            Column(SqlType::Integer),
            // first tuple
            Column(SqlType::Blob),
            Column(SqlType::Integer),
            //
            Column(SqlType::Integer),
            // first fixed array
            Column(SqlType::Blob),
            Column(SqlType::Blob),
            //
            Column(SqlType::Integer),
            // second tuple
            Column(SqlType::Blob),
            Column(SqlType::Blob),
            //
            Column(SqlType::Integer),
            // second fixed array
            Column(SqlType::Blob),
            Column(SqlType::Blob),
            Column(SqlType::Blob),
            Column(SqlType::Blob),
        ])];
        assert_eq!(schema, expected);
    }

    #[test]
    fn map_value_array() {
        let values = vec![
            AbiKind::Bool,
            AbiKind::Array(Box::new(AbiKind::Bytes)),
            AbiKind::Bool,
            AbiKind::Array(Box::new(AbiKind::Bool)),
            AbiKind::Bool,
        ];
        let schema = event_to_tables(&event_descriptor(values)).unwrap();
        let expected = vec![
            Table(vec![
                Column(SqlType::Integer),
                Column(SqlType::Integer),
                Column(SqlType::Integer),
            ]),
            Table(vec![Column(SqlType::Blob)]),
            Table(vec![Column(SqlType::Integer)]),
        ];
        assert_eq!(schema, expected);
    }

    #[test]
    fn full_leaf_types() {
        let mut sqlite = Sqlite::new_for_test();
        let values = vec![
            AbiKind::Int(BitWidth::MIN),
            AbiKind::Uint(BitWidth::MIN),
            AbiKind::Address,
            AbiKind::Bool,
            AbiKind::FixedBytes(ByteLength::MIN),
            AbiKind::Function,
            AbiKind::Bytes,
            AbiKind::String,
        ];
        let event = event_descriptor(values);
        sqlite.prepare_event("event1", &event).unwrap();

        let fields = vec![
            AbiValue::Int(Int::new(8, 1i32.into()).unwrap()),
            AbiValue::Uint(Uint::new(8, 2u32.into()).unwrap()),
            AbiValue::Address(Address([3; 20])),
            AbiValue::Bool(true),
            AbiValue::FixedBytes(FixedBytes::new(&[4, 5]).unwrap()),
            AbiValue::Function(ExternalFunction {
                address: Address([6; 20]),
                selector: Selector([7, 8, 9, 10]),
            }),
            AbiValue::Bytes(vec![11, 12]),
            AbiValue::String("abcd".to_string()),
        ];
        sqlite
            .inner
            .store_event(
                &sqlite.connection,
                &Log {
                    event: "event1",
                    block_number: 1,
                    log_index: 2,
                    transaction_index: 3,
                    address: Address([4; 20]),
                    fields,
                },
            )
            .unwrap();

        let mut statement = sqlite.connection.prepare("SELECT * from event1_0").unwrap();
        let mut rows = statement.query(()).unwrap();
        while let Some(row) = rows.next().unwrap() {
            assert_eq!(row.as_ref().column_count(), FIXED_COLUMNS + 8);
            for i in 0..row.as_ref().column_count() {
                let column = row.get_ref(i).unwrap();
                println!("{:?}", column);
            }
            println!();
        }
    }

    #[test]
    fn event_blocks() {
        let mut sqlite = Sqlite::new_for_test();
        sqlite
            .prepare_event("event", &event_descriptor(vec![]))
            .unwrap();
        let result = sqlite.event_block("event").unwrap();
        assert_eq!(result, None);
        let blocks = IndexedBlock {
            event: "event",
            number: 1,
        };
        sqlite.update(&[blocks], &[]).unwrap();
        let result = sqlite.event_block("event").unwrap();
        assert_eq!(result, Some(1));
    }
}
