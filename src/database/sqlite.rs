use crate::database::common::{
    check_descriptor, event_exists_with_same_signature, push_sql_value, PreparedEvent,
};
use {
    crate::database::{
        self,
        date_util::systemtime_to_string,
        event_to_tables::Table,
        event_visitor::{self, VisitValue},
        BlockTime, Database, Log,
    },
    anyhow::{anyhow, Context, Result},
    futures::{future::BoxFuture, FutureExt},
    rusqlite::{
        types::{ToSqlOutput, Type as SqlType, Value as SqlValue, ValueRef as SqlValueRef},
        Connection, Transaction,
    },
    solabi::{
        abi::EventDescriptor,
        value::{Value as AbiValue, ValueKind as AbiKind},
    },
    std::{collections::HashMap, fmt::Write},
};

pub struct Sqlite {
    connection: Connection,
    inner: SqliteInner,
}

impl Sqlite {
    pub fn new(connection: Connection) -> Result<Self> {
        let inner = SqliteInner::new(&connection)?;
        Ok(Self { connection, inner })
    }

    /// Opens a new SQLite database backend for the specified connection string.
    /// The connection string can either be a file path or a `file://` URL (see
    /// <https://www.sqlite.org/uri.html> for more information).
    pub fn open(connection: &str) -> Result<Self> {
        let connection = Connection::open(connection)?;
        Self::new(connection)
    }

    #[cfg(test)]
    /// Create a temporary in memory database for tests.
    pub fn new_for_test() -> Self {
        Self::new(Connection::open_in_memory().unwrap()).unwrap()
    }
}

impl Database for Sqlite {
    fn prepare_event<'a>(
        &'a mut self,
        name: &'a str,
        event: &'a EventDescriptor,
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            let transaction = self.connection.transaction().context("transaction")?;
            self.inner.prepare_event(&transaction, name, event)?;
            transaction.commit().context("commit")
        }
        .boxed()
    }

    fn event_block<'a>(&'a mut self, name: &'a str) -> BoxFuture<'a, Result<database::Block>> {
        async move { self.inner.event_block(&self.connection, name) }.boxed()
    }

    fn update<'a>(
        &'a mut self,
        blocks: &'a [database::EventBlock],
        logs: &'a [Log],
        block_times: &'a [BlockTime],
        transactions: &'a [database::Transaction],
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            let transaction = self.connection.transaction().context("transaction")?;
            self.inner
                .update(&transaction, blocks, logs, block_times, transactions)?;
            transaction.commit().context("commit")
        }
        .boxed()
    }

    fn remove<'a>(&'a mut self, uncles: &'a [database::Uncle]) -> BoxFuture<'a, Result<()>> {
        async move {
            let transaction = self.connection.transaction().context("transaction")?;
            self.inner.remove(&transaction, uncles)?;
            transaction.commit().context("commit")
        }
        .boxed()
    }
}

/// Columns that every event table has.
const FIXED_COLUMNS: &str = "block_number INTEGER NOT NULL, log_index INTEGER NOT NULL, \
                             transaction_index INTEGER NOT NULL, address BLOB NOT NULL";
const FIXED_COLUMNS_COUNT: usize = 4;
const PRIMARY_KEY: &str = "block_number ASC, log_index ASC";

/// Column for array tables.
const ARRAY_COLUMN: &str = "array_index INTEGER NOT NULL";
const PRIMARY_KEY_ARRAY: &str = "block_number ASC, log_index ASC, array_index ASC";

const CREATE_BLOCKS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS blocks
(
    number INTEGER PRIMARY KEY,
    time   TEXT    NOT NULL
);"#;

const INSERT_BLOCK: &str = "INSERT OR IGNORE INTO blocks (number, time) VALUES (?1, ?2);";

const REMOVE_BLOCKS_FROM: &str = "DELETE FROM blocks WHERE number >=?1;";

const CREATE_TRANSACTIONS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS transactions
(
    block_number INTEGER NOT NULL,
    "index"      INTEGER NOT NULL,
    hash         BLOB    NOT NULL,
    "from"       BLOB    NOT NULL,
    "to"         BLOB,
    PRIMARY KEY (block_number, "index")
);"#; // Can not add a new line here or will get Error:
      // https://docs.rs/rusqlite/0.30.0/rusqlite/enum.Error.html#variant.MultipleStatement

const INSERT_TRANSACTION: &str = r#"INSERT INTO transactions (block_number, "index", hash, "from", "to")
                                    VALUES (?1, ?2, ?3, ?4, ?5)
                                    ON CONFLICT DO NOTHING;"#;

const REMOVE_TRANSACTIONS_FROM: &str = "DELETE FROM transactions WHERE block_number >=?1;";

const CREATE_EVENT_BLOCK_TABLE: &str = "CREATE TABLE IF NOT EXISTS _event_block(event TEXT \
                                        PRIMARY KEY NOT NULL, indexed INTEGER NOT NULL, finalized \
                                        INTEGER NOT NULL) STRICT;";
const GET_EVENT_BLOCK: &str = "SELECT indexed, finalized FROM _event_block WHERE event = ?1;";
const NEW_EVENT_BLOCK: &str = "INSERT INTO _event_block (event, indexed, finalized) VALUES(?1, 0, \
                               0) ON CONFLICT(event) DO NOTHING;";
const SET_EVENT_BLOCK: &str =
    "UPDATE _event_block SET indexed = ?2, finalized = ?3 WHERE event = ?1;";
const SET_INDEXED_BLOCK: &str = "UPDATE _event_block SET indexed = ?2 WHERE event = ?1";

const TABLE_EXISTS: &str =
    "SELECT COUNT(*) > 0 FROM sqlite_schema WHERE type = 'table' AND name = ?1";

// Separate type because of lifetime issues when creating transactions. Outer
// struct only stores the connection itself.
struct SqliteInner {
    /// Invariant: Events in the map have corresponding tables in the database.
    ///
    /// The key is the `name` argument when the event was passed into
    /// `prepare_event`.
    events: HashMap<String, PreparedEvent<InsertStatement, String>>,
}

/// Parameters:
/// - 1: block number
/// - 2: log index
/// - 3: array index if this is an array table (all tables after the first)
/// - 3 + n: n-th event field/column
#[derive(Debug)]
struct InsertStatement {
    sql: String,
    /// Number of event fields that map to SQL columns. Does not count
    /// FIXED_COLUMNS and array index.
    fields: usize,
}

impl SqliteInner {
    fn new(connection: &Connection) -> Result<Self> {
        connection
            .execute(CREATE_EVENT_BLOCK_TABLE, ())
            .context("create event_block table")?;

        connection
            .prepare_cached(GET_EVENT_BLOCK)
            .context("prepare get_event_block")?;
        connection
            .prepare_cached(SET_EVENT_BLOCK)
            .context("prepare set_event_block")?;
        connection
            .prepare_cached(SET_INDEXED_BLOCK)
            .context("prepare set_indexed_block")?;
        connection
            .prepare_cached(TABLE_EXISTS)
            .context("prepare table_exists")?;

        connection
            .execute(CREATE_BLOCKS_TABLE, [])
            .context("create blocks table")?;
        connection
            .execute(CREATE_TRANSACTIONS_TABLE, [])
            .context("create transactions table")?;

        let mut new_event_block = connection
            .prepare_cached(NEW_EVENT_BLOCK)
            .context("prepare new_event_block")?;
        new_event_block
            .execute((&"blocks",))
            .context("add blocks to _event_blocks")?;
        new_event_block
            .execute((&"transactions",))
            .context("add transactions to _event_blocks")?;

        Ok(Self {
            events: Default::default(),
        })
    }

    /*
        fn read_event(
            &self,
            c: &Connection,
            name: &str,
            block_number: u64,
            log_index: u64,
        ) -> Result<Vec<AbiValue>> {
            let name = Self::internal_event_name(name);
            let event = self.events.get(&name).context("unknown event")?;

            todo!()
        }
    */

    fn event_block(&self, con: &Connection, name: &str) -> Result<database::Block> {
        let mut statement = con
            .prepare_cached(GET_EVENT_BLOCK)
            .context("prepare_cached")?;
        let block: (i64, i64) = statement
            .query_row((name,), |row| Ok((row.get(0)?, row.get(1)?)))
            .context("query_row")?;
        Ok(database::Block {
            indexed: block.0.try_into().context("indexed out of bounds")?,
            finalized: block.1.try_into().context("finalized out of bounds")?,
        })
    }

    fn set_event_blocks(&self, con: &Transaction, blocks: &[database::EventBlock]) -> Result<()> {
        let mut statement = con
            .prepare_cached(SET_EVENT_BLOCK)
            .context("prepare_cached")?;
        for block in blocks {
            if block.is_event() && !self.events.contains_key(block.event) {
                return Err(anyhow!("event {} wasn't prepared", block.event));
            }
            let indexed: i64 = block
                .block
                .indexed
                .try_into()
                .context("indexed out of bounds")?;
            let finalized: i64 = block
                .block
                .finalized
                .try_into()
                .context("finalized out of bounds")?;
            let rows = statement
                .execute((block.event, indexed, finalized))
                .context("execute")?;
            if rows != 1 {
                return Err(anyhow!(
                    "query unexpectedly changed {rows} rows instead of 1"
                ));
            }
        }
        Ok(())
    }

    fn prepare_event(
        &mut self,
        con: &Transaction,
        name: &str,
        event: &EventDescriptor,
    ) -> Result<()> {
        if event_exists_with_same_signature(&self.events, name, event)? {
            return Ok(());
        }

        let tables =
            database::event_to_tables::event_to_tables(name, event).context("unsupported event")?;
        let name = &tables.primary.name;

        let create_table = |is_array: bool, table: &Table| {
            let mut sql = String::new();
            write!(&mut sql, "CREATE TABLE IF NOT EXISTS {} (", table.name).unwrap();
            write!(&mut sql, "{FIXED_COLUMNS}, ").unwrap();
            if is_array {
                write!(&mut sql, "{ARRAY_COLUMN}, ").unwrap();
            }
            for column in table.columns.iter() {
                write!(&mut sql, "{}", column.name).unwrap();
                let type_ = match abi_kind_to_sql_type(column.kind).unwrap() {
                    SqlType::Null => unreachable!(),
                    SqlType::Integer => "INTEGER",
                    SqlType::Real => "REAL",
                    SqlType::Text => "TEXT",
                    SqlType::Blob => "BLOB",
                };
                write!(&mut sql, " {type_} NOT NULL, ").unwrap();
            }
            let primary_key = if is_array {
                PRIMARY_KEY_ARRAY
            } else {
                PRIMARY_KEY
            };
            write!(&mut sql, "PRIMARY KEY({primary_key})) STRICT;").unwrap();
            tracing::debug!("creating table:\n{}", sql);
            con.execute(&sql, ()).context("execute create_table")
        };
        create_table(false, &tables.primary)?;
        for table in &tables.dynamic_arrays {
            create_table(true, table)?;
        }

        let mut new_event_block = con
            .prepare_cached(NEW_EVENT_BLOCK)
            .context("prepare new_event_block")?;
        new_event_block
            .execute((&name,))
            .context("execute new_event_block")?;

        let insert_statements: Vec<InsertStatement> = std::iter::once((false, &tables.primary))
            .chain(std::iter::repeat(true).zip(&tables.dynamic_arrays))
            .clone()
            .map(|(is_array, table)| {
                let mut sql = String::new();
                write!(&mut sql, "INSERT INTO {} VALUES(", table.name).unwrap();
                for i in 0..table.columns.len() + FIXED_COLUMNS_COUNT + is_array as usize {
                    write!(&mut sql, "?{},", i + 1).unwrap();
                }
                assert_eq!(sql.pop(), Some(','));
                write!(&mut sql, ");").unwrap();
                tracing::debug!("creating insert statement:\n{}", sql);
                InsertStatement {
                    sql,
                    fields: table.columns.len(),
                }
            })
            .collect();

        let remove_statements: Vec<String> = std::iter::once(&tables.primary)
            .chain(&tables.dynamic_arrays)
            .map(|table| format!("DELETE FROM {} WHERE block_number >= ?1;", table.name))
            .collect();

        // Check that prepared statements are valid. Unfortunately we can't distinguish
        // the statement being wrong from other Sqlite errors like being unable to
        // access the database file on disk.
        for statement in &insert_statements {
            con.prepare_cached(&statement.sql)
                .context("invalid prepared insert statement")?;
        }
        for statement in &remove_statements {
            con.prepare_cached(statement)
                .context("invalid prepared remove statement")?;
        }

        self.events.insert(
            name.clone(),
            PreparedEvent {
                descriptor: event.clone(),
                insert_statements,
                remove_statements,
            },
        );

        Ok(())
    }

    fn store_event<'a>(
        &self,
        conn: &Transaction,
        Log {
            event,
            block_number,
            log_index,
            transaction_index,
            address,
            fields,
        }: &'a Log,
    ) -> Result<()> {
        let event = self.events.get(*event).context("unknown event")?;

        let len = fields.len();
        let expected_len = event.descriptor.inputs.len();
        if fields.len() != expected_len {
            return Err(anyhow!(
                "event value has {len} fields but should have {expected_len}"
            ));
        }
        check_descriptor(fields, event)?;

        // Outer vec maps to tables. Inner vec maps to (array element count, columns).
        let mut sql_values: Vec<(Option<usize>, Vec<ToSqlOutput<'a>>)> = vec![(None, vec![])];
        let mut in_array: bool = false;
        let mut visitor = |value: VisitValue<'a>| {
            let sql_value = match value {
                VisitValue::ArrayStart(len) => {
                    sql_values.push((Some(len), Vec::new()));
                    in_array = true;
                    return;
                }
                VisitValue::ArrayEnd => {
                    in_array = false;
                    return;
                }
                VisitValue::Value(AbiValue::Int(v)) => {
                    ToSqlOutput::Owned(SqlValue::Blob(v.get().to_be_bytes().to_vec()))
                }
                VisitValue::Value(AbiValue::Uint(v)) => {
                    ToSqlOutput::Owned(SqlValue::Blob(v.get().to_be_bytes().to_vec()))
                }
                VisitValue::Value(AbiValue::Address(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(&v.0))
                }
                VisitValue::Value(AbiValue::Bool(v)) => {
                    ToSqlOutput::Owned(SqlValue::Integer(*v as i64))
                }
                VisitValue::Value(AbiValue::FixedBytes(v)) => {
                    ToSqlOutput::Borrowed(SqlValueRef::Blob(v.as_bytes()))
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
            push_sql_value(&mut sql_values, in_array, sql_value)
        };
        for value in fields {
            event_visitor::visit_value(value, &mut visitor)
        }

        let block_number = ToSqlOutput::Owned(SqlValue::Integer((*block_number).try_into()?));
        let log_index = ToSqlOutput::Owned(SqlValue::Integer((*log_index).try_into()?));
        let transaction_index =
            ToSqlOutput::Owned(SqlValue::Integer((*transaction_index).try_into()?));
        let address = ToSqlOutput::Borrowed(SqlValueRef::Blob(&address.0));
        for (statement, (array_element_count, values)) in
            event.insert_statements.iter().zip(sql_values)
        {
            let mut statement_ = conn
                .prepare_cached(&statement.sql)
                .context("prepare_cached event")?;
            let is_array = array_element_count.is_some();
            let array_element_count = array_element_count.unwrap_or(1);
            assert_eq!(statement.fields * array_element_count, values.len());
            for i in 0..array_element_count {
                let row = &values[i * statement.fields..][..statement.fields];
                let array_index = if is_array {
                    Some(ToSqlOutput::Owned(SqlValue::Integer(i.try_into()?)))
                } else {
                    None
                };
                let params = rusqlite::params_from_iter(
                    [&block_number, &log_index, &transaction_index, &address]
                        .into_iter()
                        .chain(array_index.as_ref())
                        .chain(row),
                );
                statement_.insert(params).context("insert event")?;
            }
        }

        Ok(())
    }

    fn store_block(&self, conn: &Transaction, block_time: &BlockTime) -> Result<()> {
        let number = ToSqlOutput::Owned(SqlValue::Integer(block_time.number.try_into()?));
        let time = ToSqlOutput::Owned(SqlValue::Text(systemtime_to_string(
            block_time.timestamp,
            None,
        )));
        conn.prepare_cached(INSERT_BLOCK)
            .context("prepare_cached block insert")?
            .execute([number, time])
            .context("insert block")?;
        Ok(())
    }

    fn store_transaction(&self, conn: &Transaction, tx: &database::Transaction) -> Result<()> {
        let block_number = ToSqlOutput::Owned(SqlValue::Integer(tx.block_number.try_into()?));
        let index = ToSqlOutput::Owned(SqlValue::Integer(tx.index.try_into()?));
        let hash = ToSqlOutput::Owned(SqlValue::Blob(tx.hash.to_vec()));
        let from = ToSqlOutput::Owned(SqlValue::Blob(tx.from.to_vec()));
        let to = ToSqlOutput::Owned(match tx.to {
            Some(val) => SqlValue::Blob(val.to_vec()),
            None => SqlValue::Null,
        });

        conn.prepare_cached(INSERT_TRANSACTION)
            .context("prepare_cached transaction insert")?
            .execute([block_number, index, hash, from, to])
            .context("insert transaction")?;
        Ok(())
    }

    fn update(
        &self,
        con: &Transaction,
        blocks: &[database::EventBlock],
        logs: &[Log],
        block_times: &[BlockTime],
        transactions: &[database::Transaction],
    ) -> Result<()> {
        self.set_event_blocks(con, blocks)
            .context("set_event_blocks")?;
        for log in logs {
            self.store_event(con, log).context("store_event")?;
        }
        for block_time in block_times {
            self.store_block(con, block_time).context("store_block")?;
        }
        for tx in transactions {
            self.store_transaction(con, tx)
                .context("store_transaction")?;
        }
        Ok(())
    }

    fn remove(&self, connection: &Connection, uncles: &[database::Uncle]) -> Result<()> {
        let mut set_indexed_block: rusqlite::CachedStatement<'_> = connection
            .prepare_cached(SET_INDEXED_BLOCK)
            .context("prepare_cached set_indexed_block")?;
        for uncle in uncles {
            if uncle.number == 0 {
                return Err(anyhow!("block 0 got uncled"));
            }
            let block = i64::try_from(uncle.number).context("block out of bounds")?;
            let parent_block = block - 1;
            let prepared = self.events.get(uncle.event).context("unprepared event")?;
            for remove_statement in &prepared.remove_statements {
                let mut remove_statement = connection
                    .prepare_cached(remove_statement)
                    .context("prepare_cached remove_statement")?;
                remove_statement
                    .execute((block,))
                    .context("execute remove_statement")?;
                set_indexed_block
                    .execute((uncle.event, parent_block))
                    .context("execute set_indexed_block")?;
            }

            // Remove blocks and transactions as well.
            let mut remove_statement = connection.prepare_cached(REMOVE_BLOCKS_FROM)?;
            remove_statement
                .execute((block,))
                .context("execute remove_statement (blocks)")?;
            set_indexed_block.execute(("blocks", parent_block))?;

            let mut remove_statement = connection.prepare_cached(REMOVE_TRANSACTIONS_FROM)?;
            remove_statement
                .execute((block,))
                .context("execute remove_statement (transactions)")?;
            set_indexed_block.execute(("transactions", parent_block))?;
        }
        Ok(())
    }
}

fn abi_kind_to_sql_type(value: &AbiKind) -> Option<SqlType> {
    match value {
        AbiKind::Int(_) => Some(SqlType::Blob),
        AbiKind::Uint(_) => Some(SqlType::Blob),
        AbiKind::Address => Some(SqlType::Blob),
        AbiKind::Bool => Some(SqlType::Integer),
        AbiKind::FixedBytes(_) => Some(SqlType::Blob),
        AbiKind::Function => Some(SqlType::Blob),
        AbiKind::Bytes => Some(SqlType::Blob),
        AbiKind::String => Some(SqlType::Blob),
        AbiKind::FixedArray(_, _) | AbiKind::Tuple(_) | AbiKind::Array(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solabi::{
            digest,
            ethprim::Address,
            function::{ExternalFunction, Selector},
            value::{Array, FixedBytes, Int, Uint},
        },
        std::time::SystemTime,
    };

    #[test]
    fn new_for_test() {
        Sqlite::new_for_test();
    }

    fn print_table(con: &Connection, table: &str) {
        let mut statement = con.prepare(&format!("SELECT * FROM {table}")).unwrap();
        let mut rows = statement.query(()).unwrap();
        while let Some(row) = rows.next().unwrap() {
            for i in 0..row.as_ref().column_count() {
                let name = row.as_ref().column_name(i).unwrap();
                let value = row.get_ref(i).unwrap();
                println!("{name}: {value:?}");
            }
            println!();
        }
    }

    #[tokio::test]
    async fn full_leaf_types() {
        let mut sqlite = Sqlite::new_for_test();
        let event = r#"
            event Event (
                int256,
                uint256,
                address,
                bool,
                bytes1,
                function,
                bytes,
                string
            )
        "#;
        let event = EventDescriptor::parse_declaration(event).unwrap();
        sqlite.prepare_event("event", &event).await.unwrap();

        let fields = vec![
            AbiValue::Int(Int::new(256, 1i32.into()).unwrap()),
            AbiValue::Uint(Uint::new(256, 2u32.into()).unwrap()),
            AbiValue::Address(Address([3; 20])),
            AbiValue::Bool(true),
            AbiValue::FixedBytes(FixedBytes::new(&[4]).unwrap()),
            AbiValue::Function(ExternalFunction {
                address: Address([6; 20]),
                selector: Selector([7, 8, 9, 10]),
            }),
            AbiValue::Bytes(vec![11, 12]),
            AbiValue::String("abcd".to_string()),
        ];
        sqlite
            .update(
                &[],
                &[Log {
                    event: "event",
                    block_number: 1,
                    log_index: 2,
                    transaction_index: 3,
                    address: Address([4; 20]),
                    fields,
                }],
                &[BlockTime {
                    number: 1,
                    timestamp: SystemTime::UNIX_EPOCH,
                }],
                &[database::Transaction {
                    block_number: 1,
                    index: 2,
                    hash: digest!(
                        "0x0000000000000000000000000101010101010101010101010101010101010101"
                    ),
                    from: Address([4; 20]),
                    to: Some(Address([4; 20])),
                }],
            )
            .await
            .unwrap();

        print_table(&sqlite.connection, "event");
        print_table(&sqlite.connection, "blocks");
        print_table(&sqlite.connection, "transactions");
    }

    #[tokio::test]
    async fn with_array() {
        let mut sqlite = Sqlite::new_for_test();
        let event = r#"
            event Event (
                (bool, string)[]
            )
        "#;
        let event = EventDescriptor::parse_declaration(event).unwrap();
        sqlite.prepare_event("event", &event).await.unwrap();

        let log = Log {
            event: "event",
            block_number: 0,
            fields: vec![AbiValue::Array(
                Array::from_values(vec![
                    AbiValue::Tuple(vec![
                        AbiValue::Bool(false),
                        AbiValue::String("hello".to_string()),
                    ]),
                    AbiValue::Tuple(vec![
                        AbiValue::Bool(true),
                        AbiValue::String("world".to_string()),
                    ]),
                ])
                .unwrap(),
            )],
            ..Default::default()
        };
        sqlite.update(&[], &[log], &[], &[]).await.unwrap();

        let log = Log {
            event: "event",
            block_number: 1,
            fields: vec![AbiValue::Array(
                Array::new(AbiKind::Tuple(vec![AbiKind::Bool, AbiKind::String]), vec![]).unwrap(),
            )],
            ..Default::default()
        };
        sqlite.update(&[], &[log], &[], &[]).await.unwrap();

        print_table(&sqlite.connection, "event");
        print_table(&sqlite.connection, "event_array_0");
    }

    #[tokio::test]
    async fn event_blocks() {
        let mut sqlite = Sqlite::new_for_test();
        let event = EventDescriptor::parse_declaration("event Event()").unwrap();
        sqlite.prepare_event("event", &event).await.unwrap();
        let result = sqlite.event_block("event").await.unwrap();
        assert_eq!(result.indexed, 0);
        assert_eq!(result.finalized, 0);
        let blocks = database::EventBlock {
            event: "event",
            block: database::Block {
                indexed: 2,
                finalized: 3,
            },
        };
        sqlite.update(&[blocks], &[], &[], &[]).await.unwrap();
        let result = sqlite.event_block("event").await.unwrap();
        assert_eq!(result.indexed, 2);
        assert_eq!(result.finalized, 3);
    }

    fn count_rows(db: &Sqlite, table_name: &str) -> i64 {
        let count: i64 = db
            .connection
            .query_row(&format!("SELECT COUNT(*) FROM {}", table_name), (), |row| {
                row.get(0)
            })
            .unwrap();
        count
    }

    #[tokio::test]
    async fn remove() {
        let mut sqlite = Sqlite::new_for_test();

        let event = EventDescriptor::parse_declaration("event Event()").unwrap();
        sqlite.prepare_event("event", &event).await.unwrap();
        sqlite.prepare_event("eventAAA", &event).await.unwrap();
        sqlite
            .update(
                &[],
                &[
                    Log {
                        event: "event",
                        block_number: 1,
                        ..Default::default()
                    },
                    Log {
                        event: "event",
                        block_number: 2,
                        ..Default::default()
                    },
                    Log {
                        event: "event",
                        block_number: 5,
                        ..Default::default()
                    },
                    Log {
                        event: "event",
                        block_number: 6,
                        ..Default::default()
                    },
                ],
                &[
                    BlockTime {
                        number: 5,
                        timestamp: SystemTime::UNIX_EPOCH,
                    },
                    BlockTime {
                        number: 6,
                        timestamp: SystemTime::UNIX_EPOCH,
                    },
                ],
                &[
                    database::Transaction {
                        block_number: 5,
                        index: 2,
                        hash: digest!(
                            "0x0000000000000000000000000000000000000000000000000000000000000111"
                        ),
                        from: Address([4; 20]),
                        to: Some(Address([4; 20])),
                    },
                    database::Transaction {
                        block_number: 6,
                        index: 2,
                        hash: digest!(
                            "0x0000000000000000000000000000000000000000000000000000000000000222"
                        ),
                        from: Address([4; 20]),
                        to: Some(Address([4; 20])),
                    },
                ],
            )
            .await
            .unwrap();

        let rows = |sqlite: &Sqlite| {
            let count: i64 = sqlite
                .connection
                .query_row("SELECT COUNT(*) FROM event", (), |row| row.get(0))
                .unwrap();
            count
        };
        assert_eq!(rows(&sqlite), 4);
        assert_eq!(count_rows(&sqlite, "transactions"), 2);
        assert_eq!(count_rows(&sqlite, "blocks"), 2);

        sqlite
            .remove(&[database::Uncle {
                event: "event",
                number: 6,
            }])
            .await
            .unwrap();
        assert_eq!(rows(&sqlite), 3);
        assert_eq!(count_rows(&sqlite, "transactions"), 1);
        assert_eq!(count_rows(&sqlite, "blocks"), 1);

        sqlite
            .remove(&[database::Uncle {
                event: "eventAAA",
                number: 1,
            }])
            .await
            .unwrap();
        assert_eq!(rows(&sqlite), 3);
        assert_eq!(count_rows(&sqlite, "transactions"), 0);
        assert_eq!(count_rows(&sqlite, "blocks"), 0);
        sqlite
            .remove(&[database::Uncle {
                event: "event",
                number: 1,
            }])
            .await
            .unwrap();
        assert_eq!(rows(&sqlite), 0);
    }
}
