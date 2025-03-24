use crate::database::common::{check_descriptor, event_exists_with_same_signature, push_sql_value, PreparedEvent};
use {
    crate::database::{
        self,
        event_to_tables::Table,
        event_visitor::{self, VisitValue},
        Database, Log,
    },
    anyhow::{anyhow, Context, Result},
    futures::{future::BoxFuture, FutureExt},
    pg_bigdecimal::{BigDecimal, PgNumeric},
    solabi::{
        abi::EventDescriptor,
        value::{Value as AbiValue, ValueKind as AbiKind},
    },
    std::{collections::HashMap, fmt::Write, str::FromStr},
};

pub struct Postgres {
    client: tokio_postgres::Client,
    /// Invariant: Events in the map have corresponding tables in the database.
    ///
    /// The key is the `name` argument when the event was passed into
    /// `prepare_event`.
    events: HashMap<String, PreparedEvent<InsertStatement, tokio_postgres::Statement>>,

    get_event_block: tokio_postgres::Statement,
    set_event_block: tokio_postgres::Statement,
    set_indexed_block: tokio_postgres::Statement,
    new_event_block: tokio_postgres::Statement,
}

async fn connect(params: &str) -> Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(params, tokio_postgres::NoTls)
        .await
        .context("connect client")?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

impl Postgres {
    pub async fn connect(connection_str: &str, schema: &str) -> Result<Self> {
        tracing::debug!("opening postgres database with schema {schema}");
        let client = connect(connection_str).await.context("connect")?;
        // create and set db schema
        client
            .execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"), &[])
            .await
            .context("create schema")?;
        client
            .execute(&format!("SET search_path TO {schema};"), &[])
            .await
            .context("set schema")?;
        client
            .execute(CREATE_EVENT_BLOCK_TABLE, &[])
            .await
            .context("create event_block table")?;

        let get_event_block = client
            .prepare(GET_EVENT_BLOCK)
            .await
            .context("prepare GET_EVENT_BLOCK")?;
        let set_event_block = client
            .prepare(SET_EVENT_BLOCK)
            .await
            .context("prepare SET_EVENT_BLOCK")?;
        let set_indexed_block = client
            .prepare(SET_INDEXED_BLOCK)
            .await
            .context("prepare SET_INDEXED_BLOCK")?;
        let new_event_block = client
            .prepare(NEW_EVENT_BLOCK)
            .await
            .context("prepare new_event_block")?;

        // Prepare blocks and transactions tables:
        client
            .execute(CREATE_BLOCKS_TABLE, &[])
            .await
            .context("create blocks table")?;

        client
            .execute(CREATE_TRANSACTIONS_TABLE, &[])
            .await
            .context("create transactions table")?;
        client
            .execute(&new_event_block, &[&"blocks"])
            .await
            .context("add blocks to _event_blocks")?;
        client
            .execute(&new_event_block, &[&"transactions"])
            .await
            .context("add transactions to _event_blocks")?;

        Ok(Self {
            client,
            events: Default::default(),
            get_event_block,
            set_event_block,
            set_indexed_block,
            new_event_block,
        })
    }
}

fn validate_rows(rows: u64) -> Result<()> {
    if rows != 1 {
        return Err(anyhow!(
            "query unexpectedly changed {rows} rows instead of 1"
        ));
    }
    Ok(())
}

impl Database for Postgres {
    fn prepare_event<'a>(
        &'a mut self,
        name: &'a str,
        event: &'a EventDescriptor,
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            let transaction = self.client.transaction().await.context("transaction")?;
            // TODO:
            // - Check that either no table exists or all tables exist and with the right
            //   types.
            // - Maybe have `CHECK` clauses to enforce things like address and integers
            //   having expected length.
            // - Maybe store serialized event descriptor in the database so we can load and
            //   check it.

            if event_exists_with_same_signature(&self.events, name, event)? {
                return Ok(());
            }

            let tables = database::event_to_tables::event_to_tables(name, event)
                .context("unsupported event")?;
            let name = &tables.primary.name;
            Self::create_table(&transaction, false, &tables.primary).await?;
            for table in &tables.dynamic_arrays {
                Self::create_table(&transaction, true, table).await?;
            }

            transaction
                .execute(&self.new_event_block, &[name])
                .await
                .context("execute new_event_block")?;

            let mut insert_statements = Vec::new();
            for (is_array, table) in std::iter::once((false, &tables.primary))
                .chain(std::iter::repeat(true).zip(&tables.dynamic_arrays))
            {
                let mut sql = String::new();
                write!(&mut sql, "INSERT INTO {} VALUES(", table.name).unwrap();
                for i in 0..table.columns.len() + FIXED_COLUMNS_COUNT + is_array as usize {
                    write!(&mut sql, "${},", i + 1).unwrap();
                }
                assert_eq!(sql.pop(), Some(','));
                write!(&mut sql, ");").unwrap();
                tracing::debug!("creating insert statement:\n{}", sql);
                insert_statements.push(InsertStatement {
                    sql: transaction
                        .prepare(&sql)
                        .await
                        .context(format!("prepare {}", sql))?,
                    fields: table.columns.len(),
                });
            }

            let mut remove_statements = Vec::new();
            for table in std::iter::once(&tables.primary).chain(&tables.dynamic_arrays) {
                let sql = format!("DELETE FROM {} WHERE block_number >= $1;", table.name);
                remove_statements.push(
                    transaction
                        .prepare(&sql)
                        .await
                        .context(format!("prepare {}", sql))?,
                );
            }

            self.events.insert(
                name.clone(),
                PreparedEvent {
                    descriptor: event.clone(),
                    insert_statements,
                    remove_statements,
                },
            );

            transaction.commit().await.context("commit")
        }
        .boxed()
    }

    fn event_block<'a>(&'a mut self, name: &'a str) -> BoxFuture<'a, Result<database::Block>> {
        async move {
            let row = self
                .client
                .query_one(&self.get_event_block, &[&name])
                .await
                .context("query GET_EVENT_BLOCK")?;
            let block: (i64, i64) = (row.try_get(0)?, row.try_get(1)?);
            Ok(database::Block {
                indexed: block.0.try_into().context("indexed out of bounds")?,
                finalized: block.1.try_into().context("finalized out of bounds")?,
            })
        }
        .boxed()
    }

    fn update<'a>(
        &'a mut self,
        blocks: &'a [database::EventBlock],
        logs: &'a [Log],
        block_times: &'a [database::BlockTime],
        transactions: &'a [database::Transaction],
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            let mut transaction = self.client.transaction().await.context("transaction")?;

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
                let rows = transaction
                    .execute(&self.set_event_block, &[&block.event, &indexed, &finalized])
                    .await
                    .context("execute SET_EVENT_BLOCK")?;
                validate_rows(rows)?;
                let rows = transaction
                    .execute(&self.set_event_block, &[&"blocks", &indexed, &finalized])
                    .await
                    .context("execute SET_EVENT_BLOCK")?;
                validate_rows(rows)?;
                let rows = transaction
                    .execute(
                        &self.set_event_block,
                        &[&"transactions", &indexed, &finalized],
                    )
                    .await
                    .context("execute SET_EVENT_BLOCK")?;
                validate_rows(rows)?;
            }

            for log in logs {
                Self::store_event(&mut transaction, &self.events, log)
                    .await
                    .context(format!("store_event {:?}", log))?;
            }
            // Store blocks
            for block_time in block_times {
                Self::store_block(&mut transaction, block_time)
                    .await
                    .context(format!("store_block {:?}", block_time))?;
            }
            // Store evm transactions
            for tx in transactions {
                Self::store_transaction(&mut transaction, tx)
                    .await
                    .context(format!("store_transaction {:?}", tx))?;
            }
            transaction.commit().await.context("commit")
        }
        .boxed()
    }

    fn remove<'a>(&'a mut self, uncles: &'a [database::Uncle]) -> BoxFuture<'a, Result<()>> {
        async move {
            let transaction = self.client.transaction().await.context("transaction")?;

            for uncle in uncles {
                if uncle.number == 0 {
                    return Err(anyhow!("block 0 got uncled"));
                }
                let block = i64::try_from(uncle.number).context("block out of bounds")?;
                let parent_block = block - 1;
                let prepared = self.events.get(uncle.event).context("unprepared event")?;
                for remove_statement in &prepared.remove_statements {
                    transaction
                        .execute(remove_statement, &[&block])
                        .await
                        .context("execute remove_statement")?;
                    transaction
                        .execute(&self.set_indexed_block, &[&uncle.event, &parent_block])
                        .await
                        .context("execute set_indexed_block")?;
                }

                // Remove blocks and transactions as well.
                transaction
                    .execute(REMOVE_BLOCKS_FROM, &[&block])
                    .await
                    .context("execute remove_statement (blocks)")?;
                transaction
                    .execute(&self.set_indexed_block, &[&"blocks", &parent_block])
                    .await
                    .context("set_indexed_block")?;

                transaction
                    .execute(REMOVE_TRANSACTIONS_FROM, &[&block])
                    .await
                    .context("execute remove_statement (blocks)")?;
                transaction
                    .execute(&self.set_indexed_block, &[&"transactions", &parent_block])
                    .await
                    .context("set_indexed_block")?;
            }

            transaction.commit().await.context("commit")
        }
        .boxed()
    }
}

impl Postgres {
    async fn store_event<'a>(
        transaction: &mut tokio_postgres::Transaction<'a>,
        events: &HashMap<String, PreparedEvent<InsertStatement, tokio_postgres::Statement>>,
        Log {
            event,
            block_number,
            log_index,
            transaction_index,
            address,
            fields,
        }: &'a Log<'a>,
    ) -> Result<()> {
        let event = events.get(*event).context("unknown event")?;

        let len = fields.len();
        let expected_len = event.descriptor.inputs.len();
        if fields.len() != expected_len {
            return Err(anyhow!(
                "event value has {len} fields but should have {expected_len}"
            ));
        }
        check_descriptor(fields, event)?;

        // Outer vec maps to tables. Inner vec maps to (array element count, columns).
        type ToSqlBox = Box<dyn tokio_postgres::types::ToSql + Send + Sync>;
        let mut sql_values: Vec<(Option<usize>, Vec<ToSqlBox>)> = vec![(None, vec![])];
        let mut in_array: bool = false;
        let mut visitor = |value: VisitValue<'a>| {
            let sql_value: ToSqlBox = match value {
                VisitValue::ArrayStart(len) => {
                    sql_values.push((Some(len), Vec::new()));
                    in_array = true;
                    return;
                }
                VisitValue::ArrayEnd => {
                    in_array = false;
                    return;
                }
                VisitValue::Value(AbiValue::Int(v)) => Box::new(PgNumeric::new(Some(
                    BigDecimal::from_str(&v.get().to_string()).unwrap(),
                ))),
                VisitValue::Value(AbiValue::Uint(v)) => Box::new(PgNumeric::new(Some(
                    BigDecimal::from_str(&v.get().to_string()).unwrap(),
                ))),
                VisitValue::Value(AbiValue::Address(v)) => {
                    Box::new(v.0.into_iter().collect::<Vec<_>>())
                }
                VisitValue::Value(AbiValue::Bool(v)) => Box::new(*v),
                VisitValue::Value(AbiValue::FixedBytes(v)) => Box::new(v.as_bytes().to_vec()),
                VisitValue::Value(AbiValue::Function(v)) => Box::new(
                    v.address
                        .0
                        .iter()
                        .copied()
                        .chain(v.selector.0.iter().copied())
                        .collect::<Vec<_>>(),
                ),
                VisitValue::Value(AbiValue::Bytes(v)) => Box::new(v.to_owned()),
                VisitValue::Value(AbiValue::String(v)) => Box::new(v.replace('\0', "")),
                _ => unreachable!(),
            };
            push_sql_value(&mut sql_values, in_array, sql_value)
        };
        for value in fields {
            event_visitor::visit_value(value, &mut visitor)
        }

        let block_number = i64::try_from(*block_number)?;
        let log_index = i64::try_from(*log_index)?;
        let transaction_index = i64::try_from(*transaction_index)?;
        let address = address.0.as_slice();
        for (statement, (array_element_count, values)) in
            event.insert_statements.iter().zip(sql_values)
        {
            let is_array = array_element_count.is_some();
            let array_element_count = array_element_count.unwrap_or(1);
            assert_eq!(statement.fields * array_element_count, values.len());
            for i in 0..array_element_count {
                let row = &values[i * statement.fields..][..statement.fields];
                let array_index = if is_array {
                    Some(i64::try_from(i)?)
                } else {
                    None
                };
                let params: Vec<_> = [
                    &block_number as &(dyn tokio_postgres::types::ToSql + Sync),
                    &log_index,
                    &transaction_index,
                    &address,
                ]
                .into_iter()
                .chain(
                    array_index
                        .as_ref()
                        .map(|i| i as &(dyn tokio_postgres::types::ToSql + Sync)),
                )
                .chain(
                    row.iter()
                        .map(|v| v.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)),
                )
                .collect();
                transaction
                    .execute(&statement.sql, params.as_slice())
                    .await
                    .context("execute insert")?;
            }
        }

        Ok(())
    }

    #[allow(clippy::needless_lifetimes)]
    async fn store_block<'a>(
        transaction: &mut tokio_postgres::Transaction<'a>,
        block: &database::BlockTime,
    ) -> Result<()> {
        let block_number = i64::try_from(block.number)?;
        let params = [
            &block_number as &(dyn tokio_postgres::types::ToSql + Sync),
            &block.timestamp,
        ];
        transaction
            .execute(INSERT_BLOCK, &params)
            .await
            .context("execute insert block")?;
        Ok(())
    }

    #[allow(clippy::needless_lifetimes)]
    async fn store_transaction<'a>(
        transaction: &mut tokio_postgres::Transaction<'a>,
        tx: &database::Transaction,
    ) -> Result<()> {
        let block_number = i64::try_from(tx.block_number)?;
        let index = i64::try_from(tx.index)?;

        let params = [
            &block_number as &(dyn tokio_postgres::types::ToSql + Sync),
            &index,
            &tx.hash.0.into_iter().collect::<Vec<_>>(),
            &tx.from.0.into_iter().collect::<Vec<_>>(),
            &tx.to.map(|t| t.0.into_iter().collect::<Vec<_>>()),
        ];
        transaction
            .execute(INSERT_TRANSACTION, &params)
            .await
            .context("execute insert transaction")?;
        Ok(())
    }

    async fn create_table<'a>(
        transaction: &tokio_postgres::Transaction<'a>,
        is_array: bool,
        table: &Table<'a>,
    ) -> Result<u64> {
        let mut sql = String::new();
        write!(&mut sql, "CREATE TABLE IF NOT EXISTS {} (", table.name)?;
        write!(&mut sql, "{FIXED_COLUMNS}, ")?;
        if is_array {
            write!(&mut sql, "{ARRAY_COLUMN}, ")?;
        }
        for column in table.columns.iter() {
            write!(&mut sql, "{}", column.name)?;
            let type_ = match abi_kind_to_sql_type(column.kind).unwrap() {
                tokio_postgres::types::Type::INT8 => "INT8",
                tokio_postgres::types::Type::BYTEA => "BYTEA",
                tokio_postgres::types::Type::NUMERIC => "NUMERIC",
                tokio_postgres::types::Type::BOOL => "BOOLEAN",
                tokio_postgres::types::Type::TEXT => "TEXT",
                unhandled_type => {
                    tracing::debug!("Got Type {}", unhandled_type);
                    unreachable!()
                }
            };
            write!(&mut sql, " {type_} NOT NULL, ")?;
        }
        let primary_key = if is_array {
            PRIMARY_KEY_ARRAY
        } else {
            PRIMARY_KEY
        };
        write!(&mut sql, "PRIMARY KEY({primary_key}));")?;
        tracing::debug!("creating table:\n{}", sql);
        transaction
            .execute(&sql, &[])
            .await
            .context("execute CREATE TABLE")
    }
}

/// Columns that every event table has.
const FIXED_COLUMNS: &str = "block_number BIGINT NOT NULL, log_index BIGINT NOT NULL, \
                             transaction_index BIGINT NOT NULL, address BYTEA NOT NULL";
const FIXED_COLUMNS_COUNT: usize = 4;
const PRIMARY_KEY: &str = "block_number, log_index";

/// Column for array tables.
const ARRAY_COLUMN: &str = "array_index BIGINT NOT NULL";
const PRIMARY_KEY_ARRAY: &str = "block_number, log_index, array_index";

const CREATE_BLOCKS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS blocks
(
    number INT8      PRIMARY KEY,
    time   TIMESTAMP NOT NULL
);"#;

const INSERT_BLOCK: &str = "INSERT INTO blocks (number, time) \
                            VALUES ($1, $2) \
                            ON CONFLICT DO NOTHING;";

const REMOVE_BLOCKS_FROM: &str = "DELETE FROM blocks WHERE number >= $1;";

const CREATE_TRANSACTIONS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS transactions
(
    block_number INT8      NOT NULL,
    index        INT8      NOT NULL,
    hash         BYTEA     NOT NULL,
    "from"       BYTEA     NOT NULL,
    "to"         BYTEA,
    PRIMARY KEY (block_number, index)
);
"#;

const INSERT_TRANSACTION: &str = r#"INSERT INTO transactions (block_number, index, hash, "from", "to")
                                    VALUES ($1, $2, $3, $4, $5)
                                    ON CONFLICT DO NOTHING;"#;

const REMOVE_TRANSACTIONS_FROM: &str = "DELETE FROM transactions WHERE block_number >=$1;";
const CREATE_EVENT_BLOCK_TABLE: &str = "CREATE TABLE IF NOT EXISTS _event_block(event TEXT \
                                        PRIMARY KEY NOT NULL, indexed BIGINT NOT NULL, finalized \
                                        BIGINT NOT NULL);";
const GET_EVENT_BLOCK: &str = "SELECT indexed, finalized FROM _event_block WHERE event = $1;";
const NEW_EVENT_BLOCK: &str = "INSERT INTO _event_block (event, indexed, finalized) VALUES($1, 0, \
                               0) ON CONFLICT(event) DO NOTHING;";
const SET_EVENT_BLOCK: &str =
    "UPDATE _event_block SET indexed = $2, finalized = $3 WHERE event = $1;";
const SET_INDEXED_BLOCK: &str = "UPDATE _event_block SET indexed = $2 WHERE event = $1";

/// Parameters:
/// - 1: block number
/// - 2: log index
/// - 3: array index if this is an array table (all tables after the first)
/// - 3 + n: n-th event field/column
pub(crate) struct InsertStatement {
    sql: tokio_postgres::Statement,
    /// Number of event fields that map to SQL columns. Does not count
    /// FIXED_COLUMNS and array index.
    fields: usize,
}

impl std::fmt::Debug for InsertStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertStatement")
            .field("fields", &self.fields)
            .finish()
    }
}

fn abi_kind_to_sql_type(value: &AbiKind) -> Option<tokio_postgres::types::Type> {
    match value {
        AbiKind::Int(_) => Some(tokio_postgres::types::Type::NUMERIC),
        AbiKind::Uint(_) => Some(tokio_postgres::types::Type::NUMERIC),
        AbiKind::Address => Some(tokio_postgres::types::Type::BYTEA),
        AbiKind::Bool => Some(tokio_postgres::types::Type::BOOL),
        AbiKind::FixedBytes(_) => Some(tokio_postgres::types::Type::BYTEA),
        AbiKind::Function => Some(tokio_postgres::types::Type::BYTEA),
        AbiKind::Bytes => Some(tokio_postgres::types::Type::BYTEA),
        AbiKind::String => Some(tokio_postgres::types::Type::TEXT),
        AbiKind::FixedArray(_, _) | AbiKind::Tuple(_) | AbiKind::Array(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solabi::{
            value::{Int, Uint},
            I256, U256,
        },
    };

    fn test_setup() -> (String, String) {
        (
            "postgresql://postgres@localhost".to_string(),
            "local".to_string(),
        )
    }

    async fn empty_db() -> Postgres {
        clear_database().await;
        let (db_url, schema) = test_setup();
        Postgres::connect(&db_url, &schema).await.unwrap()
    }

    async fn clear_database() {
        let (db_url, _) = test_setup();
        let client = connect(&db_url).await.unwrap();
        // https://stackoverflow.com/a/36023359
        let query = r#"
            DO $$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) 
                LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        "#;
        client.batch_execute(query).await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn large_number() {
        let mut db = empty_db().await;
        let event = r#"
            event Event (
                uint256,
                int256
            )
        "#;
        let event = EventDescriptor::parse_declaration(event).unwrap();
        db.prepare_event("event", &event).await.unwrap();
        let log = Log {
            event: "event",
            block_number: 0,
            fields: vec![
                AbiValue::Uint(Uint::new(256, U256::MAX).unwrap()),
                AbiValue::Int(Int::new(256, I256::MIN).unwrap()),
            ],
            ..Default::default()
        };
        db.update(&[], &[log], &[], &[]).await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn boolean_and_text_fields() {
        let mut db = empty_db().await;
        let event = r#"
            event Event (
                bool,
                bool,
                string
            )
        "#;
        let event = EventDescriptor::parse_declaration(event).unwrap();
        db.prepare_event("event", &event).await.unwrap();
        let log = Log {
            event: "event",
            block_number: 0,
            fields: vec![
                AbiValue::Bool(true),
                AbiValue::Bool(false),
                // Example taken from Erc1155Uri event at tx:
                // 0x1174cad0a67be620255a206e5104ea1cadef3d300e0f2e2ab41483b249a67d2f
                AbiValue::String("\0 \0\u{1}".to_string()),
            ],
            ..Default::default()
        };
        db.update(&[], &[log], &[], &[]).await.unwrap();
    }
}
