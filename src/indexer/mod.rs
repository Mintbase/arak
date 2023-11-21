//! Ethereum event indexer for a collection of events.

mod adapter;
mod chain;

use {
    self::{adapter::Adapter, chain::Chain},
    crate::{
        config,
        database::{self, Database},
    },
    anyhow::{Context, Result},
    ethrpc::{
        eth,
        types::{Block, BlockSpec, BlockTag, BlockTransactions, Hydrated, LogBlocks},
    },
    solabi::U256,
    std::{
        cmp,
        time::{Duration, SystemTime},
    },
    tokio::time,
};

/// An Ethereum event indexer.
pub struct Indexer<D> {
    eth: ethrpc::http::Client,
    database: D,
    adapters: Vec<Adapter>,
}

/// The indexer run configuration.
#[derive(Clone, Copy, Debug)]
pub struct Run {
    /// The block page size to use when fetching historic event data. Using
    /// larger values will speed up initialization, but may cause issues if too
    /// many events are fetched per page.
    pub page_size: u64,
    /// The poll interval to use when checking for new blocks.
    pub poll_interval: Duration,
}

impl<D> Indexer<D>
where
    D: Database,
{
    /// Creates a new event indexer.
    pub fn create(
        eth: ethrpc::http::Client,
        database: D,
        events: Vec<config::Event>,
    ) -> Result<Self> {
        Ok(Self {
            eth,
            database,
            adapters: events
                .into_iter()
                .map(Adapter::new)
                .collect::<Result<_>>()?,
        })
    }

    /// Runs the indexer, continuously fetching updates from the blockchain and
    /// storing them into the database.
    pub async fn run(mut self, config: Run) -> Result<()> {
        let finalized = self.init(config).await?;
        let mut chain = Chain::new(finalized.number, finalized.hash);
        loop {
            if !self.sync(&mut chain).await? {
                time::sleep(config.poll_interval).await;
            };
        }
    }

    /// Initializes an event indexer. This syncs historical event data and
    /// ensures that all events are indexed up until the `finalized` block.
    /// Returns the `finalized` block that it finished indexing until.
    async fn init(&mut self, config: Run) -> Result<Block> {
        for adapter in &self.adapters {
            self.database
                .prepare_event(adapter.name(), adapter.signature())
                .await?;
        }
        // TODO - should probably also prepare event for block and transaction.
        //  this might make the whole flow easier if it were handled among the others.
        //  but it doesn't align so well with the fact that blocks table has field number instead of block_number.

        let mut unfinalized = Vec::new();
        for adapter in self.adapters.iter() {
            let block = self.database.event_block(adapter.name()).await?;
            if block.indexed > block.finalized {
                unfinalized.push(database::Uncle {
                    event: adapter.name(),
                    number: block.finalized + 1,
                });
            }
        }
        for unfinalized in &unfinalized {
            tracing::info!(
                event = %unfinalized.event, finalized = %unfinalized.number,
                "removing logs for unfinalized blocks"
            );
        }
        if !unfinalized.is_empty() {
            self.database.remove(&unfinalized).await?;
        }

        loop {
            let finalized = self
                .eth
                .call(
                    eth::GetBlockByNumber,
                    (BlockTag::Finalized.into(), Hydrated::No),
                )
                .await?
                .context("missing finalized block")?;

            // Compute the next block to initialize from per adapter and the
            // earliest initialization block.
            let init = self.init_blocks().await?;
            let earliest = init
                .iter()
                .copied()
                .min()
                .unwrap_or(finalized.number.as_u64());
            if finalized.number.as_u64() <= earliest {
                return Ok(finalized);
            }

            let to = cmp::min(finalized.number.as_u64(), earliest + config.page_size - 1);
            tracing::debug!(from =% earliest, %to, "indexing blocks");

            // First fetch Blocks and Transaction Data
            // TODO - ensure earliest and to are aligned with _event_blocks table.
            let block_queries: Vec<(_, _)> = (earliest..to)
                .map(|block: u64| {
                    (
                        eth::GetBlockByNumber,
                        (BlockSpec::Number(U256::from(block)), Hydrated::Yes),
                    )
                })
                .collect();
            let block_tx_data = self.eth.batch(block_queries).await?;
            // Prepare `eth_getLogs` queries, noting the indices of their
            // corresponding adapters for decoding responses.
            let (adapters, queries) = self
                .adapters
                .iter()
                .zip(init.iter().copied())
                .filter(|(_, from)| *from <= to)
                .map(|(adapter, from)| {
                    (
                        adapter,
                        (
                            eth::GetLogs,
                            (adapter.filter(LogBlocks::Range {
                                from: from.into(),
                                to: to.into(),
                            }),),
                        ),
                    )
                })
                .unzip::<_, _, Vec<_>, Vec<_>>();
            let results = if !queries.is_empty() {
                self.eth.batch(queries).await?
            } else {
                Vec::new()
            };
            // Compute the database updates required:
            // - Update latest indexed blocks for the events that were queried
            // - Add the logs to the DB.
            let mut blocks = adapters
                .iter()
                .copied()
                .map(|adapter| database::EventBlock {
                    event: adapter.name(),
                    block: database::Block {
                        indexed: to,
                        finalized: finalized.number.as_u64(),
                    },
                })
                .collect::<Vec<_>>();
            let logs = adapters
                .into_iter()
                .zip(results)
                .flat_map(|(adapter, logs)| database_logs(adapter, logs))
                .collect::<Vec<_>>();

            // Add blocks and transactions.
            blocks.extend([
                database::EventBlock {
                    event: "blocks",
                    block: database::Block {
                        indexed: to,
                        finalized: finalized.number.as_u64(),
                    },
                },
                database::EventBlock {
                    event: "transactions",
                    block: database::Block {
                        indexed: to,
                        finalized: finalized.number.as_u64(),
                    },
                },
            ]);
            let (block_times, transactions) = database_block_data(block_tx_data);
            self.database
                .update(&blocks, &logs, &block_times, &transactions)
                .await?;
        }
    }

    /// Synchronises more events. Returns `true` if new blockchain state was
    /// processed.
    async fn sync(&mut self, chain: &mut Chain) -> Result<bool> {
        // TODO(nlordell): Remove reorged blocks and update with new data in a
        // single database transaction.

        let next = match self
            .eth
            .call(eth::GetBlockByNumber, (chain.next().into(), Hydrated::Yes))
            .await?
        {
            Some(value) => value,
            None => return Ok(false),
        };

        match chain.append(next.hash, next.parent_hash)? {
            chain::Append::Ok => {
                tracing::debug!(
                    block = %next.number, hash = %next.hash,
                    "found new block"
                );
            }
            chain::Append::Reorg => {
                let block = next.number - 1;
                tracing::debug!(%block, hash = %next.parent_hash, "reorg");

                let uncles = self
                    .adapters
                    .iter()
                    .map(|adapter| database::Uncle {
                        event: adapter.name(),
                        number: block.as_u64(),
                    })
                    .collect::<Vec<_>>();
                self.database.remove(&uncles).await?;
                return Ok(true);
            }
        }

        let (finalized, results) = tokio::try_join!(
            async {
                self.eth
                    .call(
                        eth::GetBlockByNumber,
                        (BlockTag::Finalized.into(), Hydrated::No),
                    )
                    .await?
                    .context("missing finalized block")
            },
            async {
                self.eth
                    .batch(
                        self.adapters
                            .iter()
                            .map(|adapter| {
                                (eth::GetLogs, (adapter.filter(LogBlocks::Hash(next.hash)),))
                            })
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .map_err(anyhow::Error::from)
            },
        )?;

        if chain.finalize(finalized.number)? != finalized.number {
            tracing::debug!(
                block = %finalized.number,
                "updated finalized block"
            );
        }

        let blocks = self
            .adapters
            .iter()
            .map(|adapter| database::EventBlock {
                event: adapter.name(),
                block: database::Block {
                    indexed: next.number.as_u64(),
                    finalized: finalized.number.as_u64(),
                },
            })
            .collect::<Vec<_>>();
        let logs = self
            .adapters
            .iter()
            .zip(results)
            .flat_map(|(adapter, logs)| database_logs(adapter, logs))
            .collect::<Vec<_>>();
        // TODO - use non-trivial stuff here!
        let (block_times, transactions) = database_block_data(vec![Some(next)]);
        self.database
            .update(&blocks, &logs, &block_times, &transactions)
            .await?;
        Ok(true)
    }

    /// Computes the blocks to start initializing from for each adapter.
    async fn init_blocks(&mut self) -> Result<Vec<u64>> {
        let mut blocks = Vec::new();
        let mut min_index_block = u64::MAX;
        for adapter in self.adapters.iter() {
            // Compute earliest block (as min of all event adapters)
            // to start indexing blocks and transactions
            let adapter_start = adapter.start();
            if adapter_start < min_index_block {
                min_index_block = adapter_start;
            }
            blocks.push(cmp::max(
                adapter_start,
                self.database.event_block(adapter.name()).await?.indexed + 1,
            ));
        }
        // These are non-adapter tables.
        blocks.push(cmp::max(
            min_index_block,
            self.database.event_block("blocks").await?.indexed + 1,
        ));
        blocks.push(cmp::max(
            min_index_block,
            self.database.event_block("transactions").await?.indexed + 1,
        ));
        Ok(blocks)
    }
}

fn database_block_data(
    block_data: Vec<Option<Block>>,
) -> (Vec<database::BlockTime>, Vec<database::Transaction>) {
    let mut blocks = vec![];
    let mut transactions = vec![];

    for block in block_data.into_iter().flatten() {
        let number = block.number.as_u64();
        blocks.push(database::BlockTime {
            number,
            timestamp: timestamp_to_systemtime(block.timestamp.as_u64()),
        });

        let txs = match block.transactions {
            BlockTransactions::Full(txs) => txs,
            BlockTransactions::Hash(hashes) => match hashes.len() {
                // This happens when a block has no transactions
                0 => vec![],
                _ => unreachable!("expected Full for Hydrated block={}", number),
            },
        };
        for tx in txs {
            transactions.push(database::Transaction {
                block_number: number,
                index: tx.transaction_index().as_u64(),
                hash: tx.hash(),
                from: tx.from(),
                to: tx.to(),
            });
        }
    }

    (blocks, transactions)
}

fn database_logs(
    adapter: &Adapter,
    logs: Vec<ethrpc::types::Log>,
) -> impl Iterator<Item = database::Log> {
    if !logs.is_empty() {
        tracing::debug!(
            event = %adapter.name(), logs = %logs.len(),
            "fetched logs"
        );
    }

    logs.into_iter()
        // Exclude invalid topic lengths
        .filter(|log| log.topics.len() == adapter.num_topics())
        .filter_map(move |log| {
            let fields = match adapter.decode(&log.topics, &log.data) {
                Ok(fields) => fields,
                Err(err) => {
                    tracing::warn!(?err, ?log, "failed to decode log");
                    return None;
                }
            };

            Some(database::Log {
                event: adapter.name(),
                block_number: log.block_number.as_u64(),
                log_index: log.log_index.as_u64(),
                transaction_index: log.transaction_index.as_u64(),
                address: log.address,
                fields,
            })
        })
}

pub fn timestamp_to_systemtime(timestamp: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_conversions() {
        // Time Zero
        assert_eq!(timestamp_to_systemtime(0), SystemTime::UNIX_EPOCH);

        // First Ethereum Block: https://etherscan.io/block/1
        assert_eq!(
            timestamp_to_systemtime(1438262788),
            SystemTime::UNIX_EPOCH + Duration::from_secs(1438262788)
        );
    }
}
