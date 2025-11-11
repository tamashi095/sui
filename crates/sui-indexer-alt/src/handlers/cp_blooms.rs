use anyhow::Result;
use async_trait::async_trait;
use diesel::{Insertable, Queryable, Selectable};
use diesel_async::RunQueryDsl;
use fastbloom::BloomFilter;
use std::collections::HashSet;
use std::sync::Arc;
use sui_indexer_alt_framework::FieldCount;

use sui_indexer_alt_framework::{
    pipeline::{Processor, concurrent::Handler},
    postgres::{Connection, Db},
    types::{
        full_checkpoint_content::{CheckpointData, CheckpointTransaction},
        transaction::TransactionDataAPI,
    },
};
use sui_indexer_alt_schema::schema::cp_blooms;
use sui_types::base_types::SuiAddress;
use sui_types::object::Owner;

pub(crate) struct CpBlooms;

#[derive(Clone, Debug, Insertable, Selectable, Queryable, FieldCount)]
#[diesel(table_name = cp_blooms)]
pub struct StoredCpBlooms {
    pub cp_sequence_number: i64,
    pub bloom_filter: Vec<u8>,
}

#[async_trait]
impl Processor for CpBlooms {
    const NAME: &'static str = "cp_blooms";
    const FANOUT: usize = 10;

    type Value = StoredCpBlooms;

    async fn process(&self, checkpoint: &Arc<CheckpointData>) -> Result<Vec<Self::Value>> {
        let CheckpointData {
            transactions,
            checkpoint_summary,
            ..
        } = checkpoint.as_ref();

        let cp_num = checkpoint_summary.sequence_number;

        // Collect all filter values (addresses, object IDs, function names) for interesting transactions
        let mut filter_values = HashSet::new();

        for tx in transactions.iter() {
            if !valid_for_bloom(tx) {
                continue;
            }

            // Add sender address
            let sender = tx.transaction.data().transaction_data().sender();
            filter_values.insert(sender.to_vec());

            // Add affected addresses (from object ownership)
            for ((_obj_id, _version, _digest), owner, _write_kind) in
                tx.effects.all_changed_objects()
            {
                if let Some(addr) = extract_address_from_owner(&owner) {
                    filter_values.insert(addr.to_vec());
                }
            }

            // Add affected object IDs
            for ((obj_id, _version, _digest), _owner, _write_kind) in
                tx.effects.all_changed_objects()
            {
                filter_values.insert(obj_id.to_vec());
            }

            // Add function call identifiers (package::module::function)
            for (package_id, module, function) in
                tx.transaction.data().transaction_data().move_calls()
            {
                let function_key = format!("{}::{}::{}", package_id, module, function);
                filter_values.insert(function_key.into_bytes());
            }
        }

        // If no interesting transactions, don't create a bloom
        if filter_values.is_empty() {
            return Ok(vec![]);
        }

        // Build bloom filter with 1% false positive rate
        let mut bloom = BloomFilter::with_false_pos(0.01).expected_items(filter_values.len());

        for value in &filter_values {
            bloom.insert(value);
        }

        // Serialize the entire bloom filter using serde
        let bloom_bytes = bincode::serialize(&bloom).expect("Failed to serialize bloom filter");

        Ok(vec![StoredCpBlooms {
            cp_sequence_number: cp_num as i64,
            bloom_filter: bloom_bytes,
        }])
    }
}

/// Determine if a transaction is "interesting" for multi-filter queries
fn valid_for_bloom(tx: &CheckpointTransaction) -> bool {
    // Has function calls
    let has_function_calls = !tx
        .transaction
        .data()
        .transaction_data()
        .move_calls()
        .is_empty();

    // Has affected objects (which includes affected addresses via object ownership)
    let has_affected_objects = !tx.effects.all_changed_objects().is_empty();

    has_function_calls || has_affected_objects
}

/// Extract address from Owner enum for bloom filter insertion
fn extract_address_from_owner(owner: &Owner) -> Option<SuiAddress> {
    match owner {
        Owner::AddressOwner(addr) => Some(*addr),
        Owner::ObjectOwner(addr) => Some(*addr),
        Owner::ConsensusAddressOwner { owner: addr, .. } => Some(*addr),
        Owner::Shared { .. } | Owner::Immutable => None,
    }
}

#[async_trait]
impl Handler for CpBlooms {
    type Store = Db;

    const MIN_EAGER_ROWS: usize = 100; // Batch ~100 checkpoints
    const MAX_PENDING_ROWS: usize = 500;

    async fn commit<'a>(values: &[Self::Value], conn: &mut Connection<'a>) -> Result<usize> {
        if values.is_empty() {
            return Ok(0);
        }

        // Single batched insert
        let inserted = diesel::insert_into(cp_blooms::table)
            .values(values)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?;

        Ok(inserted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MIGRATIONS;
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;
    use sui_indexer_alt_framework::{
        Indexer, types::test_checkpoint_data_builder::TestCheckpointDataBuilder,
    };
    use sui_types::base_types::{ObjectID, SuiAddress};

    async fn get_all_bloom_filters(conn: &mut Connection<'_>) -> Vec<StoredCpBlooms> {
        cp_blooms::table
            .order_by(cp_blooms::cp_sequence_number)
            .load(conn)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_cp_blooms_empty_checkpoint() {
        let (indexer, _db) = Indexer::new_for_testing(&MIGRATIONS).await;
        let _conn = indexer.store().connect().await.unwrap();

        let mut builder = TestCheckpointDataBuilder::new(0);
        let checkpoint = Arc::new(builder.build_checkpoint());

        let values = CpBlooms.process(&checkpoint).await.unwrap();

        assert!(
            values.is_empty(),
            "Should produce no bloom filter for empty checkpoint"
        );
    }

    #[tokio::test]
    async fn test_cp_blooms_with_function_calls() {
        let (indexer, _db) = Indexer::new_for_testing(&MIGRATIONS).await;
        let mut conn = indexer.store().connect().await.unwrap();

        let mut builder = TestCheckpointDataBuilder::new(0);
        builder = builder
            .start_transaction(0)
            .add_move_call(ObjectID::ZERO, "module", "function")
            .finish_transaction();
        let checkpoint = Arc::new(builder.build_checkpoint());

        let values = CpBlooms.process(&checkpoint).await.unwrap();

        assert_eq!(values.len(), 1, "Should produce one bloom filter");
        assert_eq!(values[0].cp_sequence_number, 0);
        assert!(!values[0].bloom_filter.is_empty());

        CpBlooms::commit(&values, &mut conn).await.unwrap();

        let stored = get_all_bloom_filters(&mut conn).await;
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].cp_sequence_number, 0);
    }

    #[tokio::test]
    async fn test_cp_blooms_with_affected_addresses() {
        let (indexer, _db) = Indexer::new_for_testing(&MIGRATIONS).await;
        let _conn = indexer.store().connect().await.unwrap();

        let mut builder = TestCheckpointDataBuilder::new(0);
        builder = builder
            .start_transaction(0)
            .create_owned_object(0)
            .finish_transaction();
        let checkpoint = Arc::new(builder.build_checkpoint());

        let values = CpBlooms.process(&checkpoint).await.unwrap();

        assert_eq!(values.len(), 1);
        assert!(!values[0].bloom_filter.is_empty());
    }

    #[tokio::test]
    async fn test_cp_blooms_with_affected_objects() {
        let (indexer, _db) = Indexer::new_for_testing(&MIGRATIONS).await;
        let _conn = indexer.store().connect().await.unwrap();

        let mut builder = TestCheckpointDataBuilder::new(0);
        builder = builder
            .start_transaction(0)
            .create_shared_object(0)
            .finish_transaction();
        let checkpoint = Arc::new(builder.build_checkpoint());

        let values = CpBlooms.process(&checkpoint).await.unwrap();

        assert_eq!(values.len(), 1);
    }

    #[tokio::test]
    async fn test_cp_blooms_multiple_checkpoints() {
        let (indexer, _db) = Indexer::new_for_testing(&MIGRATIONS).await;
        let mut conn = indexer.store().connect().await.unwrap();

        for cp_num in 0..3 {
            let mut builder = TestCheckpointDataBuilder::new(cp_num);
            builder = builder
                .start_transaction(0)
                .add_move_call(ObjectID::ZERO, "module", "function")
                .finish_transaction();
            let checkpoint = Arc::new(builder.build_checkpoint());

            let values = CpBlooms.process(&checkpoint).await.unwrap();
            CpBlooms::commit(&values, &mut conn).await.unwrap();
        }

        let stored = get_all_bloom_filters(&mut conn).await;
        assert_eq!(stored.len(), 3);

        for (i, bloom) in stored.iter().enumerate() {
            assert_eq!(bloom.cp_sequence_number, i as i64);
        }
    }

    #[tokio::test]
    async fn test_cp_blooms_filter_accuracy() {
        let (indexer, _db) = Indexer::new_for_testing(&MIGRATIONS).await;
        let _conn = indexer.store().connect().await.unwrap();

        let mut builder = TestCheckpointDataBuilder::new(0);
        builder = builder
            .start_transaction(0)
            .add_move_call(ObjectID::ZERO, "module", "function")
            .finish_transaction()
            .start_transaction(1)
            .add_move_call(ObjectID::ZERO, "module", "function")
            .finish_transaction();
        let checkpoint = Arc::new(builder.build_checkpoint());

        let values = CpBlooms.process(&checkpoint).await.unwrap();
        assert_eq!(values.len(), 1);

        let bloom_bytes = &values[0].bloom_filter;
        let bloom: BloomFilter = bincode::deserialize(bloom_bytes).unwrap();

        // Verify bloom filter contains function call identifier
        let function_key = format!("{}::{}::{}", ObjectID::ZERO, "module", "function");
        assert!(
            bloom.contains(&function_key.as_bytes()),
            "Should contain function call {}",
            function_key
        );

        // Verify bloom filter contains sender address
        let sender = checkpoint.transactions[0]
            .transaction
            .data()
            .transaction_data()
            .sender();
        assert!(
            bloom.contains(&sender.to_vec()),
            "Should contain sender address"
        );

        // Verify bloom filter contains affected object IDs
        for tx in &checkpoint.transactions {
            for ((obj_id, _, _), _, _) in tx.effects.all_changed_objects() {
                assert!(
                    bloom.contains(&obj_id.to_vec()),
                    "Should contain object ID {}",
                    obj_id
                );
            }
        }

        // Verify bloom filter does NOT contain random values
        let random_addr = SuiAddress::random_for_testing_only();
        assert!(
            !bloom.contains(&random_addr.to_vec()),
            "Should not contain random address"
        );
    }

    #[tokio::test]
    async fn test_cp_blooms_mixed_transactions() {
        let (indexer, _db) = Indexer::new_for_testing(&MIGRATIONS).await;
        let _conn = indexer.store().connect().await.unwrap();

        let mut builder = TestCheckpointDataBuilder::new(0);
        builder = builder
            .start_transaction(0)
            // Empty transaction with no operations - still affects gas object
            .finish_transaction()
            .start_transaction(1)
            .add_move_call(ObjectID::ZERO, "module", "function")
            .finish_transaction();
        let checkpoint = Arc::new(builder.build_checkpoint());

        let values = CpBlooms.process(&checkpoint).await.unwrap();
        assert_eq!(values.len(), 1);

        let bloom: BloomFilter = bincode::deserialize(&values[0].bloom_filter).unwrap();

        // Verify bloom filter contains sender addresses from both transactions
        let sender_0 = checkpoint.transactions[0]
            .transaction
            .data()
            .transaction_data()
            .sender();
        let sender_1 = checkpoint.transactions[1]
            .transaction
            .data()
            .transaction_data()
            .sender();
        assert!(
            bloom.contains(&sender_0.to_vec()),
            "Should contain sender from tx 0"
        );
        assert!(
            bloom.contains(&sender_1.to_vec()),
            "Should contain sender from tx 1"
        );

        // Verify bloom filter contains function call from tx 1
        let function_key = format!("{}::{}::{}", ObjectID::ZERO, "module", "function");
        assert!(
            bloom.contains(&function_key.as_bytes()),
            "Should contain function call from tx 1"
        );
    }
}
