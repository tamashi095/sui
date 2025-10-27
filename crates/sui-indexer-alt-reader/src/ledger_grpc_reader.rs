// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use anyhow::{Context, anyhow};
use async_graphql::dataloader::{DataLoader, Loader};
use prost_types::FieldMask;
use sui_kvstore::TransactionEventsData;
use sui_rpc::proto::sui::rpc::v2 as proto;
use sui_rpc::proto::sui::rpc::v2::ledger_service_client::LedgerServiceClient;
use sui_rpc::{field::FieldMaskUtil, proto::proto_to_timestamp_ms};
use sui_types::{
    crypto::AuthorityQuorumSignInfo,
    effects::TransactionEvents,
    messages_checkpoint::{CheckpointContents, CheckpointSummary},
    object::Object,
};
use sui_types::{
    effects::TransactionEffects, event::Event, signature::GenericSignature,
    transaction::TransactionData,
};
use tonic::transport::{Channel, ClientTlsConfig, Uri};

use crate::{
    checkpoints::CheckpointKey, error::Error, events::TransactionEventsKey,
    objects::VersionedObjectKey, transactions::TransactionKey,
};

#[derive(clap::Args, Debug, Clone, Default)]
pub struct LedgerGrpcArgs {
    /// gRPC endpoint URL for the ledger service (e.g., archive.mainnet.sui.io)
    #[arg(long)]
    pub ledger_grpc_uri: Option<Uri>,
}

#[derive(Debug, Clone)]
pub struct CheckpointedTransaction {
    pub effects: Box<TransactionEffects>,
    pub events: Option<Vec<Event>>,
    pub transaction_data: Box<TransactionData>,
    pub signatures: Vec<GenericSignature>,
    pub timestamp_ms: Option<u64>,
    pub cp_sequence_number: Option<u64>,
}

/// A reader backed by gRPC LedgerService (sui-kv-rpc).
///
/// This connects to archival service that implements the same LedgerService gRPC interface
/// as fullnode, but is backed by Bigtable for serving historical data.
#[derive(Clone)]
pub struct LedgerGrpcReader(LedgerServiceClient<Channel>);

impl LedgerGrpcReader {
    pub async fn new(uri: Uri) -> anyhow::Result<Self> {
        let tls_config = ClientTlsConfig::new().with_native_roots();
        let channel = Channel::builder(uri)
            .tls_config(tls_config)?
            .connect()
            .await
            .context("Failed to connect to gRPC endpoint")?;

        let client = LedgerServiceClient::new(channel.clone());
        Ok(Self(client))
    }

    pub fn as_data_loader(&self) -> DataLoader<Self> {
        DataLoader::new(self.clone(), tokio::spawn)
    }
}

#[async_trait::async_trait]
impl Loader<VersionedObjectKey> for LedgerGrpcReader {
    type Value = Object;
    type Error = Error;

    async fn load(
        &self,
        keys: &[VersionedObjectKey],
    ) -> Result<HashMap<VersionedObjectKey, Object>, Error> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let requests = keys
            .iter()
            .map(|key| {
                let mut req = proto::GetObjectRequest::new(&key.0.into());
                req.version = Some(key.1);
                req
            })
            .collect();

        let mut request = proto::BatchGetObjectsRequest::default();
        request.requests = requests;
        request.read_mask = Some(FieldMask::from_paths(["bcs"]));

        let response = self.0.clone().batch_get_objects(request).await?;
        let batch_response = response.into_inner();

        let mut results = HashMap::new();
        for (key, obj_result) in keys.iter().zip(batch_response.objects) {
            if let Some(proto::get_object_result::Result::Object(object)) = obj_result.result {
                let obj: Object = object
                    .bcs
                    .as_ref()
                    .context("Missing bcs in object")?
                    .deserialize()
                    .context("Failed to deserialize object")?;
                results.insert(*key, obj);
            }
        }
        Ok(results)
    }
}

#[async_trait::async_trait]
impl Loader<CheckpointKey> for LedgerGrpcReader {
    type Value = (
        CheckpointSummary,
        CheckpointContents,
        AuthorityQuorumSignInfo<true>,
    );
    type Error = Error;

    async fn load(
        &self,
        keys: &[CheckpointKey],
    ) -> Result<HashMap<CheckpointKey, Self::Value>, Error> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let mut results = HashMap::new();
        for key in keys {
            let request = proto::GetCheckpointRequest::by_sequence_number(key.0).with_read_mask(
                FieldMask::from_paths(["summary.bcs", "signature", "contents.bcs"]),
            );

            match self.0.clone().get_checkpoint(request).await {
                Ok(response) => {
                    let checkpoint = response
                        .into_inner()
                        .checkpoint
                        .context("No checkpoint returned")?;

                    let summary: CheckpointSummary = checkpoint
                        .summary
                        .as_ref()
                        .and_then(|s| s.bcs.as_ref())
                        .context("Missing summary.bcs")?
                        .deserialize()
                        .context("Failed to deserialize checkpoint summary")?;

                    let contents: CheckpointContents = checkpoint
                        .contents
                        .as_ref()
                        .and_then(|c| c.bcs.as_ref())
                        .context("Missing contents.bcs")?
                        .deserialize()
                        .context("Failed to deserialize checkpoint contents")?;

                    let signature: AuthorityQuorumSignInfo<true> = {
                        let sdk_sig = sui_sdk_types::ValidatorAggregatedSignature::try_from(
                            checkpoint.signature.as_ref().context("Missing signature")?,
                        )
                        .context("Failed to parse signature")?;
                        AuthorityQuorumSignInfo::from(sdk_sig)
                    };

                    results.insert(*key, (summary, contents, signature));
                }
                Err(status) if status.code() == tonic::Code::NotFound => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(results)
    }
}

#[async_trait::async_trait]
impl Loader<TransactionKey> for LedgerGrpcReader {
    type Value = CheckpointedTransaction;
    type Error = Error;

    async fn load(
        &self,
        keys: &[TransactionKey],
    ) -> Result<HashMap<TransactionKey, Self::Value>, Error> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let digests = keys.iter().map(|key| key.0.to_string()).collect();

        let mut request = proto::BatchGetTransactionsRequest::default();
        request.digests = digests;
        request.read_mask = Some(FieldMask::from_paths([
            "transaction.bcs",
            "effects.bcs",
            "events.bcs",
            "signatures.bcs",
            "checkpoint",
            "timestamp",
        ]));

        let response = self.0.clone().batch_get_transactions(request).await?;
        let batch_response = response.into_inner();

        let mut results = HashMap::new();
        for (key, tx_result) in keys.iter().zip(batch_response.transactions) {
            if let Some(proto::get_transaction_result::Result::Transaction(executed)) =
                tx_result.result
            {
                let full_tx: sui_types::full_checkpoint_content::ExecutedTransaction = (&executed)
                    .try_into()
                    .context("Failed to convert ExecutedTransaction from proto")?;

                let timestamp_ms = executed
                    .timestamp
                    .map(proto_to_timestamp_ms)
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("Failed to parse timestamp: {}", e))?;

                let transaction = CheckpointedTransaction {
                    effects: Box::new(full_tx.effects),
                    events: full_tx.events.map(|events| events.data),
                    transaction_data: Box::new(full_tx.transaction),
                    signatures: full_tx.signatures,
                    timestamp_ms,
                    cp_sequence_number: executed.checkpoint,
                };
                results.insert(*key, transaction);
            }
        }
        Ok(results)
    }
}

#[async_trait::async_trait]
impl Loader<TransactionEventsKey> for LedgerGrpcReader {
    type Value = TransactionEventsData;
    type Error = Error;

    async fn load(
        &self,
        keys: &[TransactionEventsKey],
    ) -> Result<HashMap<TransactionEventsKey, Self::Value>, Self::Error> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let mut results = HashMap::new();
        for key in keys {
            let request = proto::GetTransactionRequest::new(&key.0.into())
                .with_read_mask(FieldMask::from_paths(["events.bcs", "timestamp"]));

            match self.0.clone().get_transaction(request).await {
                Ok(response) => {
                    let executed = response
                        .into_inner()
                        .transaction
                        .context("No transaction returned")?;

                    let events = executed
                        .events
                        .as_ref()
                        .and_then(|e| e.bcs.as_ref())
                        .map(|bcs| -> anyhow::Result<_> {
                            let tx_events: TransactionEvents = bcs
                                .deserialize()
                                .context("Failed to deserialize transaction events")?;
                            Ok(tx_events.data)
                        })
                        .transpose()?
                        .unwrap_or_default();

                    let timestamp_ms = executed
                        .timestamp
                        .map(proto_to_timestamp_ms)
                        .transpose()
                        .map_err(|e| anyhow!("Failed to parse timestamp: {}", e))?
                        .unwrap_or(0);

                    results.insert(
                        *key,
                        TransactionEventsData {
                            events,
                            timestamp_ms,
                        },
                    );
                }
                Err(status) if status.code() == tonic::Code::NotFound => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(results)
    }
}
