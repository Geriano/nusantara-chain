use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use nusantara_core::Transaction;
use nusantara_crypto::Hash;
use nusantara_storage::TransactionStatus;

use crate::error::RpcError;
use crate::server::RpcState;
use crate::types::{SendTransactionRequest, SendTransactionResponse, TransactionStatusResponse};

#[utoipa::path(
    get,
    path = "/v1/transaction/{hash}",
    params(
        ("hash" = String, Path, description = "Base64 transaction hash")
    ),
    responses(
        (status = 200, description = "Transaction status", body = TransactionStatusResponse),
        (status = 404, description = "Transaction not found")
    )
)]
pub async fn get_transaction(
    State(state): State<Arc<RpcState>>,
    Path(hash): Path<String>,
) -> Result<Json<TransactionStatusResponse>, RpcError> {
    metrics::counter!("nusantara_rpc_requests", "endpoint" => "transaction").increment(1);

    let tx_hash =
        Hash::from_base64(&hash).map_err(|e| RpcError::BadRequest(format!("invalid hash: {e}")))?;

    let meta = match state.storage.get_transaction_status(&tx_hash)? {
        Some(m) => m,
        None => {
            // Check mempool for "received" status
            if state.mempool.contains(&tx_hash) {
                return Ok(Json(TransactionStatusResponse {
                    signature: hash,
                    slot: 0,
                    status: "received".to_string(),
                    fee: 0,
                    pre_balances: vec![],
                    post_balances: vec![],
                    compute_units_consumed: 0,
                }));
            }
            return Err(RpcError::NotFound(format!(
                "transaction {hash} not found"
            )));
        }
    };

    let status_str = match &meta.status {
        TransactionStatus::Success => "success".to_string(),
        TransactionStatus::Failed(msg) => format!("failed: {msg}"),
    };

    Ok(Json(TransactionStatusResponse {
        signature: hash,
        slot: meta.slot,
        status: status_str,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        compute_units_consumed: meta.compute_units_consumed,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/transaction/send",
    request_body = SendTransactionRequest,
    responses(
        (status = 200, description = "Transaction submitted", body = SendTransactionResponse),
        (status = 400, description = "Invalid transaction")
    )
)]
pub async fn send_transaction(
    State(state): State<Arc<RpcState>>,
    Json(req): Json<SendTransactionRequest>,
) -> Result<Json<SendTransactionResponse>, RpcError> {
    metrics::counter!("nusantara_rpc_requests", "endpoint" => "send_transaction").increment(1);

    let bytes = URL_SAFE_NO_PAD
        .decode(&req.transaction)
        .map_err(|e| RpcError::BadRequest(format!("invalid base64: {e}")))?;

    let tx: Transaction = borsh::from_slice(&bytes)
        .map_err(|e| RpcError::BadRequest(format!("invalid transaction: {e}")))?;

    let signature = tx.hash().to_base64();

    state
        .mempool
        .insert(tx.clone())
        .map_err(|e| RpcError::BadRequest(format!("mempool rejected transaction: {e}")))?;

    // Forward via TPU path for leader routing
    if let Some(fwd) = &state.tx_forward_sender {
        let _ = fwd.try_send(tx);
    }

    metrics::counter!("nusantara_rpc_transactions_submitted").increment(1);

    Ok(Json(SendTransactionResponse {
        signature,
        status: "received".to_string(),
    }))
}
