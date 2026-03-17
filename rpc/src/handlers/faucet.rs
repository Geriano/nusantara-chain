use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use nusantara_core::Message;
use nusantara_core::Transaction;
use nusantara_crypto::Hash;

use crate::error::RpcError;
use crate::server::RpcState;
use crate::types::{AirdropRequest, AirdropResponse};

#[utoipa::path(
    post,
    path = "/v1/airdrop",
    request_body = AirdropRequest,
    responses(
        (status = 200, description = "Airdrop submitted", body = AirdropResponse),
        (status = 400, description = "Invalid request"),
        (status = 503, description = "Faucet disabled")
    )
)]
pub async fn airdrop(
    State(state): State<Arc<RpcState>>,
    Json(req): Json<AirdropRequest>,
) -> Result<Json<AirdropResponse>, RpcError> {
    metrics::counter!("nusantara_rpc_requests", "endpoint" => "airdrop").increment(1);

    let faucet_keypair = state
        .faucet_keypair
        .as_ref()
        .ok_or(RpcError::FaucetDisabled)?;

    let to = Hash::from_base64(&req.address)
        .map_err(|e| RpcError::BadRequest(format!("invalid address: {e}")))?;

    if req.lamports == 0 {
        return Err(RpcError::BadRequest("lamports must be > 0".to_string()));
    }

    // Max 10 NUSA per airdrop
    if req.lamports > 10_000_000_000 {
        return Err(RpcError::BadRequest(
            "max airdrop is 10 NUSA (10_000_000_000 lamports)".to_string(),
        ));
    }

    let from = faucet_keypair.address();
    let ix = nusantara_system_program::transfer(&from, &to, req.lamports);

    let slot_hashes = state.bank.slot_hashes();
    let recent_blockhash = slot_hashes
        .0
        .first()
        .map(|(_, h)| *h)
        .unwrap_or(Hash::zero());

    let mut msg = Message::new(&[ix], &from)
        .map_err(|e| RpcError::Internal(format!("failed to build message: {e}")))?;
    msg.recent_blockhash = recent_blockhash;

    let mut tx = Transaction::new(msg);
    tx.sign(&[faucet_keypair.as_ref()]);

    let signature = tx.hash().to_base64();

    state
        .mempool
        .insert(tx.clone())
        .map_err(|e| RpcError::Internal(format!("mempool rejected transaction: {e}")))?;

    // Forward via TPU path for leader routing
    if let Some(fwd) = &state.tx_forward_sender {
        let _ = fwd.try_send(tx);
    }

    metrics::counter!("nusantara_rpc_airdrops").increment(1);

    Ok(Json(AirdropResponse { signature }))
}
