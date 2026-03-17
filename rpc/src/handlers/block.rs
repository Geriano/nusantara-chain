use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};

use crate::error::RpcError;
use crate::server::RpcState;
use crate::types::BlockResponse;

#[utoipa::path(
    get,
    path = "/v1/block/{slot}",
    params(
        ("slot" = u64, Path, description = "Slot number")
    ),
    responses(
        (status = 200, description = "Block header", body = BlockResponse),
        (status = 404, description = "Block not found")
    )
)]
pub async fn get_block(
    State(state): State<Arc<RpcState>>,
    Path(slot): Path<u64>,
) -> Result<Json<BlockResponse>, RpcError> {
    metrics::counter!("nusantara_rpc_requests", "endpoint" => "block").increment(1);

    let header = state
        .storage
        .get_block_header(slot)?
        .ok_or_else(|| RpcError::NotFound(format!("block at slot {slot} not found")))?;

    Ok(Json(BlockResponse {
        slot: header.slot,
        parent_slot: header.parent_slot,
        parent_hash: header.parent_hash.to_base64(),
        block_hash: header.block_hash.to_base64(),
        timestamp: header.timestamp,
        validator: header.validator.to_base64(),
        transaction_count: header.transaction_count,
        merkle_root: header.merkle_root.to_base64(),
    }))
}
