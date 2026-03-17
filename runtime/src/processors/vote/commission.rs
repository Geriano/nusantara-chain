use nusantara_vote_program::VoteState;

use crate::error::RuntimeError;
use crate::processors::helpers::{load_state, require_accounts, save_state};
use crate::transaction_context::TransactionContext;

pub(super) fn process_update_commission(
    accounts: &[u8],
    commission: u8,
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    require_accounts(accounts, 1, "UpdateCommission")?;
    let vote_idx = accounts[0] as usize;

    let mut state: VoteState = load_state(ctx, vote_idx)?;

    state.commission = commission;

    save_state(ctx, vote_idx, &state)
}
