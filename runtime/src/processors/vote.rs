use borsh::BorshDeserialize;
use nusantara_vote_program::{
    Lockout, MAX_LOCKOUT_HISTORY, Vote, VoteAuthorize, VoteInit, VoteInstruction, VoteState,
};

use crate::error::RuntimeError;
use crate::sysvar_cache::SysvarCache;
use crate::transaction_context::TransactionContext;

const VOTE_BASE_COST: u64 = 2100;

pub fn process_vote(
    accounts: &[u8],
    data: &[u8],
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    ctx.consume_compute(VOTE_BASE_COST)?;

    let instruction = VoteInstruction::try_from_slice(data)
        .map_err(|e| RuntimeError::InvalidInstructionData(e.to_string()))?;

    match instruction {
        VoteInstruction::InitializeAccount(init) => {
            process_initialize(accounts, init, ctx, sysvars)
        }
        VoteInstruction::Vote(vote) => process_vote_action(accounts, vote, ctx, sysvars),
        VoteInstruction::Authorize(new_auth, auth_type) => {
            process_authorize(accounts, new_auth, auth_type, ctx)
        }
        VoteInstruction::Withdraw(lamports) => process_withdraw(accounts, lamports, ctx),
        VoteInstruction::UpdateCommission(commission) => {
            process_update_commission(accounts, commission, ctx)
        }
        VoteInstruction::SwitchVote(vote, _proof_hash) => {
            process_vote_action(accounts, vote, ctx, sysvars)
        }
        VoteInstruction::UpdateValidatorIdentity => Err(RuntimeError::ProgramError {
            program: "vote".to_string(),
            message: "instruction not yet implemented".to_string(),
        }),
    }
}

fn process_initialize(
    accounts: &[u8],
    init: VoteInit,
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    if accounts.is_empty() {
        return Err(RuntimeError::InvalidInstructionData(
            "InitializeAccount requires 1 account".to_string(),
        ));
    }
    let vote_idx = accounts[0] as usize;

    // Check if already initialized
    {
        let acc = ctx.get_account(vote_idx)?;
        if !acc.account.data.is_empty()
            && let Ok(state) = VoteState::try_from_slice(&acc.account.data)
            && (!state.votes.is_empty() || state.authorized_voter != nusantara_crypto::Hash::zero())
        {
            return Err(RuntimeError::InvalidAccountData(
                "vote account already initialized".to_string(),
            ));
        }
    }

    // Check rent exemption
    let state = VoteState::new(&init);
    let state_data =
        borsh::to_vec(&state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    {
        let acc = ctx.get_account(vote_idx)?;
        let min = sysvars.rent().minimum_balance(state_data.len());
        if acc.account.lamports < min {
            return Err(RuntimeError::RentNotMet {
                needed: min,
                available: acc.account.lamports,
            });
        }
    }

    let acc = ctx.get_account_mut(vote_idx)?;
    acc.account.data = state_data;
    Ok(())
}

fn process_vote_action(
    accounts: &[u8],
    vote: Vote,
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    if accounts.len() < 2 {
        return Err(RuntimeError::InvalidInstructionData(
            "Vote requires 2 accounts".to_string(),
        ));
    }
    let vote_idx = accounts[0] as usize;
    let voter_idx = accounts[1] as usize;

    // Verify voter is signer
    let voter_address = {
        let voter = ctx.get_account(voter_idx)?;
        if !voter.is_signer {
            return Err(RuntimeError::AccountNotSigner(voter_idx));
        }
        *voter.address
    };

    // Load state
    let mut state = {
        let acc = ctx.get_account(vote_idx)?;
        VoteState::try_from_slice(&acc.account.data)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?
    };

    // Verify authorization
    if state.authorized_voter != voter_address {
        return Err(RuntimeError::ProgramError {
            program: "vote".to_string(),
            message: "not authorized voter".to_string(),
        });
    }

    // Process each vote slot
    for &slot in &vote.slots {
        // Add new lockout
        let lockout = Lockout {
            slot,
            confirmation_count: 1,
        };
        state.votes.push(lockout);

        // Increment confirmation counts for existing votes
        // that are not locked out at this slot
        let len = state.votes.len();
        if len > 1 {
            for i in (0..len - 1).rev() {
                state.votes[i].confirmation_count += 1;
            }
        }

        // Pop votes that have reached max lockout
        while state.votes.len() > MAX_LOCKOUT_HISTORY as usize {
            let oldest = state.votes.remove(0);
            state.root_slot = Some(oldest.slot);
        }
    }

    // Update epoch credits
    let current_epoch = sysvars.clock().epoch;
    if let Some(last) = state.epoch_credits.last_mut() {
        if last.0 == current_epoch {
            last.1 += vote.slots.len() as u64;
        } else {
            let prev_credits = last.1;
            state.epoch_credits.push((
                current_epoch,
                prev_credits + vote.slots.len() as u64,
                prev_credits,
            ));
        }
    } else {
        state
            .epoch_credits
            .push((current_epoch, vote.slots.len() as u64, 0));
    }

    // Update timestamp
    if let Some(ts) = vote.timestamp {
        state.last_timestamp = nusantara_vote_program::BlockTimestamp {
            slot: *vote.slots.last().unwrap_or(&0),
            timestamp: ts,
        };
    }

    let state_data =
        borsh::to_vec(&state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    let acc = ctx.get_account_mut(vote_idx)?;
    acc.account.data = state_data;
    Ok(())
}

fn process_authorize(
    accounts: &[u8],
    new_auth: nusantara_crypto::Hash,
    auth_type: VoteAuthorize,
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.len() < 2 {
        return Err(RuntimeError::InvalidInstructionData(
            "Authorize requires 2 accounts".to_string(),
        ));
    }
    let vote_idx = accounts[0] as usize;
    let auth_idx = accounts[1] as usize;

    let auth_address = {
        let auth = ctx.get_account(auth_idx)?;
        if !auth.is_signer {
            return Err(RuntimeError::AccountNotSigner(auth_idx));
        }
        *auth.address
    };

    let mut state = {
        let acc = ctx.get_account(vote_idx)?;
        VoteState::try_from_slice(&acc.account.data)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?
    };

    match auth_type {
        VoteAuthorize::Voter => {
            if state.authorized_voter != auth_address {
                return Err(RuntimeError::ProgramError {
                    program: "vote".to_string(),
                    message: "not authorized voter".to_string(),
                });
            }
            state.authorized_voter = new_auth;
        }
        VoteAuthorize::Withdrawer => {
            if state.authorized_withdrawer != auth_address {
                return Err(RuntimeError::ProgramError {
                    program: "vote".to_string(),
                    message: "not authorized withdrawer".to_string(),
                });
            }
            state.authorized_withdrawer = new_auth;
        }
    }

    let state_data =
        borsh::to_vec(&state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    let acc = ctx.get_account_mut(vote_idx)?;
    acc.account.data = state_data;
    Ok(())
}

fn process_withdraw(
    accounts: &[u8],
    lamports: u64,
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(RuntimeError::InvalidInstructionData(
            "Withdraw requires 3 accounts".to_string(),
        ));
    }
    let vote_idx = accounts[0] as usize;
    let to_idx = accounts[1] as usize;
    let withdrawer_idx = accounts[2] as usize;

    let withdrawer_address = {
        let withdrawer = ctx.get_account(withdrawer_idx)?;
        if !withdrawer.is_signer {
            return Err(RuntimeError::AccountNotSigner(withdrawer_idx));
        }
        *withdrawer.address
    };

    let state = {
        let acc = ctx.get_account(vote_idx)?;
        VoteState::try_from_slice(&acc.account.data)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?
    };

    if state.authorized_withdrawer != withdrawer_address {
        return Err(RuntimeError::ProgramError {
            program: "vote".to_string(),
            message: "not authorized withdrawer".to_string(),
        });
    }

    {
        let acc = ctx.get_account(vote_idx)?;
        if acc.account.lamports < lamports {
            return Err(RuntimeError::InsufficientFunds {
                needed: lamports,
                available: acc.account.lamports,
            });
        }
    }

    {
        let acc = ctx.get_account_mut(vote_idx)?;
        acc.account.lamports -= lamports;
    }

    {
        let acc = ctx.get_account_mut(to_idx)?;
        acc.account.lamports = acc
            .account
            .lamports
            .checked_add(lamports)
            .ok_or(RuntimeError::LamportsOverflow)?;
    }

    Ok(())
}

fn process_update_commission(
    accounts: &[u8],
    commission: u8,
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.is_empty() {
        return Err(RuntimeError::InvalidInstructionData(
            "UpdateCommission requires 1 account".to_string(),
        ));
    }
    let vote_idx = accounts[0] as usize;

    let mut state = {
        let acc = ctx.get_account(vote_idx)?;
        VoteState::try_from_slice(&acc.account.data)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?
    };

    state.commission = commission;

    let state_data =
        borsh::to_vec(&state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    let acc = ctx.get_account_mut(vote_idx)?;
    acc.account.data = state_data;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_core::instruction::AccountMeta;
    use nusantara_core::program::VOTE_PROGRAM_ID;
    use nusantara_core::{Account, EpochSchedule, Instruction, Message};
    use nusantara_crypto::hash;
    use nusantara_rent_program::Rent;
    use nusantara_sysvar_program::{Clock, RecentBlockhashes, SlotHashes, StakeHistory};

    fn test_sysvars() -> SysvarCache {
        SysvarCache::new(
            Clock {
                slot: 100,
                epoch: 5,
                unix_timestamp: 1_000_000,
                ..Clock::default()
            },
            Rent::default(),
            EpochSchedule::default(),
            SlotHashes::default(),
            StakeHistory::default(),
            RecentBlockhashes::default(),
        )
    }

    fn setup_vote_init() -> (TransactionContext, Vec<u8>, Vec<u8>, SysvarCache) {
        let vote_acc = hash(b"vote");
        let node = hash(b"node");
        let voter = hash(b"voter");
        let withdrawer = hash(b"withdrawer");

        let init = VoteInit {
            node_pubkey: node,
            authorized_voter: voter,
            authorized_withdrawer: withdrawer,
            commission: 10,
        };

        let ix = nusantara_vote_program::initialize_account(&vote_acc, init);
        let msg = Message::new(&[ix], &vote_acc).unwrap();

        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &vote_acc {
                    (*k, Account::new(10_000_000, *VOTE_PROGRAM_ID))
                } else {
                    (*k, Account::new(0, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();

        let compiled = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let ctx = TransactionContext::new(accounts, msg, 100, 100_000);
        (ctx, compiled, data, test_sysvars())
    }

    #[test]
    fn initialize_success() {
        let (mut ctx, accounts, data, sysvars) = setup_vote_init();
        process_vote(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let idx = ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &hash(b"vote"))
            .unwrap();
        let acc = ctx.get_account(idx).unwrap();
        let state = VoteState::try_from_slice(&acc.account.data).unwrap();
        assert_eq!(state.commission, 10);
        assert_eq!(state.authorized_voter, hash(b"voter"));
    }

    #[test]
    fn initialize_already_initialized() {
        let (mut ctx, accounts, data, sysvars) = setup_vote_init();
        process_vote(&accounts, &data, &mut ctx, &sysvars).unwrap();
        // Re-init
        let mut ctx2 = TransactionContext::new(
            ctx.message()
                .account_keys
                .iter()
                .enumerate()
                .map(|(i, k)| (*k, ctx.get_account(i).unwrap().account.clone()))
                .collect(),
            ctx.message().clone(),
            100,
            100_000,
        );
        let err = process_vote(&accounts, &data, &mut ctx2, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidAccountData(_)));
    }

    #[test]
    fn vote_success() {
        let (mut ctx, accounts, data, sysvars) = setup_vote_init();
        process_vote(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let vote_acc = hash(b"vote");
        let voter = hash(b"voter");
        let v = Vote {
            slots: vec![100, 101, 102],
            hash: hash(b"blockhash"),
            timestamp: Some(1_000_000),
        };
        let vote_ix = nusantara_vote_program::vote(&vote_acc, &voter, v);
        let vote_msg = Message::new(&[vote_ix], &voter).unwrap();

        let vote_accounts: Vec<_> = vote_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &vote_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &vote_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else {
                    (*k, Account::new(1_000_000, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();

        let compiled = vote_msg.instructions[0].accounts.clone();
        let vote_data = vote_msg.instructions[0].data.clone();
        let mut vote_ctx = TransactionContext::new(vote_accounts, vote_msg, 100, 100_000);
        process_vote(&compiled, &vote_data, &mut vote_ctx, &sysvars).unwrap();

        let idx = vote_ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &vote_acc)
            .unwrap();
        let acc = vote_ctx.get_account(idx).unwrap();
        let state = VoteState::try_from_slice(&acc.account.data).unwrap();
        assert_eq!(state.votes.len(), 3);
    }

    #[test]
    fn vote_not_authorized() {
        let (mut ctx, accounts, data, sysvars) = setup_vote_init();
        process_vote(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let vote_acc = hash(b"vote");
        let wrong_voter = hash(b"wrong");
        let v = Vote {
            slots: vec![100],
            hash: hash(b"blockhash"),
            timestamp: None,
        };
        let vote_ix = nusantara_vote_program::vote(&vote_acc, &wrong_voter, v);
        let vote_msg = Message::new(&[vote_ix], &wrong_voter).unwrap();

        let vote_accounts: Vec<_> = vote_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &vote_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &vote_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else {
                    (*k, Account::new(1_000_000, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();

        let compiled = vote_msg.instructions[0].accounts.clone();
        let vote_data = vote_msg.instructions[0].data.clone();
        let mut vote_ctx = TransactionContext::new(vote_accounts, vote_msg, 100, 100_000);
        let err = process_vote(&compiled, &vote_data, &mut vote_ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::ProgramError { .. }));
    }

    #[test]
    fn authorize_voter() {
        let (mut ctx, accounts, data, sysvars) = setup_vote_init();
        process_vote(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let vote_acc = hash(b"vote");
        let voter = hash(b"voter");
        let new_voter = hash(b"new_voter");
        let auth_ix =
            nusantara_vote_program::authorize(&vote_acc, &voter, new_voter, VoteAuthorize::Voter);
        let auth_msg = Message::new(&[auth_ix], &voter).unwrap();

        let auth_accounts: Vec<_> = auth_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &vote_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &vote_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else {
                    (*k, Account::new(1_000_000, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();

        let compiled = auth_msg.instructions[0].accounts.clone();
        let auth_data = auth_msg.instructions[0].data.clone();
        let mut auth_ctx = TransactionContext::new(auth_accounts, auth_msg, 100, 100_000);
        process_vote(&compiled, &auth_data, &mut auth_ctx, &sysvars).unwrap();

        let idx = auth_ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &vote_acc)
            .unwrap();
        let acc = auth_ctx.get_account(idx).unwrap();
        let state = VoteState::try_from_slice(&acc.account.data).unwrap();
        assert_eq!(state.authorized_voter, new_voter);
    }

    #[test]
    fn authorize_withdrawer() {
        let (mut ctx, accounts, data, sysvars) = setup_vote_init();
        process_vote(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let vote_acc = hash(b"vote");
        let withdrawer = hash(b"withdrawer");
        let new_withdrawer = hash(b"new_withdrawer");
        let auth_ix = nusantara_vote_program::authorize(
            &vote_acc,
            &withdrawer,
            new_withdrawer,
            VoteAuthorize::Withdrawer,
        );
        let auth_msg = Message::new(&[auth_ix], &withdrawer).unwrap();

        let auth_accounts: Vec<_> = auth_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &vote_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &vote_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else {
                    (*k, Account::new(1_000_000, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();

        let compiled = auth_msg.instructions[0].accounts.clone();
        let auth_data = auth_msg.instructions[0].data.clone();
        let mut auth_ctx = TransactionContext::new(auth_accounts, auth_msg, 100, 100_000);
        process_vote(&compiled, &auth_data, &mut auth_ctx, &sysvars).unwrap();

        let idx = auth_ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &vote_acc)
            .unwrap();
        let acc = auth_ctx.get_account(idx).unwrap();
        let state = VoteState::try_from_slice(&acc.account.data).unwrap();
        assert_eq!(state.authorized_withdrawer, new_withdrawer);
    }

    #[test]
    fn withdraw_success() {
        let (mut ctx, accounts, data, sysvars) = setup_vote_init();
        process_vote(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let vote_acc = hash(b"vote");
        let withdrawer = hash(b"withdrawer");
        let to = hash(b"to");
        let w_ix = nusantara_vote_program::withdraw(&vote_acc, &withdrawer, &to, 100_000);
        let w_msg = Message::new(&[w_ix], &withdrawer).unwrap();

        let w_accounts: Vec<_> = w_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &vote_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &vote_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else if k == &withdrawer {
                    (*k, Account::new(1_000_000, nusantara_crypto::Hash::zero()))
                } else {
                    (*k, Account::new(0, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();

        let compiled = w_msg.instructions[0].accounts.clone();
        let w_data = w_msg.instructions[0].data.clone();
        let mut w_ctx = TransactionContext::new(w_accounts, w_msg, 100, 100_000);
        process_vote(&compiled, &w_data, &mut w_ctx, &sysvars).unwrap();
    }

    #[test]
    fn update_commission() {
        let (mut ctx, accounts, data, sysvars) = setup_vote_init();
        process_vote(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let vote_acc = hash(b"vote");
        let ix = Instruction {
            program_id: *VOTE_PROGRAM_ID,
            accounts: vec![AccountMeta::new(vote_acc, false)],
            data: borsh::to_vec(&VoteInstruction::UpdateCommission(25)).unwrap(),
        };
        let msg = Message::new(&[ix], &vote_acc).unwrap();

        let c_accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &vote_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &vote_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else {
                    (*k, Account::new(0, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();

        let compiled = msg.instructions[0].accounts.clone();
        let c_data = msg.instructions[0].data.clone();
        let mut c_ctx = TransactionContext::new(c_accounts, msg, 100, 100_000);
        process_vote(&compiled, &c_data, &mut c_ctx, &sysvars).unwrap();

        let idx = c_ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &vote_acc)
            .unwrap();
        let acc = c_ctx.get_account(idx).unwrap();
        let state = VoteState::try_from_slice(&acc.account.data).unwrap();
        assert_eq!(state.commission, 25);
    }
}
