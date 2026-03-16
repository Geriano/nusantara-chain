use borsh::BorshDeserialize;
use nusantara_stake_program::{
    Authorized, DEFAULT_MIN_DELEGATION, DEFAULT_WARMUP_COOLDOWN_RATE_BPS, Delegation, Lockup, Meta,
    Stake, StakeInstruction, StakeStateV2,
};

use crate::error::RuntimeError;
use crate::sysvar_cache::SysvarCache;
use crate::transaction_context::TransactionContext;

const STAKE_BASE_COST: u64 = 750;

pub fn process_stake(
    accounts: &[u8],
    data: &[u8],
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    ctx.consume_compute(STAKE_BASE_COST)?;

    let instruction = StakeInstruction::try_from_slice(data)
        .map_err(|e| RuntimeError::InvalidInstructionData(e.to_string()))?;

    match instruction {
        StakeInstruction::Initialize(authorized, lockup) => {
            process_initialize(accounts, authorized, lockup, ctx, sysvars)
        }
        StakeInstruction::DelegateStake => process_delegate(accounts, ctx, sysvars),
        StakeInstruction::Deactivate => process_deactivate(accounts, ctx, sysvars),
        StakeInstruction::Withdraw(lamports) => process_withdraw(accounts, lamports, ctx, sysvars),
        StakeInstruction::Split(lamports) => process_split(accounts, lamports, ctx, sysvars),
        StakeInstruction::Merge
        | StakeInstruction::Authorize(_, _)
        | StakeInstruction::SetLockup(_) => Err(RuntimeError::ProgramError {
            program: "stake".to_string(),
            message: "instruction not yet implemented".to_string(),
        }),
    }
}

fn process_initialize(
    accounts: &[u8],
    authorized: Authorized,
    lockup: Lockup,
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    if accounts.is_empty() {
        return Err(RuntimeError::InvalidInstructionData(
            "Initialize requires 1 account".to_string(),
        ));
    }
    let stake_idx = accounts[0] as usize;

    // Check current state
    let current_state = {
        let acc = ctx.get_account(stake_idx)?;
        if acc.account.data.is_empty() {
            StakeStateV2::Uninitialized
        } else {
            BorshDeserialize::deserialize(&mut acc.account.data.as_slice())
                .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?
        }
    };

    if current_state != StakeStateV2::Uninitialized {
        return Err(RuntimeError::InvalidAccountData(
            "stake account already initialized".to_string(),
        ));
    }

    // Check rent exemption
    let rent_exempt_reserve = {
        let acc = ctx.get_account(stake_idx)?;
        let reserve = sysvars.rent().minimum_balance(acc.account.data.len());
        if acc.account.lamports < reserve {
            return Err(RuntimeError::RentNotMet {
                needed: reserve,
                available: acc.account.lamports,
            });
        }
        reserve
    };

    let meta = Meta {
        rent_exempt_reserve,
        authorized,
        lockup,
    };
    let state = StakeStateV2::Initialized(meta);
    let state_data =
        borsh::to_vec(&state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    let acc = ctx.get_account_mut(stake_idx)?;
    acc.account.data = state_data;
    Ok(())
}

fn process_delegate(
    accounts: &[u8],
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(RuntimeError::InvalidInstructionData(
            "DelegateStake requires 3 accounts".to_string(),
        ));
    }
    let stake_idx = accounts[0] as usize;
    let vote_idx = accounts[1] as usize;
    let staker_idx = accounts[2] as usize;

    // Verify staker is signer
    {
        let staker = ctx.get_account(staker_idx)?;
        if !staker.is_signer {
            return Err(RuntimeError::AccountNotSigner(staker_idx));
        }
    }

    let staker_address = {
        let staker = ctx.get_account(staker_idx)?;
        *staker.address
    };

    let vote_address = {
        let vote = ctx.get_account(vote_idx)?;
        *vote.address
    };

    // Load current stake state
    let (meta, _current_state) = {
        let acc = ctx.get_account(stake_idx)?;
        let state = StakeStateV2::try_from_slice(&acc.account.data)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;
        match state {
            StakeStateV2::Initialized(m) => (m, "initialized"),
            _ => {
                return Err(RuntimeError::InvalidAccountData(
                    "stake account must be initialized to delegate".to_string(),
                ));
            }
        }
    };

    // Verify staker authorization
    if meta.authorized.staker != staker_address {
        return Err(RuntimeError::AccountNotSigner(staker_idx));
    }

    let stake_lamports = {
        let acc = ctx.get_account(stake_idx)?;
        acc.account
            .lamports
            .saturating_sub(meta.rent_exempt_reserve)
    };

    if stake_lamports < DEFAULT_MIN_DELEGATION {
        return Err(RuntimeError::InsufficientFunds {
            needed: DEFAULT_MIN_DELEGATION,
            available: stake_lamports,
        });
    }

    let delegation = Delegation {
        voter_pubkey: vote_address,
        stake: stake_lamports,
        activation_epoch: sysvars.clock().epoch,
        deactivation_epoch: u64::MAX,
        warmup_cooldown_rate_bps: DEFAULT_WARMUP_COOLDOWN_RATE_BPS,
    };

    let stake = Stake {
        delegation,
        credits_observed: 0,
    };

    let new_state = StakeStateV2::Stake(meta, stake);
    let state_data =
        borsh::to_vec(&new_state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    let acc = ctx.get_account_mut(stake_idx)?;
    acc.account.data = state_data;
    Ok(())
}

fn process_deactivate(
    accounts: &[u8],
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    if accounts.len() < 2 {
        return Err(RuntimeError::InvalidInstructionData(
            "Deactivate requires 2 accounts".to_string(),
        ));
    }
    let stake_idx = accounts[0] as usize;
    let staker_idx = accounts[1] as usize;

    // Verify staker is signer
    let staker_address = {
        let staker = ctx.get_account(staker_idx)?;
        if !staker.is_signer {
            return Err(RuntimeError::AccountNotSigner(staker_idx));
        }
        *staker.address
    };

    let (meta, mut stake) = {
        let acc = ctx.get_account(stake_idx)?;
        let state = StakeStateV2::try_from_slice(&acc.account.data)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;
        match state {
            StakeStateV2::Stake(m, s) => (m, s),
            _ => {
                return Err(RuntimeError::InvalidAccountData(
                    "stake account must be delegated to deactivate".to_string(),
                ));
            }
        }
    };

    if meta.authorized.staker != staker_address {
        return Err(RuntimeError::AccountNotSigner(staker_idx));
    }

    stake.delegation.deactivation_epoch = sysvars.clock().epoch;

    let new_state = StakeStateV2::Stake(meta, stake);
    let state_data =
        borsh::to_vec(&new_state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    let acc = ctx.get_account_mut(stake_idx)?;
    acc.account.data = state_data;
    Ok(())
}

fn process_withdraw(
    accounts: &[u8],
    lamports: u64,
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(RuntimeError::InvalidInstructionData(
            "Withdraw requires 3 accounts".to_string(),
        ));
    }
    let stake_idx = accounts[0] as usize;
    let to_idx = accounts[1] as usize;
    let withdrawer_idx = accounts[2] as usize;

    // Verify withdrawer is signer
    let withdrawer_address = {
        let withdrawer = ctx.get_account(withdrawer_idx)?;
        if !withdrawer.is_signer {
            return Err(RuntimeError::AccountNotSigner(withdrawer_idx));
        }
        *withdrawer.address
    };

    let state = {
        let acc = ctx.get_account(stake_idx)?;
        StakeStateV2::try_from_slice(&acc.account.data)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?
    };

    let meta = match &state {
        StakeStateV2::Initialized(m) | StakeStateV2::Stake(m, _) => m,
        _ => {
            return Err(RuntimeError::InvalidAccountData(
                "stake account not initialized".to_string(),
            ));
        }
    };

    if meta.authorized.withdrawer != withdrawer_address {
        return Err(RuntimeError::AccountNotSigner(withdrawer_idx));
    }

    // Check lockup
    if meta.lockup.unix_timestamp > sysvars.clock().unix_timestamp
        || meta.lockup.epoch > sysvars.clock().epoch
    {
        return Err(RuntimeError::ProgramError {
            program: "stake".to_string(),
            message: "stake account is locked".to_string(),
        });
    }

    // Check available balance
    let available = {
        let acc = ctx.get_account(stake_idx)?;
        match &state {
            StakeStateV2::Initialized(_) => acc
                .account
                .lamports
                .saturating_sub(meta.rent_exempt_reserve),
            StakeStateV2::Stake(_, s) => {
                if s.delegation.deactivation_epoch < sysvars.clock().epoch {
                    acc.account
                        .lamports
                        .saturating_sub(meta.rent_exempt_reserve)
                } else {
                    0 // cannot withdraw while active
                }
            }
            _ => 0,
        }
    };

    if lamports > available {
        return Err(RuntimeError::InsufficientFunds {
            needed: lamports,
            available,
        });
    }

    // Debit stake account
    {
        let acc = ctx.get_account_mut(stake_idx)?;
        acc.account.lamports -= lamports;
    }

    // Credit destination
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

fn process_split(
    accounts: &[u8],
    lamports: u64,
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(RuntimeError::InvalidInstructionData(
            "Split requires 3 accounts".to_string(),
        ));
    }
    let stake_idx = accounts[0] as usize;
    let split_idx = accounts[1] as usize;
    let staker_idx = accounts[2] as usize;

    // Verify staker is signer
    let staker_address = {
        let staker = ctx.get_account(staker_idx)?;
        if !staker.is_signer {
            return Err(RuntimeError::AccountNotSigner(staker_idx));
        }
        *staker.address
    };

    let (meta, stake_opt) = {
        let acc = ctx.get_account(stake_idx)?;
        let state = StakeStateV2::try_from_slice(&acc.account.data)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;
        match state {
            StakeStateV2::Initialized(m) => (m, None),
            StakeStateV2::Stake(m, s) => (m, Some(s)),
            _ => {
                return Err(RuntimeError::InvalidAccountData(
                    "stake account not initialized".to_string(),
                ));
            }
        }
    };

    if meta.authorized.staker != staker_address {
        return Err(RuntimeError::AccountNotSigner(staker_idx));
    }

    let source_lamports = {
        let acc = ctx.get_account(stake_idx)?;
        acc.account.lamports
    };

    if lamports > source_lamports.saturating_sub(meta.rent_exempt_reserve) {
        return Err(RuntimeError::InsufficientFunds {
            needed: lamports,
            available: source_lamports.saturating_sub(meta.rent_exempt_reserve),
        });
    }

    // Check rent on split account
    let split_rent_exempt = {
        let acc = ctx.get_account(stake_idx)?;
        sysvars.rent().minimum_balance(acc.account.data.len())
    };

    // Build split state
    let split_state = if let Some(ref original_stake) = stake_opt {
        let original_total = source_lamports.saturating_sub(meta.rent_exempt_reserve);
        let split_stake_amount = if original_total > 0 {
            (original_stake.delegation.stake as u128 * lamports as u128 / original_total as u128)
                as u64
        } else {
            0
        };

        let split_delegation = Delegation {
            voter_pubkey: original_stake.delegation.voter_pubkey,
            stake: split_stake_amount,
            activation_epoch: original_stake.delegation.activation_epoch,
            deactivation_epoch: original_stake.delegation.deactivation_epoch,
            warmup_cooldown_rate_bps: original_stake.delegation.warmup_cooldown_rate_bps,
        };

        StakeStateV2::Stake(
            Meta {
                rent_exempt_reserve: split_rent_exempt,
                authorized: meta.authorized.clone(),
                lockup: meta.lockup.clone(),
            },
            Stake {
                delegation: split_delegation,
                credits_observed: original_stake.credits_observed,
            },
        )
    } else {
        StakeStateV2::Initialized(Meta {
            rent_exempt_reserve: split_rent_exempt,
            authorized: meta.authorized.clone(),
            lockup: meta.lockup.clone(),
        })
    };

    let split_data =
        borsh::to_vec(&split_state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    // Update source: reduce lamports and stake
    if let Some(mut original_stake) = stake_opt {
        let original_total = source_lamports.saturating_sub(meta.rent_exempt_reserve);
        let split_stake_amount = if original_total > 0 {
            (original_stake.delegation.stake as u128 * lamports as u128 / original_total as u128)
                as u64
        } else {
            0
        };
        original_stake.delegation.stake -= split_stake_amount;

        let updated_source = StakeStateV2::Stake(meta, original_stake);
        let source_data = borsh::to_vec(&updated_source)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

        let acc = ctx.get_account_mut(stake_idx)?;
        acc.account.lamports -= lamports;
        acc.account.data = source_data;
    } else {
        let acc = ctx.get_account_mut(stake_idx)?;
        acc.account.lamports -= lamports;
    }

    // Configure split account
    {
        let acc = ctx.get_account_mut(split_idx)?;
        acc.account.lamports = acc
            .account
            .lamports
            .checked_add(lamports)
            .ok_or(RuntimeError::LamportsOverflow)?;
        acc.account.data = split_data;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_core::program::STAKE_PROGRAM_ID;
    use nusantara_core::{Account, EpochSchedule, Message};
    use nusantara_crypto::{Hash, hash};
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

    fn setup_stake_init() -> (TransactionContext, Vec<u8>, Vec<u8>, SysvarCache) {
        let stake_acc = hash(b"stake");
        let staker = hash(b"staker");
        let withdrawer = hash(b"withdrawer");

        let authorized = Authorized { staker, withdrawer };
        let lockup = Lockup {
            unix_timestamp: 0,
            epoch: 0,
            custodian: Hash::zero(),
        };

        let ix = nusantara_stake_program::initialize(&stake_acc, authorized, lockup);
        let msg = Message::new(&[ix], &stake_acc).unwrap();

        let rent = Rent::default();
        // Estimate data size for StakeStateV2::Initialized
        let state = StakeStateV2::Initialized(Meta {
            rent_exempt_reserve: 0,
            authorized: Authorized { staker, withdrawer },
            lockup: Lockup {
                unix_timestamp: 0,
                epoch: 0,
                custodian: Hash::zero(),
            },
        });
        let state_size = borsh::to_vec(&state).unwrap().len();
        let min = rent.minimum_balance(state_size);

        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &stake_acc {
                    let mut a = Account::new(min + 1_000_000_000, *STAKE_PROGRAM_ID);
                    a.data = vec![0u8; state_size]; // pre-allocate
                    (*k, a)
                } else {
                    (*k, Account::new(0, Hash::zero()))
                }
            })
            .collect();

        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let ctx = TransactionContext::new(accounts, msg, 100, 100_000);
        (ctx, compiled_accounts, data, test_sysvars())
    }

    #[test]
    fn initialize_success() {
        let (mut ctx, accounts, data, sysvars) = setup_stake_init();
        process_stake(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let stake_idx = ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &hash(b"stake"))
            .unwrap();
        let acc = ctx.get_account(stake_idx).unwrap();
        let state = StakeStateV2::try_from_slice(&acc.account.data).unwrap();
        assert!(matches!(state, StakeStateV2::Initialized(_)));
    }

    #[test]
    fn initialize_already_initialized() {
        let (mut ctx, accounts, data, sysvars) = setup_stake_init();
        process_stake(&accounts, &data, &mut ctx, &sysvars).unwrap();
        // Try to initialize again
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
        let err = process_stake(&accounts, &data, &mut ctx2, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidAccountData(_)));
    }

    #[test]
    fn delegate_success() {
        let (mut ctx, accounts, data, sysvars) = setup_stake_init();
        process_stake(&accounts, &data, &mut ctx, &sysvars).unwrap();

        // Now delegate
        let stake_acc = hash(b"stake");
        let vote_acc = hash(b"vote");
        let staker = hash(b"staker");
        let del_ix = nusantara_stake_program::delegate_stake(&stake_acc, &vote_acc, &staker);
        let del_msg = Message::new(&[del_ix], &staker).unwrap();

        let del_accounts: Vec<_> = del_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &stake_acc {
                    let stake_idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &stake_acc)
                        .unwrap();
                    (*k, ctx.get_account(stake_idx).unwrap().account.clone())
                } else if k == &staker {
                    (*k, Account::new(1_000_000, Hash::zero()))
                } else {
                    (*k, Account::new(0, Hash::zero()))
                }
            })
            .collect();

        let compiled = del_msg.instructions[0].accounts.clone();
        let del_data = del_msg.instructions[0].data.clone();
        let mut del_ctx = TransactionContext::new(del_accounts, del_msg, 100, 100_000);
        process_stake(&compiled, &del_data, &mut del_ctx, &sysvars).unwrap();

        let idx = del_ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &stake_acc)
            .unwrap();
        let acc = del_ctx.get_account(idx).unwrap();
        let state = StakeStateV2::try_from_slice(&acc.account.data).unwrap();
        assert!(matches!(state, StakeStateV2::Stake(_, _)));
    }

    #[test]
    fn delegate_not_initialized() {
        let stake_acc = hash(b"stake");
        let vote_acc = hash(b"vote");
        let staker = hash(b"staker");
        let ix = nusantara_stake_program::delegate_stake(&stake_acc, &vote_acc, &staker);
        let msg = Message::new(&[ix], &staker).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &stake_acc {
                    let mut a = Account::new(2_000_000_000, *STAKE_PROGRAM_ID);
                    let state = StakeStateV2::Uninitialized;
                    a.data = borsh::to_vec(&state).unwrap();
                    (*k, a)
                } else {
                    (*k, Account::new(1_000_000, Hash::zero()))
                }
            })
            .collect();
        let compiled = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 100, 100_000);
        let err = process_stake(&compiled, &data, &mut ctx, &test_sysvars()).unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidAccountData(_)));
    }

    #[test]
    fn delegate_wrong_signer() {
        let (mut ctx, accounts, data, sysvars) = setup_stake_init();
        process_stake(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let stake_acc = hash(b"stake");
        let vote_acc = hash(b"vote");
        let wrong_staker = hash(b"wrong_staker");
        let del_ix = nusantara_stake_program::delegate_stake(&stake_acc, &vote_acc, &wrong_staker);
        let del_msg = Message::new(&[del_ix], &wrong_staker).unwrap();

        let del_accounts: Vec<_> = del_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &stake_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &stake_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else {
                    (*k, Account::new(1_000_000, Hash::zero()))
                }
            })
            .collect();

        let compiled = del_msg.instructions[0].accounts.clone();
        let del_data = del_msg.instructions[0].data.clone();
        let mut del_ctx = TransactionContext::new(del_accounts, del_msg, 100, 100_000);
        let err = process_stake(&compiled, &del_data, &mut del_ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::AccountNotSigner(_)));
    }

    #[test]
    fn withdraw_success() {
        let (mut ctx, accounts, data, sysvars) = setup_stake_init();
        process_stake(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let stake_acc = hash(b"stake");
        let withdrawer = hash(b"withdrawer");
        let to = hash(b"to");
        let w_ix = nusantara_stake_program::withdraw(&stake_acc, &withdrawer, &to, 100_000);
        let w_msg = Message::new(&[w_ix], &withdrawer).unwrap();

        let w_accounts: Vec<_> = w_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &stake_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &stake_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else if k == &withdrawer {
                    (*k, Account::new(1_000_000, Hash::zero()))
                } else {
                    (*k, Account::new(0, Hash::zero()))
                }
            })
            .collect();

        let compiled = w_msg.instructions[0].accounts.clone();
        let w_data = w_msg.instructions[0].data.clone();
        let mut w_ctx = TransactionContext::new(w_accounts, w_msg, 100, 100_000);
        process_stake(&compiled, &w_data, &mut w_ctx, &sysvars).unwrap();
    }

    #[test]
    fn deactivate_success() {
        // First initialize and delegate, then deactivate
        let (mut ctx, accounts, data, sysvars) = setup_stake_init();
        process_stake(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let stake_acc = hash(b"stake");
        let vote_acc = hash(b"vote");
        let staker = hash(b"staker");

        // Delegate first
        let del_ix = nusantara_stake_program::delegate_stake(&stake_acc, &vote_acc, &staker);
        let del_msg = Message::new(&[del_ix], &staker).unwrap();
        let del_accounts: Vec<_> = del_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &stake_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &stake_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else {
                    (*k, Account::new(1_000_000, Hash::zero()))
                }
            })
            .collect();
        let compiled_del = del_msg.instructions[0].accounts.clone();
        let del_data = del_msg.instructions[0].data.clone();
        let mut del_ctx = TransactionContext::new(del_accounts, del_msg, 100, 100_000);
        process_stake(&compiled_del, &del_data, &mut del_ctx, &sysvars).unwrap();

        // Now deactivate
        let deact_ix = nusantara_stake_program::deactivate(&stake_acc, &staker);
        let deact_msg = Message::new(&[deact_ix], &staker).unwrap();
        let deact_accounts: Vec<_> = deact_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &stake_acc {
                    let idx = del_ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &stake_acc)
                        .unwrap();
                    (*k, del_ctx.get_account(idx).unwrap().account.clone())
                } else {
                    (*k, Account::new(1_000_000, Hash::zero()))
                }
            })
            .collect();
        let compiled_deact = deact_msg.instructions[0].accounts.clone();
        let deact_data = deact_msg.instructions[0].data.clone();
        let mut deact_ctx = TransactionContext::new(deact_accounts, deact_msg, 100, 100_000);
        process_stake(&compiled_deact, &deact_data, &mut deact_ctx, &sysvars).unwrap();

        let idx = deact_ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &stake_acc)
            .unwrap();
        let acc = deact_ctx.get_account(idx).unwrap();
        let state = StakeStateV2::try_from_slice(&acc.account.data).unwrap();
        if let StakeStateV2::Stake(_, s) = state {
            assert_eq!(s.delegation.deactivation_epoch, 5);
        } else {
            panic!("expected Stake state");
        }
    }

    #[test]
    fn split_success() {
        let (mut ctx, accounts, data, sysvars) = setup_stake_init();
        process_stake(&accounts, &data, &mut ctx, &sysvars).unwrap();

        let stake_acc = hash(b"stake");
        let split_acc = hash(b"split");
        let staker = hash(b"staker");

        let split_ix = nusantara_stake_program::split(&stake_acc, &staker, &split_acc, 500_000_000);
        let split_msg = Message::new(&[split_ix], &staker).unwrap();

        let state_size = {
            let idx = ctx
                .message()
                .account_keys
                .iter()
                .position(|a| a == &stake_acc)
                .unwrap();
            ctx.get_account(idx).unwrap().account.data.len()
        };

        let split_accounts: Vec<_> = split_msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &stake_acc {
                    let idx = ctx
                        .message()
                        .account_keys
                        .iter()
                        .position(|a| a == &stake_acc)
                        .unwrap();
                    (*k, ctx.get_account(idx).unwrap().account.clone())
                } else if k == &split_acc {
                    let mut a = Account::new(0, *STAKE_PROGRAM_ID);
                    a.data = vec![0u8; state_size];
                    (*k, a)
                } else {
                    (*k, Account::new(1_000_000, Hash::zero()))
                }
            })
            .collect();

        let compiled = split_msg.instructions[0].accounts.clone();
        let split_data = split_msg.instructions[0].data.clone();
        let mut split_ctx = TransactionContext::new(split_accounts, split_msg, 100, 100_000);
        process_stake(&compiled, &split_data, &mut split_ctx, &sysvars).unwrap();
    }
}
