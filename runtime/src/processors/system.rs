use borsh::BorshDeserialize;
use nusantara_system_program::SystemInstruction;

use crate::error::RuntimeError;
use crate::sysvar_cache::SysvarCache;
use crate::transaction_context::TransactionContext;

const CREATE_ACCOUNT_COST: u64 = 1500;
const TRANSFER_COST: u64 = 450;
const ASSIGN_COST: u64 = 450;
const ALLOCATE_COST: u64 = 450;

pub fn process_system(
    accounts: &[u8],
    data: &[u8],
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    let instruction = SystemInstruction::try_from_slice(data)
        .map_err(|e| RuntimeError::InvalidInstructionData(e.to_string()))?;

    match instruction {
        SystemInstruction::CreateAccount {
            lamports,
            space,
            owner,
        } => {
            ctx.consume_compute(CREATE_ACCOUNT_COST)?;
            process_create_account(accounts, lamports, space, owner, ctx, sysvars)
        }
        SystemInstruction::Transfer { lamports } => {
            ctx.consume_compute(TRANSFER_COST)?;
            process_transfer(accounts, lamports, ctx)
        }
        SystemInstruction::Assign { owner } => {
            ctx.consume_compute(ASSIGN_COST)?;
            process_assign(accounts, owner, ctx)
        }
        SystemInstruction::Allocate { space } => {
            ctx.consume_compute(ALLOCATE_COST)?;
            process_allocate(accounts, space, ctx)
        }
        SystemInstruction::AdvanceNonceAccount
        | SystemInstruction::WithdrawNonceAccount(_)
        | SystemInstruction::InitializeNonceAccount(_)
        | SystemInstruction::AuthorizeNonceAccount(_)
        | SystemInstruction::CreateAccountWithSeed { .. } => Err(RuntimeError::ProgramError {
            program: "system".to_string(),
            message: "instruction not yet implemented".to_string(),
        }),
    }
}

fn process_create_account(
    accounts: &[u8],
    lamports: u64,
    space: u64,
    owner: nusantara_crypto::Hash,
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    if accounts.len() < 2 {
        return Err(RuntimeError::InvalidInstructionData(
            "CreateAccount requires 2 accounts".to_string(),
        ));
    }
    let funder_idx = accounts[0] as usize;
    let new_idx = accounts[1] as usize;

    // Verify new account is signer
    {
        let new_acc = ctx.get_account(new_idx)?;
        if !new_acc.is_signer {
            return Err(RuntimeError::AccountNotSigner(new_idx));
        }
        // Verify new account is empty
        if !new_acc.account.is_empty() {
            return Err(RuntimeError::AccountAlreadyExists);
        }
    }

    // Verify funder is signer
    {
        let funder = ctx.get_account(funder_idx)?;
        if !funder.is_signer {
            return Err(RuntimeError::AccountNotSigner(funder_idx));
        }
    }

    // Check rent exemption
    let min_balance = sysvars.rent().minimum_balance(space as usize);
    if lamports < min_balance {
        return Err(RuntimeError::RentNotMet {
            needed: min_balance,
            available: lamports,
        });
    }

    // Debit funder
    {
        let funder = ctx.get_account_mut(funder_idx)?;
        if funder.account.lamports < lamports {
            return Err(RuntimeError::InsufficientFunds {
                needed: lamports,
                available: funder.account.lamports,
            });
        }
        funder.account.lamports -= lamports;
    }

    // Credit and configure new account
    {
        let new_acc = ctx.get_account_mut(new_idx)?;
        new_acc.account.lamports = lamports;
        new_acc.account.owner = owner;
        new_acc.account.data = vec![0u8; space as usize];
    }

    Ok(())
}

fn process_transfer(
    accounts: &[u8],
    lamports: u64,
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.len() < 2 {
        return Err(RuntimeError::InvalidInstructionData(
            "Transfer requires 2 accounts".to_string(),
        ));
    }
    let from_idx = accounts[0] as usize;
    let to_idx = accounts[1] as usize;

    // Verify signer
    {
        let from = ctx.get_account(from_idx)?;
        if !from.is_signer {
            return Err(RuntimeError::AccountNotSigner(from_idx));
        }
    }

    // Debit
    {
        let from = ctx.get_account_mut(from_idx)?;
        if from.account.lamports < lamports {
            return Err(RuntimeError::InsufficientFunds {
                needed: lamports,
                available: from.account.lamports,
            });
        }
        from.account.lamports -= lamports;
    }

    // Credit
    {
        let to = ctx.get_account_mut(to_idx)?;
        to.account.lamports = to
            .account
            .lamports
            .checked_add(lamports)
            .ok_or(RuntimeError::LamportsOverflow)?;
    }

    Ok(())
}

fn process_assign(
    accounts: &[u8],
    owner: nusantara_crypto::Hash,
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.is_empty() {
        return Err(RuntimeError::InvalidInstructionData(
            "Assign requires 1 account".to_string(),
        ));
    }
    let account_idx = accounts[0] as usize;

    {
        let acc = ctx.get_account(account_idx)?;
        if !acc.is_signer {
            return Err(RuntimeError::AccountNotSigner(account_idx));
        }
    }

    let acc = ctx.get_account_mut(account_idx)?;
    acc.account.owner = owner;
    Ok(())
}

fn process_allocate(
    accounts: &[u8],
    space: u64,
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.is_empty() {
        return Err(RuntimeError::InvalidInstructionData(
            "Allocate requires 1 account".to_string(),
        ));
    }
    let account_idx = accounts[0] as usize;

    {
        let acc = ctx.get_account(account_idx)?;
        if !acc.is_signer {
            return Err(RuntimeError::AccountNotSigner(account_idx));
        }
        if !acc.account.data.is_empty() {
            return Err(RuntimeError::InvalidAccountData(
                "account already has data".to_string(),
            ));
        }
    }

    let acc = ctx.get_account_mut(account_idx)?;
    acc.account.data = vec![0u8; space as usize];
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_core::instruction::{AccountMeta, Instruction};
    use nusantara_core::program::SYSTEM_PROGRAM_ID;
    use nusantara_core::{Account, EpochSchedule, Message};
    use nusantara_crypto::hash;
    use nusantara_rent_program::Rent;
    use nusantara_sysvar_program::{Clock, RecentBlockhashes, SlotHashes, StakeHistory};

    fn test_sysvars() -> SysvarCache {
        SysvarCache::new(
            Clock::default(),
            Rent::default(),
            EpochSchedule::default(),
            SlotHashes::default(),
            StakeHistory::default(),
            RecentBlockhashes::default(),
        )
    }

    fn setup_transfer(
        from_balance: u64,
        to_balance: u64,
        transfer_amount: u64,
    ) -> (TransactionContext, Vec<u8>, Vec<u8>, SysvarCache) {
        let from = hash(b"from");
        let to = hash(b"to");
        let ix = nusantara_system_program::transfer(&from, &to, transfer_amount);
        let msg = Message::new(&[ix], &from).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &from {
                    (*k, Account::new(from_balance, hash(b"system")))
                } else if k == &to {
                    (*k, Account::new(to_balance, hash(b"system")))
                } else {
                    (*k, Account::new(0, hash(b"system")))
                }
            })
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        (ctx, compiled_accounts, data, test_sysvars())
    }

    #[test]
    fn create_account_success() {
        let from = hash(b"funder");
        let new_acc = hash(b"new");
        let owner = hash(b"owner");
        let rent = Rent::default();
        let min = rent.minimum_balance(100);
        let ix = nusantara_system_program::create_account(&from, &new_acc, min, 100, &owner);
        let msg = Message::new(&[ix], &from).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &from {
                    (*k, Account::new(min + 100_000, hash(b"system")))
                } else {
                    (*k, Account::new(0, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        process_system(&compiled_accounts, &data, &mut ctx, &sysvars).unwrap();

        let new_idx = ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &new_acc)
            .unwrap();
        let acc = ctx.get_account(new_idx).unwrap();
        assert_eq!(acc.account.lamports, min);
        assert_eq!(acc.account.owner, owner);
        assert_eq!(acc.account.data.len(), 100);
    }

    #[test]
    fn create_account_insufficient_funds() {
        let from = hash(b"funder");
        let new_acc = hash(b"new");
        let owner = hash(b"owner");
        let rent = Rent::default();
        let min = rent.minimum_balance(100);
        let ix = nusantara_system_program::create_account(&from, &new_acc, min, 100, &owner);
        let msg = Message::new(&[ix], &from).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &from {
                    (*k, Account::new(100, hash(b"system"))) // not enough
                } else {
                    (*k, Account::new(0, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        let err = process_system(&compiled_accounts, &data, &mut ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::InsufficientFunds { .. }));
    }

    #[test]
    fn create_account_below_rent() {
        let from = hash(b"funder");
        let new_acc = hash(b"new");
        let owner = hash(b"owner");
        // Set lamports below rent minimum
        let ix = nusantara_system_program::create_account(&from, &new_acc, 100, 100, &owner);
        let msg = Message::new(&[ix], &from).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &from {
                    (*k, Account::new(1_000_000, hash(b"system")))
                } else {
                    (*k, Account::new(0, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        let err = process_system(&compiled_accounts, &data, &mut ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::RentNotMet { .. }));
    }

    #[test]
    fn create_account_already_exists() {
        let from = hash(b"funder");
        let existing = hash(b"existing");
        let owner = hash(b"owner");
        let rent = Rent::default();
        let min = rent.minimum_balance(100);
        let ix = nusantara_system_program::create_account(&from, &existing, min, 100, &owner);
        let msg = Message::new(&[ix], &from).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &from {
                    (*k, Account::new(min + 100_000, hash(b"system")))
                } else if k == &existing {
                    (*k, Account::new(500, hash(b"system"))) // already has lamports
                } else {
                    (*k, Account::new(0, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        let err = process_system(&compiled_accounts, &data, &mut ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::AccountAlreadyExists));
    }

    #[test]
    fn create_account_not_signer() {
        let from = hash(b"funder");
        let new_acc = hash(b"new");
        let owner = hash(b"owner");
        let rent = Rent::default();
        let min = rent.minimum_balance(100);
        // Build instruction where new_acc is NOT a signer
        let ix = Instruction {
            program_id: *SYSTEM_PROGRAM_ID,
            accounts: vec![
                AccountMeta::new(from, true),
                AccountMeta::new(new_acc, false), // not signer!
            ],
            data: borsh::to_vec(&SystemInstruction::CreateAccount {
                lamports: min,
                space: 100,
                owner,
            })
            .unwrap(),
        };
        let msg = Message::new(&[ix], &from).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| {
                if k == &from {
                    (*k, Account::new(min + 100_000, hash(b"system")))
                } else {
                    (*k, Account::new(0, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        let err = process_system(&compiled_accounts, &data, &mut ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::AccountNotSigner(_)));
    }

    #[test]
    fn transfer_success() {
        let (mut ctx, accounts, data, sysvars) = setup_transfer(1000, 500, 300);
        process_system(&accounts, &data, &mut ctx, &sysvars).unwrap();
        let balances = ctx.post_balances();
        let from_idx = ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &hash(b"from"))
            .unwrap();
        let to_idx = ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &hash(b"to"))
            .unwrap();
        assert_eq!(balances[from_idx], 700);
        assert_eq!(balances[to_idx], 800);
    }

    #[test]
    fn transfer_insufficient() {
        let (mut ctx, accounts, data, sysvars) = setup_transfer(100, 500, 300);
        let err = process_system(&accounts, &data, &mut ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::InsufficientFunds { .. }));
    }

    #[test]
    fn transfer_not_signer() {
        let from = hash(b"from");
        let to = hash(b"to");
        let ix = Instruction {
            program_id: *SYSTEM_PROGRAM_ID,
            accounts: vec![
                AccountMeta::new(from, false), // not signer!
                AccountMeta::new(to, false),
            ],
            data: borsh::to_vec(&SystemInstruction::Transfer { lamports: 100 }).unwrap(),
        };
        let msg = Message::new(&[ix], &hash(b"payer")).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| (*k, Account::new(1000, hash(b"system"))))
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        let err = process_system(&compiled_accounts, &data, &mut ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::AccountNotSigner(_)));
    }

    #[test]
    fn transfer_overflow() {
        let (mut ctx, accounts, data, sysvars) = setup_transfer(100, u64::MAX, 100);
        let err = process_system(&accounts, &data, &mut ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::LamportsOverflow));
    }

    #[test]
    fn assign_success() {
        let account = hash(b"account");
        let new_owner = hash(b"new_owner");
        let ix = nusantara_system_program::assign(&account, &new_owner);
        let msg = Message::new(&[ix], &account).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| (*k, Account::new(1000, hash(b"system"))))
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        process_system(&compiled_accounts, &data, &mut ctx, &sysvars).unwrap();

        let idx = ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &account)
            .unwrap();
        assert_eq!(ctx.get_account(idx).unwrap().account.owner, new_owner);
    }

    #[test]
    fn assign_not_signer() {
        let account = hash(b"account");
        let new_owner = hash(b"new_owner");
        let ix = Instruction {
            program_id: *SYSTEM_PROGRAM_ID,
            accounts: vec![AccountMeta::new(account, false)],
            data: borsh::to_vec(&SystemInstruction::Assign { owner: new_owner }).unwrap(),
        };
        let msg = Message::new(&[ix], &hash(b"payer")).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| (*k, Account::new(1000, hash(b"system"))))
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        let err = process_system(&compiled_accounts, &data, &mut ctx, &sysvars).unwrap_err();
        assert!(matches!(err, RuntimeError::AccountNotSigner(_)));
    }

    #[test]
    fn allocate_success() {
        let account = hash(b"account");
        let ix = nusantara_system_program::allocate(&account, 200);
        let msg = Message::new(&[ix], &account).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| (*k, Account::new(1000, hash(b"system"))))
            .collect();
        let compiled_accounts = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        process_system(&compiled_accounts, &data, &mut ctx, &sysvars).unwrap();

        let idx = ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &account)
            .unwrap();
        assert_eq!(ctx.get_account(idx).unwrap().account.data.len(), 200);
    }
}
