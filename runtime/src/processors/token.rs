use borsh::BorshDeserialize;
use nusantara_core::program::TOKEN_PROGRAM_ID;
use nusantara_token_program::TokenInstruction;
use nusantara_token_program::error::TokenError;
use nusantara_token_program::state::{AccountState, Mint, TokenAccount};

use crate::error::RuntimeError;
use crate::sysvar_cache::SysvarCache;
use crate::transaction_context::TransactionContext;

const INIT_MINT_COST: u64 = 1000;
const INIT_ACCOUNT_COST: u64 = 1000;
const MINT_TO_COST: u64 = 500;
const TRANSFER_COST: u64 = 300;
const APPROVE_COST: u64 = 300;
const REVOKE_COST: u64 = 300;
const BURN_COST: u64 = 500;
const CLOSE_COST: u64 = 500;
const FREEZE_COST: u64 = 300;
const THAW_COST: u64 = 300;

pub fn process_token(
    accounts: &[u8],
    data: &[u8],
    ctx: &mut TransactionContext,
    _sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    let instruction = TokenInstruction::try_from_slice(data)
        .map_err(|e| RuntimeError::InvalidInstructionData(e.to_string()))?;

    match instruction {
        TokenInstruction::InitializeMint {
            decimals,
            mint_authority,
            freeze_authority,
        } => {
            ctx.consume_compute(INIT_MINT_COST)?;
            process_initialize_mint(accounts, ctx, decimals, mint_authority, freeze_authority)
        }
        TokenInstruction::InitializeAccount => {
            ctx.consume_compute(INIT_ACCOUNT_COST)?;
            process_initialize_account(accounts, ctx)
        }
        TokenInstruction::MintTo { amount } => {
            ctx.consume_compute(MINT_TO_COST)?;
            process_mint_to(accounts, ctx, amount)
        }
        TokenInstruction::Transfer { amount } => {
            ctx.consume_compute(TRANSFER_COST)?;
            process_transfer(accounts, ctx, amount)
        }
        TokenInstruction::Approve { amount } => {
            ctx.consume_compute(APPROVE_COST)?;
            process_approve(accounts, ctx, amount)
        }
        TokenInstruction::Revoke => {
            ctx.consume_compute(REVOKE_COST)?;
            process_revoke(accounts, ctx)
        }
        TokenInstruction::Burn { amount } => {
            ctx.consume_compute(BURN_COST)?;
            process_burn(accounts, ctx, amount)
        }
        TokenInstruction::CloseAccount => {
            ctx.consume_compute(CLOSE_COST)?;
            process_close_account(accounts, ctx)
        }
        TokenInstruction::FreezeAccount => {
            ctx.consume_compute(FREEZE_COST)?;
            process_freeze_account(accounts, ctx)
        }
        TokenInstruction::ThawAccount => {
            ctx.consume_compute(THAW_COST)?;
            process_thaw_account(accounts, ctx)
        }
    }
}

fn token_err(e: TokenError) -> RuntimeError {
    RuntimeError::ProgramError {
        program: "token".to_string(),
        message: e.to_string(),
    }
}

fn borsh_err(e: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::ProgramError {
        program: "token".to_string(),
        message: e.to_string(),
    }
}

fn process_initialize_mint(
    accounts: &[u8],
    ctx: &mut TransactionContext,
    decimals: u8,
    mint_authority: nusantara_crypto::Hash,
    freeze_authority: Option<nusantara_crypto::Hash>,
) -> Result<(), RuntimeError> {
    if accounts.is_empty() {
        return Err(token_err(TokenError::MissingAccount));
    }
    let mint_idx = accounts[0] as usize;

    // Check if already initialized
    let existing = ctx.get_account(mint_idx)?;
    if !existing.account.data.is_empty()
        && let Ok(m) = Mint::try_from_slice(&existing.account.data)
        && m.is_initialized
    {
        return Err(token_err(TokenError::AlreadyInitialized));
    }

    let mint = Mint {
        mint_authority: Some(mint_authority),
        supply: 0,
        decimals,
        is_initialized: true,
        freeze_authority,
    };

    let mint_data = borsh::to_vec(&mint).map_err(borsh_err)?;

    {
        let acc = ctx.get_account_mut(mint_idx)?;
        acc.account.data = mint_data;
        acc.account.owner = *TOKEN_PROGRAM_ID;
    }

    Ok(())
}

fn process_initialize_account(
    accounts: &[u8],
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(token_err(TokenError::MissingAccount));
    }
    let account_idx = accounts[0] as usize;
    let mint_idx = accounts[1] as usize;
    let owner_idx = accounts[2] as usize;

    // Verify mint is initialized
    let mint_acc = ctx.get_account(mint_idx)?;
    let mint = Mint::try_from_slice(&mint_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;
    if !mint.is_initialized {
        return Err(token_err(TokenError::NotInitialized));
    }
    let mint_address = *mint_acc.address;

    let owner_address = *ctx.get_account(owner_idx)?.address;

    // Check not already initialized
    let existing = ctx.get_account(account_idx)?;
    if !existing.account.data.is_empty()
        && let Ok(ta) = TokenAccount::try_from_slice(&existing.account.data)
        && ta.state != AccountState::Uninitialized
    {
        return Err(token_err(TokenError::AlreadyInitialized));
    }

    let token_account = TokenAccount {
        mint: mint_address,
        owner: owner_address,
        amount: 0,
        delegate: None,
        state: AccountState::Initialized,
        delegated_amount: 0,
        close_authority: None,
    };

    let acc_data = borsh::to_vec(&token_account).map_err(borsh_err)?;

    {
        let acc = ctx.get_account_mut(account_idx)?;
        acc.account.data = acc_data;
        acc.account.owner = *TOKEN_PROGRAM_ID;
    }

    Ok(())
}

fn process_mint_to(
    accounts: &[u8],
    ctx: &mut TransactionContext,
    amount: u64,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(token_err(TokenError::MissingAccount));
    }
    let mint_idx = accounts[0] as usize;
    let dest_idx = accounts[1] as usize;
    let auth_idx = accounts[2] as usize;

    // Verify authority is signer
    let auth = ctx.get_account(auth_idx)?;
    if !auth.is_signer {
        return Err(token_err(TokenError::MissingSigner));
    }
    let auth_address = *auth.address;

    // Load and update mint
    let mint_acc = ctx.get_account(mint_idx)?;
    let mut mint = Mint::try_from_slice(&mint_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;
    if !mint.is_initialized {
        return Err(token_err(TokenError::NotInitialized));
    }
    if mint.mint_authority != Some(auth_address) {
        return Err(token_err(TokenError::AuthorityMismatch));
    }
    mint.supply = mint
        .supply
        .checked_add(amount)
        .ok_or(token_err(TokenError::SupplyOverflow))?;

    // Load and update destination token account
    let dest_acc = ctx.get_account(dest_idx)?;
    let mut token_acc = TokenAccount::try_from_slice(&dest_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;
    if token_acc.state == AccountState::Frozen {
        return Err(token_err(TokenError::AccountFrozen));
    }
    let mint_address = *ctx.get_account(mint_idx)?.address;
    if token_acc.mint != mint_address {
        return Err(token_err(TokenError::MintMismatch));
    }
    token_acc.amount = token_acc.amount.saturating_add(amount);

    // Write back
    let mint_data = borsh::to_vec(&mint).map_err(borsh_err)?;
    ctx.get_account_mut(mint_idx)?.account.data = mint_data;

    let acc_data = borsh::to_vec(&token_acc).map_err(borsh_err)?;
    ctx.get_account_mut(dest_idx)?.account.data = acc_data;

    Ok(())
}

fn process_transfer(
    accounts: &[u8],
    ctx: &mut TransactionContext,
    amount: u64,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(token_err(TokenError::MissingAccount));
    }
    let src_idx = accounts[0] as usize;
    let dest_idx = accounts[1] as usize;
    let auth_idx = accounts[2] as usize;

    let auth = ctx.get_account(auth_idx)?;
    if !auth.is_signer {
        return Err(token_err(TokenError::MissingSigner));
    }
    let auth_address = *auth.address;

    // Load source
    let src_acc = ctx.get_account(src_idx)?;
    let mut src_token = TokenAccount::try_from_slice(&src_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;
    if src_token.state == AccountState::Frozen {
        return Err(token_err(TokenError::AccountFrozen));
    }

    // Check authority: must be owner or delegate
    let is_delegate = src_token.delegate == Some(auth_address) && src_token.delegated_amount > 0;
    if src_token.owner != auth_address && !is_delegate {
        return Err(token_err(TokenError::OwnerMismatch));
    }

    if src_token.amount < amount {
        return Err(token_err(TokenError::InsufficientBalance {
            need: amount,
            have: src_token.amount,
        }));
    }

    if is_delegate {
        if src_token.delegated_amount < amount {
            return Err(token_err(TokenError::InsufficientDelegation {
                need: amount,
                have: src_token.delegated_amount,
            }));
        }
        src_token.delegated_amount -= amount;
    }

    // Load destination
    let dest_acc = ctx.get_account(dest_idx)?;
    let mut dest_token = TokenAccount::try_from_slice(&dest_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;
    if dest_token.state == AccountState::Frozen {
        return Err(token_err(TokenError::AccountFrozen));
    }
    if src_token.mint != dest_token.mint {
        return Err(token_err(TokenError::MintMismatch));
    }

    src_token.amount -= amount;
    dest_token.amount = dest_token.amount.saturating_add(amount);

    // Write back
    let src_data = borsh::to_vec(&src_token).map_err(borsh_err)?;
    ctx.get_account_mut(src_idx)?.account.data = src_data;

    let dest_data = borsh::to_vec(&dest_token).map_err(borsh_err)?;
    ctx.get_account_mut(dest_idx)?.account.data = dest_data;

    Ok(())
}

fn process_approve(
    accounts: &[u8],
    ctx: &mut TransactionContext,
    amount: u64,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(token_err(TokenError::MissingAccount));
    }
    let src_idx = accounts[0] as usize;
    let delegate_idx = accounts[1] as usize;
    let owner_idx = accounts[2] as usize;

    let owner = ctx.get_account(owner_idx)?;
    if !owner.is_signer {
        return Err(token_err(TokenError::MissingSigner));
    }
    let owner_address = *owner.address;

    let delegate_address = *ctx.get_account(delegate_idx)?.address;

    let src_acc = ctx.get_account(src_idx)?;
    let mut token_acc = TokenAccount::try_from_slice(&src_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;

    if token_acc.owner != owner_address {
        return Err(token_err(TokenError::OwnerMismatch));
    }

    token_acc.delegate = Some(delegate_address);
    token_acc.delegated_amount = amount;

    let acc_data = borsh::to_vec(&token_acc).map_err(borsh_err)?;
    ctx.get_account_mut(src_idx)?.account.data = acc_data;

    Ok(())
}

fn process_revoke(accounts: &[u8], ctx: &mut TransactionContext) -> Result<(), RuntimeError> {
    if accounts.len() < 2 {
        return Err(token_err(TokenError::MissingAccount));
    }
    let src_idx = accounts[0] as usize;
    let owner_idx = accounts[1] as usize;

    let owner = ctx.get_account(owner_idx)?;
    if !owner.is_signer {
        return Err(token_err(TokenError::MissingSigner));
    }
    let owner_address = *owner.address;

    let src_acc = ctx.get_account(src_idx)?;
    let mut token_acc = TokenAccount::try_from_slice(&src_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;

    if token_acc.owner != owner_address {
        return Err(token_err(TokenError::OwnerMismatch));
    }

    token_acc.delegate = None;
    token_acc.delegated_amount = 0;

    let acc_data = borsh::to_vec(&token_acc).map_err(borsh_err)?;
    ctx.get_account_mut(src_idx)?.account.data = acc_data;

    Ok(())
}

fn process_burn(
    accounts: &[u8],
    ctx: &mut TransactionContext,
    amount: u64,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(token_err(TokenError::MissingAccount));
    }
    let src_idx = accounts[0] as usize;
    let mint_idx = accounts[1] as usize;
    let auth_idx = accounts[2] as usize;

    let auth = ctx.get_account(auth_idx)?;
    if !auth.is_signer {
        return Err(token_err(TokenError::MissingSigner));
    }
    let auth_address = *auth.address;

    // Load source token account
    let src_acc = ctx.get_account(src_idx)?;
    let mut token_acc = TokenAccount::try_from_slice(&src_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;

    if token_acc.owner != auth_address && token_acc.delegate != Some(auth_address) {
        return Err(token_err(TokenError::OwnerMismatch));
    }

    if token_acc.amount < amount {
        return Err(token_err(TokenError::InsufficientBalance {
            need: amount,
            have: token_acc.amount,
        }));
    }

    let mint_address = *ctx.get_account(mint_idx)?.address;
    if token_acc.mint != mint_address {
        return Err(token_err(TokenError::MintMismatch));
    }

    token_acc.amount -= amount;

    // Update delegate if burning via delegation
    if token_acc.delegate == Some(auth_address) {
        token_acc.delegated_amount = token_acc.delegated_amount.saturating_sub(amount);
    }

    // Update mint supply
    let mint_acc = ctx.get_account(mint_idx)?;
    let mut mint = Mint::try_from_slice(&mint_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;
    mint.supply = mint.supply.saturating_sub(amount);

    // Write back
    let src_data = borsh::to_vec(&token_acc).map_err(borsh_err)?;
    ctx.get_account_mut(src_idx)?.account.data = src_data;

    let mint_data = borsh::to_vec(&mint).map_err(borsh_err)?;
    ctx.get_account_mut(mint_idx)?.account.data = mint_data;

    Ok(())
}

fn process_close_account(
    accounts: &[u8],
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(token_err(TokenError::MissingAccount));
    }
    let account_idx = accounts[0] as usize;
    let dest_idx = accounts[1] as usize;
    let auth_idx = accounts[2] as usize;

    let auth = ctx.get_account(auth_idx)?;
    if !auth.is_signer {
        return Err(token_err(TokenError::MissingSigner));
    }
    let auth_address = *auth.address;

    let acc = ctx.get_account(account_idx)?;
    let token_acc = TokenAccount::try_from_slice(&acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;

    // Must be owner or close_authority
    let close_auth = token_acc.close_authority.unwrap_or(token_acc.owner);
    if close_auth != auth_address {
        return Err(token_err(TokenError::OwnerMismatch));
    }

    if token_acc.amount > 0 {
        return Err(token_err(TokenError::CloseNonEmpty));
    }

    // Transfer lamports to destination
    let lamports = acc.account.lamports;
    {
        let dest = ctx.get_account_mut(dest_idx)?;
        dest.account.lamports = dest.account.lamports.saturating_add(lamports);
    }
    {
        let acc = ctx.get_account_mut(account_idx)?;
        acc.account.lamports = 0;
        acc.account.data.clear();
    }

    Ok(())
}

fn process_freeze_account(
    accounts: &[u8],
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(token_err(TokenError::MissingAccount));
    }
    let account_idx = accounts[0] as usize;
    let mint_idx = accounts[1] as usize;
    let auth_idx = accounts[2] as usize;

    let auth = ctx.get_account(auth_idx)?;
    if !auth.is_signer {
        return Err(token_err(TokenError::MissingSigner));
    }
    let auth_address = *auth.address;

    // Check mint has freeze authority
    let mint_acc = ctx.get_account(mint_idx)?;
    let mint = Mint::try_from_slice(&mint_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;
    if mint.freeze_authority != Some(auth_address) {
        return Err(token_err(TokenError::NoFreezeAuthority));
    }

    let acc = ctx.get_account(account_idx)?;
    let mut token_acc = TokenAccount::try_from_slice(&acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;

    let mint_address = *ctx.get_account(mint_idx)?.address;
    if token_acc.mint != mint_address {
        return Err(token_err(TokenError::MintMismatch));
    }

    token_acc.state = AccountState::Frozen;

    let acc_data = borsh::to_vec(&token_acc).map_err(borsh_err)?;
    ctx.get_account_mut(account_idx)?.account.data = acc_data;

    Ok(())
}

fn process_thaw_account(accounts: &[u8], ctx: &mut TransactionContext) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(token_err(TokenError::MissingAccount));
    }
    let account_idx = accounts[0] as usize;
    let mint_idx = accounts[1] as usize;
    let auth_idx = accounts[2] as usize;

    let auth = ctx.get_account(auth_idx)?;
    if !auth.is_signer {
        return Err(token_err(TokenError::MissingSigner));
    }
    let auth_address = *auth.address;

    let mint_acc = ctx.get_account(mint_idx)?;
    let mint = Mint::try_from_slice(&mint_acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;
    if mint.freeze_authority != Some(auth_address) {
        return Err(token_err(TokenError::NoFreezeAuthority));
    }

    let acc = ctx.get_account(account_idx)?;
    let mut token_acc = TokenAccount::try_from_slice(&acc.account.data)
        .map_err(|_| token_err(TokenError::NotInitialized))?;

    let mint_address = *ctx.get_account(mint_idx)?.address;
    if token_acc.mint != mint_address {
        return Err(token_err(TokenError::MintMismatch));
    }

    if token_acc.state != AccountState::Frozen {
        return Err(token_err(TokenError::NotInitialized));
    }

    token_acc.state = AccountState::Initialized;

    let acc_data = borsh::to_vec(&token_acc).map_err(borsh_err)?;
    ctx.get_account_mut(account_idx)?.account.data = acc_data;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn mint_and_transfer() {
        let mint_addr = hash(b"mint");
        let owner = hash(b"owner");
        let alice = hash(b"alice");
        let bob = hash(b"bob");

        // Build a tx that initializes mint, then init two token accounts, mint, and transfer
        let ix_init_mint = nusantara_token_program::initialize_mint(&mint_addr, 9, &owner, None);
        let ix_init_alice = nusantara_token_program::initialize_account(&alice, &mint_addr, &owner);
        let ix_init_bob = nusantara_token_program::initialize_account(&bob, &mint_addr, &owner);
        let ix_mint = nusantara_token_program::mint_to(&mint_addr, &alice, &owner, 1000);
        let ix_transfer = nusantara_token_program::transfer(&alice, &bob, &owner, 400);

        let msg = Message::new(
            &[
                ix_init_mint,
                ix_init_alice,
                ix_init_bob,
                ix_mint,
                ix_transfer,
            ],
            &owner,
        )
        .unwrap();

        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| (*k, Account::new(100_000, nusantara_crypto::Hash::zero())))
            .collect();

        let mut ctx = TransactionContext::new(accounts, msg.clone(), 0, 1_000_000);
        let sysvars = test_sysvars();

        for ix in &msg.instructions {
            process_token(&ix.accounts, &ix.data, &mut ctx, &sysvars).unwrap();
        }

        // Find alice and bob indices
        let alice_idx = msg.account_keys.iter().position(|k| k == &alice).unwrap();
        let bob_idx = msg.account_keys.iter().position(|k| k == &bob).unwrap();
        let mint_idx = msg
            .account_keys
            .iter()
            .position(|k| k == &mint_addr)
            .unwrap();

        let alice_acc = ctx.get_account(alice_idx).unwrap();
        let alice_token: TokenAccount = borsh::from_slice(&alice_acc.account.data).unwrap();
        assert_eq!(alice_token.amount, 600);

        let bob_acc = ctx.get_account(bob_idx).unwrap();
        let bob_token: TokenAccount = borsh::from_slice(&bob_acc.account.data).unwrap();
        assert_eq!(bob_token.amount, 400);

        let mint_acc = ctx.get_account(mint_idx).unwrap();
        let mint: Mint = borsh::from_slice(&mint_acc.account.data).unwrap();
        assert_eq!(mint.supply, 1000);
    }

    #[test]
    fn burn_tokens() {
        let mint_addr = hash(b"mint");
        let owner = hash(b"owner");
        let alice = hash(b"alice");

        let ix_init_mint = nusantara_token_program::initialize_mint(&mint_addr, 9, &owner, None);
        let ix_init_alice = nusantara_token_program::initialize_account(&alice, &mint_addr, &owner);
        let ix_mint = nusantara_token_program::mint_to(&mint_addr, &alice, &owner, 1000);
        let ix_burn = nusantara_token_program::burn(&alice, &mint_addr, &owner, 300);

        let msg = Message::new(&[ix_init_mint, ix_init_alice, ix_mint, ix_burn], &owner).unwrap();

        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| (*k, Account::new(100_000, nusantara_crypto::Hash::zero())))
            .collect();

        let mut ctx = TransactionContext::new(accounts, msg.clone(), 0, 1_000_000);
        let sysvars = test_sysvars();

        for ix in &msg.instructions {
            process_token(&ix.accounts, &ix.data, &mut ctx, &sysvars).unwrap();
        }

        let alice_idx = msg.account_keys.iter().position(|k| k == &alice).unwrap();
        let alice_acc = ctx.get_account(alice_idx).unwrap();
        let alice_token: TokenAccount = borsh::from_slice(&alice_acc.account.data).unwrap();
        assert_eq!(alice_token.amount, 700);

        let mint_idx = msg
            .account_keys
            .iter()
            .position(|k| k == &mint_addr)
            .unwrap();
        let mint_acc = ctx.get_account(mint_idx).unwrap();
        let mint: Mint = borsh::from_slice(&mint_acc.account.data).unwrap();
        assert_eq!(mint.supply, 700);
    }

    #[test]
    fn freeze_and_thaw() {
        let mint_addr = hash(b"mint");
        let owner = hash(b"owner");
        let alice = hash(b"alice");

        let ix_init_mint =
            nusantara_token_program::initialize_mint(&mint_addr, 9, &owner, Some(&owner));
        let ix_init_alice = nusantara_token_program::initialize_account(&alice, &mint_addr, &owner);
        let ix_freeze = nusantara_token_program::freeze_account(&alice, &mint_addr, &owner);

        let msg = Message::new(&[ix_init_mint, ix_init_alice, ix_freeze], &owner).unwrap();

        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| (*k, Account::new(100_000, nusantara_crypto::Hash::zero())))
            .collect();

        let mut ctx = TransactionContext::new(accounts, msg.clone(), 0, 1_000_000);
        let sysvars = test_sysvars();

        for ix in &msg.instructions {
            process_token(&ix.accounts, &ix.data, &mut ctx, &sysvars).unwrap();
        }

        let alice_idx = msg.account_keys.iter().position(|k| k == &alice).unwrap();
        let alice_acc = ctx.get_account(alice_idx).unwrap();
        let alice_token: TokenAccount = borsh::from_slice(&alice_acc.account.data).unwrap();
        assert_eq!(alice_token.state, AccountState::Frozen);

        // Now thaw
        let ix_thaw = nusantara_token_program::thaw_account(&alice, &mint_addr, &owner);
        let msg2 = Message::new(&[ix_thaw], &owner).unwrap();
        let accounts2: Vec<_> = msg2
            .account_keys
            .iter()
            .map(|k| {
                // Carry over the data from ctx for alice and mint
                let idx = msg.account_keys.iter().position(|mk| mk == k);
                if let Some(i) = idx {
                    let a = ctx.get_account(i).unwrap();
                    (*k, a.account.clone())
                } else {
                    (*k, Account::new(100_000, nusantara_crypto::Hash::zero()))
                }
            })
            .collect();

        let mut ctx2 = TransactionContext::new(accounts2, msg2.clone(), 0, 1_000_000);
        for ix in &msg2.instructions {
            process_token(&ix.accounts, &ix.data, &mut ctx2, &sysvars).unwrap();
        }

        let alice_idx2 = msg2.account_keys.iter().position(|k| k == &alice).unwrap();
        let alice_acc2 = ctx2.get_account(alice_idx2).unwrap();
        let alice_token2: TokenAccount = borsh::from_slice(&alice_acc2.account.data).unwrap();
        assert_eq!(alice_token2.state, AccountState::Initialized);
    }
}
