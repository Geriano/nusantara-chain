use borsh::BorshDeserialize;
use nusantara_core::program::LOADER_PROGRAM_ID;
use nusantara_loader_program::LoaderInstruction;
use nusantara_loader_program::state::LoaderState;
use nusantara_vm::{ProgramCache, validate_wasm};

use crate::error::RuntimeError;
use crate::sysvar_cache::SysvarCache;
use crate::transaction_context::TransactionContext;

const INITIALIZE_BUFFER_COST: u64 = 500;
const WRITE_COST: u64 = 200;
const DEPLOY_COST: u64 = 5000;
const UPGRADE_COST: u64 = 5000;
const SET_AUTHORITY_COST: u64 = 500;
const CLOSE_COST: u64 = 500;

pub fn process_loader(
    accounts: &[u8],
    data: &[u8],
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
    program_cache: &ProgramCache,
) -> Result<(), RuntimeError> {
    let instruction = LoaderInstruction::try_from_slice(data)
        .map_err(|e| RuntimeError::InvalidInstructionData(e.to_string()))?;

    match instruction {
        LoaderInstruction::InitializeBuffer => {
            ctx.consume_compute(INITIALIZE_BUFFER_COST)?;
            process_initialize_buffer(accounts, ctx)
        }
        LoaderInstruction::Write { offset, data } => {
            ctx.consume_compute(WRITE_COST + data.len() as u64)?;
            process_write(accounts, offset, &data, ctx)
        }
        LoaderInstruction::Deploy { max_data_len } => {
            ctx.consume_compute(DEPLOY_COST)?;
            let program_id = get_program_address(accounts, 1, ctx)?;
            process_deploy(accounts, max_data_len, ctx, sysvars)?;
            // Invalidate cached compiled module so the next invocation
            // recompiles from the freshly deployed bytecode.
            program_cache.invalidate(&program_id);
            Ok(())
        }
        LoaderInstruction::Upgrade => {
            ctx.consume_compute(UPGRADE_COST)?;
            let program_id = get_program_address(accounts, 0, ctx)?;
            process_upgrade(accounts, ctx)?;
            // Invalidate the cached module so the upgraded bytecode takes
            // effect immediately.
            program_cache.invalidate(&program_id);
            Ok(())
        }
        LoaderInstruction::SetAuthority { new_authority } => {
            ctx.consume_compute(SET_AUTHORITY_COST)?;
            process_set_authority(accounts, new_authority, ctx)
        }
        LoaderInstruction::Close => {
            ctx.consume_compute(CLOSE_COST)?;
            process_close(accounts, ctx)
        }
    }
}

/// Read the address of the account at the given position in the accounts
/// slice. Used to obtain the program_id for cache invalidation before
/// the processor potentially moves data around.
fn get_program_address(
    accounts: &[u8],
    position: usize,
    ctx: &TransactionContext,
) -> Result<nusantara_crypto::Hash, RuntimeError> {
    if position >= accounts.len() {
        return Err(RuntimeError::InvalidInstructionData(format!(
            "account index {position} out of bounds for accounts list of length {}",
            accounts.len()
        )));
    }
    let idx = accounts[position] as usize;
    let acc = ctx.get_account(idx)?;
    Ok(*acc.address)
}

fn process_initialize_buffer(
    accounts: &[u8],
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.len() < 2 {
        return Err(RuntimeError::InvalidInstructionData(
            "InitializeBuffer requires 2 accounts".to_string(),
        ));
    }
    let buffer_idx = accounts[0] as usize;
    let authority_idx = accounts[1] as usize;

    // Verify authority is signer
    let authority_address = {
        let auth = ctx.get_account(authority_idx)?;
        if !auth.is_signer {
            return Err(RuntimeError::AccountNotSigner(authority_idx));
        }
        *auth.address
    };

    // Verify buffer is signer (new account)
    {
        let buffer = ctx.get_account(buffer_idx)?;
        if !buffer.is_signer {
            return Err(RuntimeError::AccountNotSigner(buffer_idx));
        }
    }

    // Write buffer state
    let state = LoaderState::Buffer {
        authority: Some(authority_address),
    };
    let state_bytes =
        borsh::to_vec(&state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    let buffer = ctx.get_account_mut(buffer_idx)?;
    buffer.account.owner = *LOADER_PROGRAM_ID;
    buffer.account.data = state_bytes;

    Ok(())
}

fn process_write(
    accounts: &[u8],
    offset: u32,
    data: &[u8],
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.len() < 2 {
        return Err(RuntimeError::InvalidInstructionData(
            "Write requires 2 accounts".to_string(),
        ));
    }
    let buffer_idx = accounts[0] as usize;
    let authority_idx = accounts[1] as usize;

    // Verify authority is signer
    let authority_address = {
        let auth = ctx.get_account(authority_idx)?;
        if !auth.is_signer {
            return Err(RuntimeError::AccountNotSigner(authority_idx));
        }
        *auth.address
    };

    // Verify buffer state and authority match
    {
        let buffer = ctx.get_account(buffer_idx)?;
        if buffer.account.owner != *LOADER_PROGRAM_ID {
            return Err(RuntimeError::AccountOwnerMismatch);
        }
        let state = LoaderState::from_account_data(&buffer.account.data)
            .map_err(RuntimeError::InvalidAccountData)?;
        match state {
            LoaderState::Buffer { authority } => {
                if authority != Some(authority_address) {
                    return Err(RuntimeError::AccountNotSigner(authority_idx));
                }
            }
            _ => {
                return Err(RuntimeError::InvalidAccountData(
                    "account is not a buffer".to_string(),
                ));
            }
        }
    }

    // Write data at offset. The buffer data layout is:
    // [LoaderState::Buffer header] ++ [raw bytecode bytes]
    // We need to ensure the data vec is large enough
    let buffer = ctx.get_account_mut(buffer_idx)?;
    let header_len = {
        let state = LoaderState::Buffer {
            authority: Some(authority_address),
        };
        borsh::to_vec(&state)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?
            .len()
    };

    let write_start = header_len + offset as usize;
    let write_end = write_start + data.len();

    // Extend data if needed
    if write_end > buffer.account.data.len() {
        buffer.account.data.resize(write_end, 0);
    }

    buffer.account.data[write_start..write_end].copy_from_slice(data);

    Ok(())
}

fn process_deploy(
    accounts: &[u8],
    max_data_len: u64,
    ctx: &mut TransactionContext,
    sysvars: &SysvarCache,
) -> Result<(), RuntimeError> {
    if accounts.len() < 5 {
        return Err(RuntimeError::InvalidInstructionData(
            "Deploy requires 5 accounts".to_string(),
        ));
    }
    let payer_idx = accounts[0] as usize;
    let program_idx = accounts[1] as usize;
    let program_data_idx = accounts[2] as usize;
    let buffer_idx = accounts[3] as usize;
    let authority_idx = accounts[4] as usize;

    // Verify payer is signer
    {
        let payer = ctx.get_account(payer_idx)?;
        if !payer.is_signer {
            return Err(RuntimeError::AccountNotSigner(payer_idx));
        }
    }

    // Verify authority is signer
    let authority_address = {
        let auth = ctx.get_account(authority_idx)?;
        if !auth.is_signer {
            return Err(RuntimeError::AccountNotSigner(authority_idx));
        }
        *auth.address
    };

    // Extract bytecode from buffer
    let (bytecode, buffer_lamports) = {
        let buffer = ctx.get_account(buffer_idx)?;
        if buffer.account.owner != *LOADER_PROGRAM_ID {
            return Err(RuntimeError::AccountOwnerMismatch);
        }
        let state = LoaderState::from_account_data(&buffer.account.data)
            .map_err(RuntimeError::InvalidAccountData)?;
        match &state {
            LoaderState::Buffer { authority } => {
                if *authority != Some(authority_address) {
                    return Err(RuntimeError::AccountNotSigner(authority_idx));
                }
            }
            _ => {
                return Err(RuntimeError::InvalidAccountData(
                    "account is not a buffer".to_string(),
                ));
            }
        }
        // Extract bytecode (everything after the header)
        let header_bytes =
            borsh::to_vec(&state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;
        let bytecode = buffer.account.data[header_bytes.len()..].to_vec();
        (bytecode, buffer.account.lamports)
    };

    // Validate WASM bytecode
    validate_wasm(&bytecode).map_err(|e| RuntimeError::WasmError(e.to_string()))?;

    // Get program_data_address for the Program account to point to
    let program_data_address = {
        let pd = ctx.get_account(program_data_idx)?;
        *pd.address
    };

    // Create Program account (executable = true, owner = LOADER_PROGRAM_ID)
    let program_state = LoaderState::Program {
        program_data_address,
    };
    let program_state_bytes = borsh::to_vec(&program_state)
        .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    {
        let program = ctx.get_account_mut(program_idx)?;
        program.account.owner = *LOADER_PROGRAM_ID;
        program.account.executable = true;
        program.account.data = program_state_bytes;
    }

    // Create ProgramData account (header + bytecode, with max_data_len padding)
    let pd_header = LoaderState::ProgramData {
        slot: ctx.slot,
        upgrade_authority: Some(authority_address),
        bytecode_len: bytecode.len() as u64,
    };
    let pd_header_bytes =
        borsh::to_vec(&pd_header).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    let bytecode_space = max_data_len.max(bytecode.len() as u64) as usize;
    let total_pd_size = pd_header_bytes.len() + bytecode_space;

    // Calculate rent for ProgramData
    let pd_rent = sysvars.rent().minimum_balance(total_pd_size);

    // Deduct rent from payer
    {
        let payer = ctx.get_account_mut(payer_idx)?;
        if payer.account.lamports < pd_rent {
            return Err(RuntimeError::InsufficientFunds {
                needed: pd_rent,
                available: payer.account.lamports,
            });
        }
        payer.account.lamports -= pd_rent;
    }

    // Write ProgramData account
    {
        let pd = ctx.get_account_mut(program_data_idx)?;
        pd.account.owner = *LOADER_PROGRAM_ID;
        pd.account.lamports = pd_rent;
        let mut pd_data = pd_header_bytes;
        pd_data.extend_from_slice(&bytecode);
        pd_data.resize(pd_data.len() + bytecode_space - bytecode.len(), 0);
        pd.account.data = pd_data;
    }

    // Close buffer: transfer lamports to payer, clear data
    {
        let buffer = ctx.get_account_mut(buffer_idx)?;
        buffer.account.data.clear();
        buffer.account.lamports = 0;
    }
    {
        let payer = ctx.get_account_mut(payer_idx)?;
        payer.account.lamports += buffer_lamports;
    }

    Ok(())
}

fn process_upgrade(accounts: &[u8], ctx: &mut TransactionContext) -> Result<(), RuntimeError> {
    if accounts.len() < 4 {
        return Err(RuntimeError::InvalidInstructionData(
            "Upgrade requires 4 accounts".to_string(),
        ));
    }
    let program_idx = accounts[0] as usize;
    let program_data_idx = accounts[1] as usize;
    let buffer_idx = accounts[2] as usize;
    let authority_idx = accounts[3] as usize;

    // Verify authority is signer
    let authority_address = {
        let auth = ctx.get_account(authority_idx)?;
        if !auth.is_signer {
            return Err(RuntimeError::AccountNotSigner(authority_idx));
        }
        *auth.address
    };

    // Verify program account points to program_data
    let program_data_address = {
        let pd = ctx.get_account(program_data_idx)?;
        *pd.address
    };
    {
        let program = ctx.get_account(program_idx)?;
        let state = LoaderState::from_account_data(&program.account.data)
            .map_err(RuntimeError::InvalidAccountData)?;
        match state {
            LoaderState::Program {
                program_data_address: pda,
            } => {
                if pda != program_data_address {
                    return Err(RuntimeError::InvalidAccountData(
                        "program data address mismatch".to_string(),
                    ));
                }
            }
            _ => {
                return Err(RuntimeError::InvalidAccountData(
                    "not a program account".to_string(),
                ));
            }
        }
    }

    // Verify ProgramData authority matches
    let old_pd_header_len = {
        let pd = ctx.get_account(program_data_idx)?;
        let state = LoaderState::from_account_data(&pd.account.data)
            .map_err(RuntimeError::InvalidAccountData)?;
        match &state {
            LoaderState::ProgramData {
                upgrade_authority, ..
            } => {
                if *upgrade_authority != Some(authority_address) {
                    return Err(RuntimeError::AccountNotSigner(authority_idx));
                }
                if upgrade_authority.is_none() {
                    return Err(RuntimeError::ProgramError {
                        program: "loader".to_string(),
                        message: "program is immutable".to_string(),
                    });
                }
            }
            _ => {
                return Err(RuntimeError::InvalidAccountData(
                    "not a program data account".to_string(),
                ));
            }
        }
        borsh::to_vec(&state)
            .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?
            .len()
    };

    // Extract new bytecode from buffer
    let (new_bytecode, buffer_lamports) = {
        let buffer = ctx.get_account(buffer_idx)?;
        let state = LoaderState::from_account_data(&buffer.account.data)
            .map_err(RuntimeError::InvalidAccountData)?;
        match &state {
            LoaderState::Buffer { authority } => {
                if *authority != Some(authority_address) {
                    return Err(RuntimeError::AccountNotSigner(authority_idx));
                }
            }
            _ => {
                return Err(RuntimeError::InvalidAccountData(
                    "not a buffer account".to_string(),
                ));
            }
        }
        let header_bytes =
            borsh::to_vec(&state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;
        let bytecode = buffer.account.data[header_bytes.len()..].to_vec();
        (bytecode, buffer.account.lamports)
    };

    // Validate new WASM bytecode
    validate_wasm(&new_bytecode).map_err(|e| RuntimeError::WasmError(e.to_string()))?;

    // Update ProgramData with new bytecode
    let new_header = LoaderState::ProgramData {
        slot: ctx.slot,
        upgrade_authority: Some(authority_address),
        bytecode_len: new_bytecode.len() as u64,
    };
    let new_header_bytes =
        borsh::to_vec(&new_header).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    {
        let pd = ctx.get_account_mut(program_data_idx)?;
        let old_bytecode_space = pd.account.data.len() - old_pd_header_len;
        if new_bytecode.len() > old_bytecode_space {
            return Err(RuntimeError::AccountDataTooLarge);
        }
        let mut new_data = new_header_bytes;
        new_data.extend_from_slice(&new_bytecode);
        new_data.resize(new_data.len() + old_bytecode_space - new_bytecode.len(), 0);
        pd.account.data = new_data;
    }

    // Close buffer: return lamports to authority
    {
        let buffer = ctx.get_account_mut(buffer_idx)?;
        buffer.account.data.clear();
        buffer.account.lamports = 0;
    }
    {
        let auth = ctx.get_account_mut(authority_idx)?;
        auth.account.lamports += buffer_lamports;
    }

    Ok(())
}

fn process_set_authority(
    accounts: &[u8],
    new_authority: Option<nusantara_crypto::Hash>,
    ctx: &mut TransactionContext,
) -> Result<(), RuntimeError> {
    if accounts.len() < 2 {
        return Err(RuntimeError::InvalidInstructionData(
            "SetAuthority requires 2 accounts".to_string(),
        ));
    }
    let account_idx = accounts[0] as usize;
    let current_authority_idx = accounts[1] as usize;

    // Verify current authority is signer
    let current_authority_address = {
        let auth = ctx.get_account(current_authority_idx)?;
        if !auth.is_signer {
            return Err(RuntimeError::AccountNotSigner(current_authority_idx));
        }
        *auth.address
    };

    // Read current state, verify authority, write new state
    let current_state = {
        let acc = ctx.get_account(account_idx)?;
        LoaderState::from_account_data(&acc.account.data)
            .map_err(RuntimeError::InvalidAccountData)?
    };

    let new_state = match &current_state {
        LoaderState::Buffer { authority } => {
            if *authority != Some(current_authority_address) {
                return Err(RuntimeError::AccountNotSigner(current_authority_idx));
            }
            LoaderState::Buffer {
                authority: new_authority,
            }
        }
        LoaderState::ProgramData {
            slot,
            upgrade_authority,
            bytecode_len,
        } => {
            if *upgrade_authority != Some(current_authority_address) {
                return Err(RuntimeError::AccountNotSigner(current_authority_idx));
            }
            LoaderState::ProgramData {
                slot: *slot,
                upgrade_authority: new_authority,
                bytecode_len: *bytecode_len,
            }
        }
        _ => {
            return Err(RuntimeError::InvalidAccountData(
                "cannot set authority on this account type".to_string(),
            ));
        }
    };

    let new_state_bytes =
        borsh::to_vec(&new_state).map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;

    // For ProgramData, preserve the bytecode after the header
    let acc = ctx.get_account_mut(account_idx)?;
    match &current_state {
        LoaderState::ProgramData { .. } => {
            // Keep bytecode intact, only update the header portion
            let old_header_bytes = borsh::to_vec(&current_state)
                .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;
            let bytecode_start = old_header_bytes.len();
            let bytecode = acc.account.data[bytecode_start..].to_vec();
            let mut new_data = new_state_bytes;
            new_data.extend_from_slice(&bytecode);
            acc.account.data = new_data;
        }
        _ => {
            acc.account.data = new_state_bytes;
        }
    }

    Ok(())
}

fn process_close(accounts: &[u8], ctx: &mut TransactionContext) -> Result<(), RuntimeError> {
    if accounts.len() < 3 {
        return Err(RuntimeError::InvalidInstructionData(
            "Close requires 3 accounts".to_string(),
        ));
    }
    let close_idx = accounts[0] as usize;
    let recipient_idx = accounts[1] as usize;
    let authority_idx = accounts[2] as usize;

    // Verify authority is signer
    let authority_address = {
        let auth = ctx.get_account(authority_idx)?;
        if !auth.is_signer {
            return Err(RuntimeError::AccountNotSigner(authority_idx));
        }
        *auth.address
    };

    // Verify the account's authority matches
    let lamports_to_transfer = {
        let acc = ctx.get_account(close_idx)?;
        let state = LoaderState::from_account_data(&acc.account.data)
            .map_err(RuntimeError::InvalidAccountData)?;
        match state.authority() {
            Some(auth) if *auth == authority_address => {}
            _ => {
                return Err(RuntimeError::AccountNotSigner(authority_idx));
            }
        }
        acc.account.lamports
    };

    // Transfer lamports to recipient
    {
        let recipient = ctx.get_account_mut(recipient_idx)?;
        recipient.account.lamports = recipient
            .account
            .lamports
            .checked_add(lamports_to_transfer)
            .ok_or(RuntimeError::LamportsOverflow)?;
    }

    // Clear the closed account
    {
        let acc = ctx.get_account_mut(close_idx)?;
        acc.account.lamports = 0;
        acc.account.data.clear();
    }

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

    fn test_cache() -> ProgramCache {
        ProgramCache::new(16)
    }

    #[test]
    fn initialize_buffer_success() {
        let buffer = hash(b"buffer");
        let authority = hash(b"authority");
        let ix = nusantara_loader_program::initialize_buffer(&buffer, &authority);
        let msg = Message::new(&[ix], &authority).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| (*k, Account::new(100_000, nusantara_crypto::Hash::zero())))
            .collect();
        let compiled = msg.instructions[0].accounts.clone();
        let data = msg.instructions[0].data.clone();
        let mut ctx = TransactionContext::new(accounts, msg, 0, 100_000);
        let sysvars = test_sysvars();
        let cache = test_cache();
        process_loader(&compiled, &data, &mut ctx, &sysvars, &cache).unwrap();

        // Find buffer account and verify state
        let buffer_idx = ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &buffer)
            .unwrap();
        let acc = ctx.get_account(buffer_idx).unwrap();
        assert_eq!(acc.account.owner, *LOADER_PROGRAM_ID);

        let state = LoaderState::from_account_data(&acc.account.data).unwrap();
        assert!(state.is_buffer());
        assert_eq!(state.authority(), Some(&authority));
    }

    #[test]
    fn write_to_buffer() {
        let buffer = hash(b"buffer");
        let authority = hash(b"authority");

        // First initialize
        let init_ix = nusantara_loader_program::initialize_buffer(&buffer, &authority);
        let write_ix =
            nusantara_loader_program::write(&buffer, &authority, 0, vec![0x00, 0x61, 0x73, 0x6d]);
        let msg = Message::new(&[init_ix, write_ix], &authority).unwrap();
        let accounts: Vec<_> = msg
            .account_keys
            .iter()
            .map(|k| (*k, Account::new(100_000, nusantara_crypto::Hash::zero())))
            .collect();

        let sysvars = test_sysvars();
        let cache = test_cache();
        let mut ctx = TransactionContext::new(accounts, msg.clone(), 0, 200_000);

        // Execute both instructions
        for ix in &msg.instructions {
            let program_id = &msg.account_keys[ix.program_id_index as usize];
            assert_eq!(*program_id, *LOADER_PROGRAM_ID);
            process_loader(&ix.accounts, &ix.data, &mut ctx, &sysvars, &cache).unwrap();
        }

        // Verify buffer has the written data
        let buffer_idx = ctx
            .message()
            .account_keys
            .iter()
            .position(|k| k == &buffer)
            .unwrap();
        let acc = ctx.get_account(buffer_idx).unwrap();
        // Data should contain header + written bytes
        assert!(acc.account.data.len() > 4);
    }
}
