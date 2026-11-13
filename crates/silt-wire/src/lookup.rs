//! Lookup fast path (PLAN D9, D10): a fixed little-endian request/response
//! layout for tip-state reads and trie proofs, written by hand.
//!
//! No postcard, serde or varints here: every field sits at a fixed offset,
//! so [`RequestRef::decode`] and [`ResponseRef::decode`] only check lengths
//! and borrow slices — they never allocate. Frames keep the crate's `u32`
//! LE length prefix; the bodies below exclude it.
//!
//! ```text
//! request  (40 B header): u8 op | u8 flags | u16 count | u32 id | [32] pin_hash | count × item
//!   items: Account [20] addr | Storage [20] addr,[32] slot | Code [32] code_hash
//!          Proof   [32] hashed_address, u16 slot_count, [2] reserved, slot_count × [32] hashed_slot
//! response (56 B header): u8 op | u8 flags | u16 count | u32 id
//!                         | u8 status | [3] reserved | u32 server_us
//!                         | u64 read_number | [32] read_hash | count × result
//!   results: u8 item_status, then
//!     Account: u64 nonce, [32] balance, [32] code_hash [+ u32 code_len, code iff INLINE_CODE]
//!     Storage: [32] value
//!     Code:    u32 len, bytes
//!   Proof (count = storage tries; no per-item results):
//!     u32 account_nodes, account_nodes × node,
//!     count × ([32] hashed_address, [32] storage_root, u32 nodes, nodes × node)
//!     node: u8 path_len (nibbles, ≤ 64), ⌈path_len/2⌉ packed path, u32 rlp_len (≥ 1), rlp
//! ```
//!
//! Integers are little-endian; 32-byte words are opaque arrays exactly as in
//! [`crate::BlockDiff`] (U256 values big-endian).

use std::collections::VecDeque;
use std::io::IoSlice;
use std::sync::Mutex;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Notify;

use crate::{Addr, KECCAK_EMPTY, Word};

/// All-zero pin: "read at the server's latest".
pub const LATEST: Word = [0; 32];

/// Request flag: inline the bytecode in Account results.
pub const FLAG_INLINE_CODE: u8 = 0x01;
const KNOWN_FLAGS: u8 = FLAG_INLINE_CODE;

pub const REQUEST_HEADER: usize = 40;
pub const RESPONSE_HEADER: usize = 56;
/// `op` + `flags` + `count` + `id`: the prefix both directions share.
pub const PREFIX: usize = 8;

pub const ACCOUNT_ITEM: usize = 20;
pub const STORAGE_ITEM: usize = 52;
pub const CODE_ITEM: usize = 32;
/// Fixed part of a Proof item: `hashed_address, slot_count, reserved`;
/// `slot_count × 32` bytes of hashed slots follow.
pub const PROOF_TARGET: usize = 32 + 2 + 2;

/// `item_status, nonce, balance, code_hash` (+ `u32 code_len, code` inline).
pub const ACCOUNT_RESULT: usize = 1 + 8 + 32 + 32;
pub const STORAGE_RESULT: usize = 1 + 32;
/// `item_status, len` (+ `len` bytes).
pub const CODE_RESULT_HEADER: usize = 1 + 4;

/// Items per frame for Account (without inline code) and Storage.
pub const MAX_ITEMS: usize = 1024;
/// Items per frame for Code and Account + `INLINE_CODE`: 64 × a 256 KiB
/// contract still fits [`MAX_RESPONSE_BYTES`] (10× EIP-170 headroom).
pub const MAX_CODE_ITEMS: usize = 64;
/// Hashed slots across all targets of one Proof frame (D10).
pub const MAX_PROOF_SLOTS: usize = 4096;
/// A trie path is at most 64 nibbles (a full 32-byte key).
pub const MAX_PATH_NIBBLES: usize = 64;
/// Largest valid request body: a full Proof frame (`MAX_ITEMS` targets
/// carrying `MAX_PROOF_SLOTS` slots), larger than a full Storage frame.
pub const MAX_REQUEST_BYTES: usize =
    REQUEST_HEADER + MAX_ITEMS * PROOF_TARGET + MAX_PROOF_SLOTS * 32;
/// Largest response body either side accepts or produces.
pub const MAX_RESPONSE_BYTES: usize = 16 << 20;
/// Requests a client may have decoded-but-unanswered per connection; the
/// server answers [`Status::Busy`] beyond this.
pub const MAX_INFLIGHT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Op {
    Account = 1,
    Storage = 2,
    Code = 3,
    /// Multiproof for pre-hashed targets (D10).
    Proof = 4,
}

impl Op {
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Account),
            2 => Some(Self::Storage),
            3 => Some(Self::Code),
            4 => Some(Self::Proof),
            _ => None,
        }
    }

    /// Request item size; for `Proof`, the fixed part ([`PROOF_TARGET`]).
    pub const fn item_len(self) -> usize {
        match self {
            Self::Account => ACCOUNT_ITEM,
            Self::Storage => STORAGE_ITEM,
            Self::Code => CODE_ITEM,
            Self::Proof => PROOF_TARGET,
        }
    }

    /// Item cap for this op with these flags (`Proof`: account targets;
    /// its slots are capped by [`MAX_PROOF_SLOTS`]).
    pub const fn max_items(self, flags: u8) -> usize {
        match self {
            Self::Account if flags & FLAG_INLINE_CODE != 0 => MAX_CODE_ITEMS,
            Self::Account | Self::Storage | Self::Proof => MAX_ITEMS,
            Self::Code => MAX_CODE_ITEMS,
        }
    }
}

/// Frame status. Any non-`Ok` frame has `count = 0`, zeroed
/// `read_number`/`read_hash` and no results.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    /// Unknown, non-canonical, pruned or not-yet-readable pin.
    UnknownBlock = 1,
    BadRequest = 2,
    Internal = 3,
    /// Not attempted (in-flight cap or server overload): retry or fall back.
    Busy = 4,
    /// The answer would exceed [`MAX_RESPONSE_BYTES`]: split the request.
    TooLarge = 5,
}

impl Status {
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Ok),
            1 => Some(Self::UnknownBlock),
            2 => Some(Self::BadRequest),
            3 => Some(Self::Internal),
            4 => Some(Self::Busy),
            5 => Some(Self::TooLarge),
            _ => None,
        }
    }
}

/// Per-item status. `Ok` and `Absent` both mean the payload is valid;
/// `Error` means this key's read failed and its payload is zeroed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ItemStatus {
    /// Account exists / code found / storage read (missing slot = zero).
    Ok = 0,
    /// No such account (zeroed fields, `KECCAK_EMPTY`) or no such code.
    /// Never sent for Storage.
    Absent = 1,
    Error = 2,
}

impl ItemStatus {
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Ok),
            1 => Some(Self::Absent),
            2 => Some(Self::Error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// Shorter than its header, or its results run past the end.
    Truncated,
    /// Bytes left over after the last item/result.
    TrailingBytes,
    BadOp(u8),
    BadFlags(u8),
    BadStatus(u8),
    BadItemStatus(u8),
    /// Nonzero reserved bytes.
    BadReserved,
    TooManyItems(u16),
    /// Non-`Ok` response that still carries items.
    ErrorWithItems,
    /// Length prefix outside `[min, max]`.
    BadLength(u32),
    /// More hashed slots than [`MAX_PROOF_SLOTS`] in one Proof frame.
    TooManySlots(usize),
    /// A proof node path longer than 64 nibbles, or an odd path whose
    /// padding nibble is not zero.
    BadPath,
    /// A proof node with an empty RLP encoding.
    EmptyNode,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated lookup frame"),
            Self::TrailingBytes => write!(f, "trailing bytes after lookup frame"),
            Self::BadOp(op) => write!(f, "unknown lookup op {op}"),
            Self::BadFlags(flags) => write!(f, "invalid lookup flags {flags:#04x}"),
            Self::BadStatus(s) => write!(f, "unknown lookup status {s}"),
            Self::BadItemStatus(s) => write!(f, "unknown lookup item status {s}"),
            Self::BadReserved => write!(f, "nonzero reserved bytes"),
            Self::TooManyItems(n) => write!(f, "{n} items exceed the frame cap"),
            Self::ErrorWithItems => write!(f, "non-Ok lookup response carries items"),
            Self::BadLength(len) => write!(f, "lookup frame length {len} out of bounds"),
            Self::TooManySlots(n) => write!(f, "{n} proof slots exceed the frame cap"),
            Self::BadPath => write!(f, "invalid proof node path"),
            Self::EmptyNode => write!(f, "empty proof node"),
        }
    }
}

impl std::error::Error for DecodeError {}

// --- little-endian field access (bounds are checked by the callers) ---

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

fn word_at(b: &[u8], at: usize) -> &Word {
    b[at..at + 32].try_into().expect("32 bytes")
}

fn addr_at(b: &[u8], at: usize) -> &Addr {
    b[at..at + 20].try_into().expect("20 bytes")
}

/// `(op, flags, id)` of a body that may otherwise be invalid, so a server can echo
/// them in a `BadRequest`. `None` below the shared 8-byte prefix.
pub fn peek_prefix(body: &[u8]) -> Option<(u8, u8, u32)> {
    (body.len() >= PREFIX).then(|| (body[0], body[1], u32_at(body, 4)))
}

/// Validates a length prefix against `[PREFIX, max]`.
pub fn check_len(len: u32, max: usize) -> Result<usize, DecodeError> {
    let n = len as usize;
    if n < PREFIX || n > max {
        return Err(DecodeError::BadLength(len));
    }
    Ok(n)
}

// ------------------------------------------------------------------ request

/// One request item, borrowed from the frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Item<'a> {
    Account(&'a Addr),
    Storage(&'a Addr, &'a Word),
    Code(&'a Word),
    Proof(ProofTarget<'a>),
}

/// One Proof target: a hashed address and the hashed slots to prove under
/// it (none = account trie only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProofTarget<'a> {
    pub hashed_address: &'a Word,
    /// `slot_count × 32` bytes.
    slots: &'a [u8],
}

impl<'a> ProofTarget<'a> {
    pub fn slot_count(&self) -> usize {
        self.slots.len() / 32
    }

    pub fn slots(&self) -> impl ExactSizeIterator<Item = &'a Word> + 'a {
        self.slots.as_chunks::<32>().0.iter()
    }
}

/// A validated request, borrowing the frame body. Decoding allocates
/// nothing.
#[derive(Clone, Copy, Debug)]
pub struct RequestRef<'a> {
    pub op: Op,
    pub flags: u8,
    pub id: u32,
    pub pin: &'a Word,
    count: u16,
    items: &'a [u8],
}

impl<'a> RequestRef<'a> {
    /// Validates op, flags, count cap, Proof slot caps and reserved bytes,
    /// and the exact length.
    pub fn decode(body: &'a [u8]) -> Result<Self, DecodeError> {
        if body.len() < REQUEST_HEADER {
            return Err(DecodeError::Truncated);
        }
        let op = Op::from_u8(body[0]).ok_or(DecodeError::BadOp(body[0]))?;
        let flags = body[1];
        if flags & !KNOWN_FLAGS != 0 || (flags & FLAG_INLINE_CODE != 0 && op != Op::Account) {
            return Err(DecodeError::BadFlags(flags));
        }
        let count = u16_at(body, 2);
        if count as usize > op.max_items(flags) {
            return Err(DecodeError::TooManyItems(count));
        }
        let items = &body[REQUEST_HEADER..];
        let want = if op == Op::Proof {
            proof_items_len(items, count)?
        } else {
            count as usize * op.item_len()
        };
        match items.len().cmp(&want) {
            std::cmp::Ordering::Less => return Err(DecodeError::Truncated),
            std::cmp::Ordering::Greater => return Err(DecodeError::TrailingBytes),
            std::cmp::Ordering::Equal => {}
        }
        Ok(Self {
            op,
            flags,
            id: u32_at(body, 4),
            pin: word_at(body, 8),
            count,
            items,
        })
    }

    pub fn count(&self) -> usize {
        self.count as usize
    }

    pub fn inline_code(&self) -> bool {
        self.flags & FLAG_INLINE_CODE != 0
    }

    pub fn is_latest(&self) -> bool {
        *self.pin == LATEST
    }

    /// Items in request order.
    pub fn items(&self) -> Items<'a> {
        Items {
            op: self.op,
            left: self.count,
            rest: self.items,
        }
    }
}

/// Bytes `count` Proof items occupy at the start of `items`, validating
/// each item's reserved bytes and the frame's slot cap. Stops at the first
/// item that runs past the end (`decode` then reports `Truncated`).
fn proof_items_len(items: &[u8], count: u16) -> Result<usize, DecodeError> {
    let (mut at, mut slots) = (0usize, 0usize);
    for _ in 0..count {
        let Some(fixed) = items.get(at..at + PROOF_TARGET) else {
            return Err(DecodeError::Truncated);
        };
        if fixed[34..36] != [0, 0] {
            return Err(DecodeError::BadReserved);
        }
        let n = u16_at(fixed, 32) as usize;
        slots += n;
        if slots > MAX_PROOF_SLOTS {
            return Err(DecodeError::TooManySlots(slots));
        }
        at += PROOF_TARGET + n * 32;
    }
    Ok(at)
}

/// Iterator over a decoded request's items.
#[derive(Clone, Debug)]
pub struct Items<'a> {
    op: Op,
    left: u16,
    rest: &'a [u8],
}

impl<'a> Iterator for Items<'a> {
    type Item = Item<'a>;

    fn next(&mut self) -> Option<Item<'a>> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let b = self.rest;
        // Lengths were validated by `RequestRef::decode`.
        let (item, used) = match self.op {
            Op::Account => (Item::Account(addr_at(b, 0)), ACCOUNT_ITEM),
            Op::Storage => (Item::Storage(addr_at(b, 0), word_at(b, 20)), STORAGE_ITEM),
            Op::Code => (Item::Code(word_at(b, 0)), CODE_ITEM),
            Op::Proof => {
                let end = PROOF_TARGET + u16_at(b, 32) as usize * 32;
                let target = ProofTarget {
                    hashed_address: word_at(b, 0),
                    slots: &b[PROOF_TARGET..end],
                };
                (Item::Proof(target), end)
            }
        };
        self.rest = &b[used..];
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left as usize, Some(self.left as usize))
    }
}

impl ExactSizeIterator for Items<'_> {}

/// Builds one request frame, length prefix included, ready to write.
#[derive(Clone, Debug)]
pub struct RequestBuilder {
    op: Op,
    flags: u8,
    count: u16,
    /// Proof frames: hashed slots so far.
    slots: usize,
    buf: Vec<u8>,
}

impl RequestBuilder {
    pub fn new(op: Op, flags: u8, id: u32, pin: &Word) -> Self {
        Self::with_capacity(op, flags, id, pin, 1)
    }

    pub fn with_capacity(op: Op, flags: u8, id: u32, pin: &Word, items: usize) -> Self {
        let mut buf = Vec::with_capacity(4 + REQUEST_HEADER + items * op.item_len());
        buf.extend_from_slice(&[0; 4]); // length, patched in finish()
        buf.extend_from_slice(&[op as u8, flags, 0, 0]);
        buf.extend_from_slice(&id.to_le_bytes());
        buf.extend_from_slice(pin);
        Self {
            op,
            flags,
            count: 0,
            slots: 0,
            buf,
        }
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// True once the frame holds its op's item cap.
    pub fn is_full(&self) -> bool {
        self.count as usize >= self.op.max_items(self.flags)
    }

    /// Whether one more Proof target with `slots` hashed slots fits (both
    /// the target cap and [`MAX_PROOF_SLOTS`]).
    pub fn fits_proof_target(&self, slots: usize) -> bool {
        !self.is_full() && self.slots + slots <= MAX_PROOF_SLOTS
    }

    fn bump(&mut self, op: Op) {
        assert_eq!(self.op, op, "item does not match the frame's op");
        assert!(!self.is_full(), "lookup frame already holds its item cap");
        self.count += 1;
    }

    pub fn push_account(&mut self, addr: &Addr) -> &mut Self {
        self.bump(Op::Account);
        self.buf.extend_from_slice(addr);
        self
    }

    pub fn push_storage(&mut self, addr: &Addr, slot: &Word) -> &mut Self {
        self.bump(Op::Storage);
        self.buf.extend_from_slice(addr);
        self.buf.extend_from_slice(slot);
        self
    }

    pub fn push_code(&mut self, code_hash: &Word) -> &mut Self {
        self.bump(Op::Code);
        self.buf.extend_from_slice(code_hash);
        self
    }

    /// One Proof target. Panics past the caps (check
    /// [`Self::fits_proof_target`]).
    pub fn push_proof_target(&mut self, hashed_address: &Word, hashed_slots: &[Word]) -> &mut Self {
        self.push_proof_raw(hashed_address, hashed_slots.as_flattened())
    }

    fn push_proof_raw(&mut self, hashed_address: &Word, slots: &[u8]) -> &mut Self {
        let n = slots.len() / 32;
        assert!(
            self.slots + n <= MAX_PROOF_SLOTS,
            "proof frame already holds its slot cap"
        );
        self.bump(Op::Proof);
        self.slots += n;
        self.buf.extend_from_slice(hashed_address);
        self.buf.extend_from_slice(&(n as u16).to_le_bytes());
        self.buf.extend_from_slice(&[0, 0]);
        self.buf.extend_from_slice(slots);
        self
    }

    pub fn push(&mut self, item: Item<'_>) -> &mut Self {
        match item {
            Item::Account(addr) => self.push_account(addr),
            Item::Storage(addr, slot) => self.push_storage(addr, slot),
            Item::Code(hash) => self.push_code(hash),
            Item::Proof(t) => self.push_proof_raw(t.hashed_address, t.slots),
        }
    }

    /// The whole frame: `u32` LE body length, then the body.
    pub fn finish(mut self) -> Vec<u8> {
        let body = (self.buf.len() - 4) as u32;
        self.buf[0..4].copy_from_slice(&body.to_le_bytes());
        self.buf[6..8].copy_from_slice(&self.count.to_le_bytes());
        self.buf
    }
}

// ----------------------------------------------------------------- response

/// Builds one `Ok` response frame (length prefix included); `count`, the
/// length and `server_us` are patched in [`ResponseBuilder::finish`] so
/// the timing covers the encoding too.
#[derive(Debug)]
pub struct ResponseBuilder {
    op: Op,
    inline_code: bool,
    count: u16,
    buf: Vec<u8>,
}

impl ResponseBuilder {
    /// `capacity`: expected item count (pre-sizes the buffer).
    pub fn new(
        op: Op,
        flags: u8,
        id: u32,
        read_number: u64,
        read_hash: &Word,
        capacity: usize,
    ) -> Self {
        let per_item = match op {
            Op::Account => ACCOUNT_RESULT,
            Op::Storage => STORAGE_RESULT,
            Op::Code => CODE_RESULT_HEADER,
            Op::Proof => panic!("Proof answers are built with ProofBuilder"),
        };
        let mut buf = Vec::with_capacity(4 + RESPONSE_HEADER + capacity * per_item);
        write_response_header(
            &mut buf,
            op as u8,
            flags,
            id,
            Status::Ok,
            read_number,
            read_hash,
        );
        Self {
            op,
            inline_code: op == Op::Account && flags & FLAG_INLINE_CODE != 0,
            count: 0,
            buf,
        }
    }

    fn bump(&mut self, op: Op) {
        assert_eq!(self.op, op, "result does not match the frame's op");
        self.count = self.count.checked_add(1).expect("u16 result count");
    }

    /// `code` is written only when the request set `INLINE_CODE`.
    pub fn push_account(
        &mut self,
        status: ItemStatus,
        nonce: u64,
        balance: &Word,
        code_hash: &Word,
        code: &[u8],
    ) {
        self.bump(Op::Account);
        self.buf.push(status as u8);
        self.buf.extend_from_slice(&nonce.to_le_bytes());
        self.buf.extend_from_slice(balance);
        self.buf.extend_from_slice(code_hash);
        if self.inline_code {
            self.buf
                .extend_from_slice(&(code.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(code);
        }
    }

    /// The canonical "no such account" result.
    pub fn push_account_absent(&mut self) {
        self.push_account(ItemStatus::Absent, 0, &[0; 32], &KECCAK_EMPTY, &[]);
    }

    pub fn push_account_error(&mut self) {
        self.push_account(ItemStatus::Error, 0, &[0; 32], &[0; 32], &[]);
    }

    pub fn push_storage(&mut self, status: ItemStatus, value: &Word) {
        self.bump(Op::Storage);
        self.buf.push(status as u8);
        self.buf.extend_from_slice(value);
    }

    pub fn push_code(&mut self, status: ItemStatus, code: &[u8]) {
        self.bump(Op::Code);
        self.buf.push(status as u8);
        self.buf
            .extend_from_slice(&(code.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(code);
    }

    /// Body bytes so far (compare against [`MAX_RESPONSE_BYTES`]).
    pub fn body_len(&self) -> usize {
        self.buf.len() - 4
    }

    pub fn finish(mut self, server_us: u32) -> Vec<u8> {
        let body = (self.buf.len() - 4) as u32;
        self.buf[0..4].copy_from_slice(&body.to_le_bytes());
        self.buf[4 + 2..4 + 4].copy_from_slice(&self.count.to_le_bytes());
        self.buf[4 + 12..4 + 16].copy_from_slice(&server_us.to_le_bytes());
        self.buf
    }
}

/// Builds one `Ok` Proof response frame (length prefix included): account
/// trie nodes first, then each storage trie via [`Self::begin_storage`].
/// Node counts, `count`, the length and `server_us` are patched as
/// sections close and in [`Self::finish`].
#[derive(Debug)]
pub struct ProofBuilder {
    count: u16,
    buf: Vec<u8>,
    /// Offset of the node count of the section being filled.
    nodes_at: usize,
    nodes: u32,
    in_storage: bool,
}

impl ProofBuilder {
    /// `capacity`: expected body bytes after the header.
    pub fn new(flags: u8, id: u32, read_number: u64, read_hash: &Word, capacity: usize) -> Self {
        let mut buf = Vec::with_capacity(4 + RESPONSE_HEADER + 4 + capacity);
        write_response_header(
            &mut buf,
            Op::Proof as u8,
            flags,
            id,
            Status::Ok,
            read_number,
            read_hash,
        );
        let nodes_at = buf.len();
        buf.extend_from_slice(&[0; 4]);
        Self {
            count: 0,
            buf,
            nodes_at,
            nodes: 0,
            in_storage: false,
        }
    }

    fn close_section(&mut self) {
        self.buf[self.nodes_at..self.nodes_at + 4].copy_from_slice(&self.nodes.to_le_bytes());
    }

    fn push_node(&mut self, path_len: usize, packed: &[u8], rlp: &[u8]) {
        assert!(path_len <= MAX_PATH_NIBBLES, "path longer than 64 nibbles");
        assert_eq!(packed.len(), path_len.div_ceil(2), "packed path length");
        assert!(
            path_len.is_multiple_of(2) || packed[packed.len() - 1] & 0x0F == 0,
            "odd path padding nibble must be zero"
        );
        assert!(!rlp.is_empty(), "empty proof node");
        self.nodes = self.nodes.checked_add(1).expect("u32 node count");
        self.buf.push(path_len as u8);
        self.buf.extend_from_slice(packed);
        self.buf
            .extend_from_slice(&(rlp.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(rlp);
    }

    /// An account trie node; all of them come before the first storage
    /// trie. `packed`: `path_len` nibbles packed as by [`pack_path`].
    pub fn push_account_node(&mut self, path_len: usize, packed: &[u8], rlp: &[u8]) {
        assert!(!self.in_storage, "account nodes come before storage tries");
        self.push_node(path_len, packed, rlp);
    }

    /// Opens the next storage trie section.
    pub fn begin_storage(&mut self, hashed_address: &Word, storage_root: &Word) {
        self.close_section();
        self.in_storage = true;
        self.count = self.count.checked_add(1).expect("u16 storage count");
        self.buf.extend_from_slice(hashed_address);
        self.buf.extend_from_slice(storage_root);
        self.nodes_at = self.buf.len();
        self.buf.extend_from_slice(&[0; 4]);
        self.nodes = 0;
    }

    /// A node of the storage trie opened last.
    pub fn push_storage_node(&mut self, path_len: usize, packed: &[u8], rlp: &[u8]) {
        assert!(self.in_storage, "no storage trie open");
        self.push_node(path_len, packed, rlp);
    }

    /// Body bytes so far (compare against [`MAX_RESPONSE_BYTES`]).
    pub fn body_len(&self) -> usize {
        self.buf.len() - 4
    }

    pub fn finish(mut self, server_us: u32) -> Vec<u8> {
        self.close_section();
        let body = (self.buf.len() - 4) as u32;
        self.buf[0..4].copy_from_slice(&body.to_le_bytes());
        self.buf[4 + 2..4 + 4].copy_from_slice(&self.count.to_le_bytes());
        self.buf[4 + 12..4 + 16].copy_from_slice(&server_us.to_le_bytes());
        self.buf
    }
}

/// Packs a one-nibble-per-byte path (each `< 16`, at most 64) the way
/// proof nodes carry it: high nibble first, an odd path's last low nibble
/// zero.
pub fn pack_path(nibbles: &[u8]) -> Vec<u8> {
    assert!(
        nibbles.len() <= MAX_PATH_NIBBLES,
        "path longer than 64 nibbles"
    );
    nibbles
        .chunks(2)
        .map(|pair| {
            assert!(pair.iter().all(|&n| n < 16), "nibble out of range");
            pair[0] << 4 | pair.get(1).copied().unwrap_or(0)
        })
        .collect()
}

fn write_response_header(
    buf: &mut Vec<u8>,
    op: u8,
    flags: u8,
    id: u32,
    status: Status,
    read_number: u64,
    read_hash: &Word,
) {
    buf.extend_from_slice(&[0; 4]); // length
    buf.extend_from_slice(&[op, flags, 0, 0]); // count patched later
    buf.extend_from_slice(&id.to_le_bytes());
    buf.extend_from_slice(&[status as u8, 0, 0, 0]);
    buf.extend_from_slice(&[0; 4]); // server_us
    buf.extend_from_slice(&read_number.to_le_bytes());
    buf.extend_from_slice(read_hash);
}

/// A header-only non-`Ok` response frame (length prefix included). `op`
/// and `flags` are echoed raw — they may be the invalid bytes a
/// `BadRequest` is answering.
pub fn error_response(op: u8, flags: u8, id: u32, status: Status, server_us: u32) -> Vec<u8> {
    debug_assert_ne!(status, Status::Ok);
    let mut buf = Vec::with_capacity(4 + RESPONSE_HEADER);
    write_response_header(&mut buf, op, flags, id, status, 0, &[0; 32]);
    buf[0..4].copy_from_slice(&(RESPONSE_HEADER as u32).to_le_bytes());
    buf[4 + 12..4 + 16].copy_from_slice(&server_us.to_le_bytes());
    buf
}

/// One result, borrowed from the frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultRef<'a> {
    Account {
        status: ItemStatus,
        nonce: u64,
        balance: &'a Word,
        code_hash: &'a Word,
        /// `Some` iff the request set `INLINE_CODE`.
        code: Option<&'a [u8]>,
    },
    Storage {
        status: ItemStatus,
        value: &'a Word,
    },
    Code {
        status: ItemStatus,
        code: &'a [u8],
    },
}

/// A validated response, borrowing the frame body. Decoding walks the
/// results once to check every length and status; it allocates nothing.
#[derive(Clone, Copy, Debug)]
pub struct ResponseRef<'a> {
    /// Raw: a `BadRequest` may echo an invalid op.
    pub op: u8,
    pub flags: u8,
    pub id: u32,
    pub status: Status,
    pub server_us: u32,
    pub read_number: u64,
    pub read_hash: &'a Word,
    count: u16,
    results: &'a [u8],
}

impl<'a> ResponseRef<'a> {
    pub fn decode(body: &'a [u8]) -> Result<Self, DecodeError> {
        if body.len() < RESPONSE_HEADER {
            return Err(DecodeError::Truncated);
        }
        let status = Status::from_u8(body[8]).ok_or(DecodeError::BadStatus(body[8]))?;
        if body[9..12] != [0, 0, 0] {
            return Err(DecodeError::BadReserved);
        }
        let (op, flags, count) = (body[0], body[1], u16_at(body, 2));
        let results = &body[RESPONSE_HEADER..];
        let resp = Self {
            op,
            flags,
            id: u32_at(body, 4),
            status,
            server_us: u32_at(body, 12),
            read_number: u64_at(body, 16),
            read_hash: word_at(body, 24),
            count,
            results,
        };
        if status != Status::Ok {
            if count != 0 {
                return Err(DecodeError::ErrorWithItems);
            }
            if !results.is_empty() {
                return Err(DecodeError::TrailingBytes);
            }
            return Ok(resp);
        }
        let op = Op::from_u8(op).ok_or(DecodeError::BadOp(op))?;
        if flags & !KNOWN_FLAGS != 0 || (flags & FLAG_INLINE_CODE != 0 && op != Op::Account) {
            return Err(DecodeError::BadFlags(flags));
        }
        if count as usize > op.max_items(flags) {
            return Err(DecodeError::TooManyItems(count));
        }
        if op == Op::Proof {
            check_proof(results, count)?;
            return Ok(resp);
        }
        // One validating walk; `results()` can then index without checks.
        let mut at = 0usize;
        for _ in 0..count {
            let (status_byte, len) = match op {
                Op::Storage => (results.get(at).copied(), STORAGE_RESULT),
                Op::Account if flags & FLAG_INLINE_CODE == 0 => {
                    (results.get(at).copied(), ACCOUNT_RESULT)
                }
                Op::Account => {
                    let head = ACCOUNT_RESULT + 4;
                    let code_len = results
                        .get(at + ACCOUNT_RESULT..at + head)
                        .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")) as usize)
                        .ok_or(DecodeError::Truncated)?;
                    (results.get(at).copied(), head + code_len)
                }
                Op::Code => {
                    let code_len = results
                        .get(at + 1..at + CODE_RESULT_HEADER)
                        .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")) as usize)
                        .ok_or(DecodeError::Truncated)?;
                    (results.get(at).copied(), CODE_RESULT_HEADER + code_len)
                }
                Op::Proof => unreachable!("validated by check_proof"),
            };
            let status_byte = status_byte.ok_or(DecodeError::Truncated)?;
            ItemStatus::from_u8(status_byte).ok_or(DecodeError::BadItemStatus(status_byte))?;
            at = at
                .checked_add(len)
                .filter(|&end| end <= results.len())
                .ok_or(DecodeError::Truncated)?;
        }
        if at != results.len() {
            return Err(DecodeError::TrailingBytes);
        }
        Ok(resp)
    }

    pub fn count(&self) -> usize {
        self.count as usize
    }

    /// Results in request order (empty unless `status == Ok`; always empty
    /// for `Proof`, see [`Self::proof`]).
    pub fn results(&self) -> Results<'a> {
        let op = Op::from_u8(self.op);
        Results {
            op,
            inline_code: self.flags & FLAG_INLINE_CODE != 0,
            left: if self.status == Status::Ok && op != Some(Op::Proof) {
                self.count
            } else {
                0
            },
            rest: self.results,
        }
    }

    /// The proof of an `Ok` Proof response.
    pub fn proof(&self) -> Option<ProofRef<'a>> {
        (self.status == Status::Ok && self.op == Op::Proof as u8)
            .then(|| ProofRef::new(self.results, self.count))
    }
}

/// Validates the node list starting at `at` (`u32` count, then nodes);
/// returns the offset after it.
fn check_nodes(b: &[u8], mut at: usize) -> Result<usize, DecodeError> {
    let n = b
        .get(at..at + 4)
        .map(|x| u32_at(x, 0))
        .ok_or(DecodeError::Truncated)?;
    at += 4;
    for _ in 0..n {
        let path_len = *b.get(at).ok_or(DecodeError::Truncated)? as usize;
        if path_len > MAX_PATH_NIBBLES {
            return Err(DecodeError::BadPath);
        }
        let packed = path_len.div_ceil(2);
        let path = b
            .get(at + 1..at + 1 + packed)
            .ok_or(DecodeError::Truncated)?;
        if !path_len.is_multiple_of(2) && path[packed - 1] & 0x0F != 0 {
            return Err(DecodeError::BadPath);
        }
        at += 1 + packed;
        let rlp_len = b
            .get(at..at + 4)
            .map(|x| u32_at(x, 0) as usize)
            .ok_or(DecodeError::Truncated)?;
        if rlp_len == 0 {
            return Err(DecodeError::EmptyNode);
        }
        at = at
            .checked_add(4 + rlp_len)
            .filter(|&end| end <= b.len())
            .ok_or(DecodeError::Truncated)?;
    }
    Ok(at)
}

/// Validates a whole Proof body: account nodes, then `count` storage tries,
/// nothing after.
fn check_proof(results: &[u8], count: u16) -> Result<(), DecodeError> {
    let mut at = check_nodes(results, 0)?;
    for _ in 0..count {
        if results.len() < at + 64 {
            return Err(DecodeError::Truncated);
        }
        at = check_nodes(results, at + 64)?;
    }
    if at != results.len() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(())
}

/// Bytes `n` validated nodes occupy at the start of `b`.
fn nodes_len(b: &[u8], n: u32) -> usize {
    let mut at = 0;
    for _ in 0..n {
        let packed = (b[at] as usize).div_ceil(2);
        at += 1 + packed;
        at += 4 + u32_at(b, at) as usize;
    }
    at
}

/// A validated proof, borrowing the frame.
#[derive(Clone, Copy, Debug)]
pub struct ProofRef<'a> {
    account: &'a [u8],
    account_nodes: u32,
    storages: &'a [u8],
    storage_count: u16,
}

impl<'a> ProofRef<'a> {
    fn new(results: &'a [u8], count: u16) -> Self {
        let n = u32_at(results, 0);
        let end = 4 + nodes_len(&results[4..], n);
        Self {
            account: &results[4..end],
            account_nodes: n,
            storages: &results[end..],
            storage_count: count,
        }
    }

    /// Account trie nodes, sorted by path (server guarantee).
    pub fn account_nodes(&self) -> Nodes<'a> {
        Nodes {
            left: self.account_nodes,
            rest: self.account,
        }
    }

    /// Storage tries, sorted by hashed address (server guarantee).
    pub fn storages(&self) -> Storages<'a> {
        Storages {
            left: self.storage_count,
            rest: self.storages,
        }
    }
}

/// One trie node: its path and RLP encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeRef<'a> {
    /// In nibbles (`0` = the trie root).
    pub path_len: u8,
    /// `⌈path_len / 2⌉` bytes, high nibble first.
    pub packed_path: &'a [u8],
    pub rlp: &'a [u8],
}

impl<'a> NodeRef<'a> {
    /// The path, one nibble per item.
    pub fn nibbles(&self) -> impl ExactSizeIterator<Item = u8> + 'a {
        let packed = self.packed_path;
        (0..self.path_len as usize).map(move |i| {
            let byte = packed[i / 2];
            if i.is_multiple_of(2) {
                byte >> 4
            } else {
                byte & 0x0F
            }
        })
    }
}

/// Iterator over a validated node list.
#[derive(Clone, Debug)]
pub struct Nodes<'a> {
    left: u32,
    rest: &'a [u8],
}

impl<'a> Iterator for Nodes<'a> {
    type Item = NodeRef<'a>;

    fn next(&mut self) -> Option<NodeRef<'a>> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let b = self.rest;
        let path_len = b[0];
        let packed = (path_len as usize).div_ceil(2);
        let rlp_at = 1 + packed + 4;
        let rlp_len = u32_at(b, 1 + packed) as usize;
        self.rest = &b[rlp_at + rlp_len..];
        Some(NodeRef {
            path_len,
            packed_path: &b[1..1 + packed],
            rlp: &b[rlp_at..rlp_at + rlp_len],
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left as usize, Some(self.left as usize))
    }
}

impl ExactSizeIterator for Nodes<'_> {}

/// One storage trie of a proof.
#[derive(Clone, Debug)]
pub struct StorageRef<'a> {
    pub hashed_address: &'a Word,
    pub storage_root: &'a Word,
    /// Sorted by path (server guarantee).
    pub nodes: Nodes<'a>,
}

/// Iterator over a validated proof's storage tries.
#[derive(Clone, Debug)]
pub struct Storages<'a> {
    left: u16,
    rest: &'a [u8],
}

impl<'a> Iterator for Storages<'a> {
    type Item = StorageRef<'a>;

    fn next(&mut self) -> Option<StorageRef<'a>> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let b = self.rest;
        let n = u32_at(b, 64);
        let end = 68 + nodes_len(&b[68..], n);
        self.rest = &b[end..];
        Some(StorageRef {
            hashed_address: word_at(b, 0),
            storage_root: word_at(b, 32),
            nodes: Nodes {
                left: n,
                rest: &b[68..end],
            },
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left as usize, Some(self.left as usize))
    }
}

impl ExactSizeIterator for Storages<'_> {}

/// Iterator over a decoded response's results.
#[derive(Clone, Debug)]
pub struct Results<'a> {
    op: Option<Op>,
    inline_code: bool,
    left: u16,
    rest: &'a [u8],
}

impl<'a> Iterator for Results<'a> {
    type Item = ResultRef<'a>;

    fn next(&mut self) -> Option<ResultRef<'a>> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let b = self.rest;
        // Lengths were validated by `ResponseRef::decode`.
        let status = ItemStatus::from_u8(b[0]).expect("validated item status");
        let (result, used) = match self.op.expect("validated op") {
            Op::Storage => (
                ResultRef::Storage {
                    status,
                    value: word_at(b, 1),
                },
                STORAGE_RESULT,
            ),
            Op::Account => {
                let (code, used) = if self.inline_code {
                    let len = u32_at(b, ACCOUNT_RESULT) as usize;
                    let start = ACCOUNT_RESULT + 4;
                    (Some(&b[start..start + len]), start + len)
                } else {
                    (None, ACCOUNT_RESULT)
                };
                (
                    ResultRef::Account {
                        status,
                        nonce: u64_at(b, 1),
                        balance: word_at(b, 9),
                        code_hash: word_at(b, 41),
                        code,
                    },
                    used,
                )
            }
            Op::Code => {
                let len = u32_at(b, 1) as usize;
                (
                    ResultRef::Code {
                        status,
                        code: &b[CODE_RESULT_HEADER..CODE_RESULT_HEADER + len],
                    },
                    CODE_RESULT_HEADER + len,
                )
            }
            Op::Proof => unreachable!("Proof responses have no per-item results"),
        };
        self.rest = &b[used..];
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left as usize, Some(self.left as usize))
    }
}

impl ExactSizeIterator for Results<'_> {}

// ------------------------------------------------------------------ framing

/// Reads one length-prefixed lookup frame body into `buf` (cleared and
/// reused). `Ok(false)` on clean EOF at a frame boundary. A length outside
/// `[PREFIX, max]` is rejected before anything is allocated for it.
pub async fn read_body<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
    max: usize,
) -> std::io::Result<bool> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
        Err(e) => return Err(e),
    }
    let len = check_len(u32::from_le_bytes(len), max)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    buf.clear();
    buf.resize(len, 0);
    r.read_exact(buf).await?;
    Ok(true)
}

/// The write side of a lookup connection, shared by many producers (server
/// read workers, client callers), with no task hop and no lock convoy:
///
/// - a sender drops its frame in a small inbox and then tries to become the
///   *combiner* (`try_lock` on the socket side). The combiner writes every
///   frame waiting in the inbox with one vectored, non-blocking syscall from
///   its own thread, re-checking the inbox before it leaves; a sender that
///   finds a combiner already active just returns — its frame rides along.
///   At low load that is one direct write per frame; under load, many
///   responses share a syscall instead of queueing for the lock.
/// - only what the socket can't take right now waits in a backlog for the
///   async flusher ([`FrameWriter::run`]), which waits for writability with a
///   deadline. Frame order is the order senders took the inbox lock.
///
/// Each frame carries a guard `G` dropped once the frame is fully written
/// (the server's output-slot permit), so queued output stays bounded.
pub struct FrameWriter<G> {
    half: OwnedWriteHalf,
    inbox: Mutex<Vec<(Vec<u8>, G)>>,
    io: Mutex<Backlog<G>>,
    closed: std::sync::atomic::AtomicBool,
    wake: Notify,
}

struct Backlog<G> {
    /// (frame, bytes already written, guard).
    frames: VecDeque<(Vec<u8>, usize, G)>,
}

/// The connection's write side is closed (I/O error or deadline).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterClosed;

impl std::fmt::Display for WriterClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("lookup connection write side closed")
    }
}

impl std::error::Error for WriterClosed {}

/// Frames per vectored write.
const MAX_IOV: usize = 128;

impl<G: Send> FrameWriter<G> {
    pub fn new(half: OwnedWriteHalf) -> Self {
        Self {
            half,
            inbox: Mutex::new(Vec::new()),
            io: Mutex::new(Backlog {
                frames: VecDeque::new(),
            }),
            closed: false.into(),
            wake: Notify::new(),
        }
    }

    /// Queues `frame` and writes it (with anything else waiting) unless
    /// another thread is already writing, in which case that thread will.
    /// Never blocks on the socket; callable from any thread (no tokio
    /// runtime context needed).
    pub fn send(&self, frame: Vec<u8>, guard: G) -> Result<(), WriterClosed> {
        if self.is_closed() {
            return Err(WriterClosed);
        }
        self.inbox
            .lock()
            .map_err(|_| WriterClosed)?
            .push((frame, guard));
        self.combine(false);
        if self.is_closed() {
            return Err(WriterClosed);
        }
        Ok(())
    }

    /// Marks the writer closed and drops queued frames (and their guards).
    pub fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Ok(mut inbox) = self.inbox.lock() {
            inbox.clear();
        }
        if let Ok(mut io) = self.io.lock() {
            io.frames.clear();
        }
        self.wake.notify_one();
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Becomes the combiner if nobody else is (`wait = false`), or waits for
    /// the socket side (`wait = true`, the flusher). Returns whether frames
    /// are left waiting for writability.
    fn combine(&self, wait: bool) -> bool {
        loop {
            let io = if wait {
                self.io.lock().ok()
            } else {
                match self.io.try_lock() {
                    Ok(io) => Some(io),
                    // An active combiner re-checks the inbox before leaving.
                    Err(std::sync::TryLockError::WouldBlock) => return false,
                    Err(std::sync::TryLockError::Poisoned(_)) => None,
                }
            };
            let Some(mut io) = io else {
                self.close();
                return false;
            };
            if let Ok(mut inbox) = self.inbox.lock() {
                io.frames
                    .extend(inbox.drain(..).map(|(frame, guard)| (frame, 0, guard)));
            }
            let blocked = match self.write_backlog(&mut io) {
                Ok(blocked) => blocked,
                Err(_) => {
                    drop(io);
                    self.close();
                    return false;
                }
            };
            drop(io);
            if blocked {
                // Leave the rest to the flusher (it waits for writability).
                self.wake.notify_one();
                return true;
            }
            // Frames that arrived while we wrote, from senders that found us
            // busy, are ours to write too.
            if self.inbox.lock().map_or(true, |i| i.is_empty()) {
                return false;
            }
        }
    }

    /// Writes the backlog until it is empty (`Ok(false)`) or the socket is
    /// full (`Ok(true)`).
    fn write_backlog(&self, io: &mut Backlog<G>) -> std::io::Result<bool> {
        if self.is_closed() {
            io.frames.clear();
            return Ok(false);
        }
        while !io.frames.is_empty() {
            let slices: Vec<IoSlice<'_>> = io
                .frames
                .iter()
                .take(MAX_IOV)
                .map(|(f, at, _)| IoSlice::new(&f[*at..]))
                .collect();
            let mut n = match self.half.try_write_vectored(&slices) {
                Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(true),
                Err(e) => return Err(e),
            };
            drop(slices);
            while n > 0 {
                let (frame, at, _) = io.frames.front_mut().expect("bytes written from a frame");
                let left = frame.len() - *at;
                if n >= left {
                    n -= left;
                    io.frames.pop_front(); // drops the guard: slot released
                } else {
                    *at += n;
                    n = 0;
                }
            }
        }
        Ok(false)
    }

    /// The flusher: whenever a combiner left frames behind because the
    /// socket was full, waits (at most `deadline` each time) for it to drain
    /// and writes the rest. Returns once closed; an I/O error or a missed
    /// deadline closes the writer.
    pub async fn run(&self, deadline: Duration) -> std::io::Result<()> {
        loop {
            self.wake.notified().await;
            while !self.is_closed() && self.combine(true) {
                match tokio::time::timeout(deadline, self.half.writable()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        self.close();
                        return Err(err);
                    }
                    Err(_) => {
                        self.close();
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("lookup write timed out after {deadline:?}"),
                        ));
                    }
                }
            }
            if self.is_closed() {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// xorshift64* — same deterministic PRNG as the T0.3 tests.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        fn word(&mut self) -> Word {
            let mut w = [0u8; 32];
            for c in w.chunks_mut(8) {
                c.copy_from_slice(&self.next().to_le_bytes());
            }
            w
        }
        fn addr(&mut self) -> Addr {
            self.word()[..20].try_into().unwrap()
        }
        fn bytes(&mut self, max: usize) -> Vec<u8> {
            let n = self.below(max as u64 + 1) as usize;
            (0..n).map(|_| self.next() as u8).collect()
        }
        /// Mostly small counts, sometimes the cap, sometimes zero.
        fn count(&mut self, cap: usize) -> usize {
            match self.below(10) {
                0 => 0,
                1 => cap,
                _ => self.below(cap.min(40) as u64 + 1) as usize,
            }
        }
    }

    /// Owned model of a request (test-only).
    #[derive(Clone, Debug, PartialEq)]
    enum OwnedItem {
        Account(Addr),
        Storage(Addr, Word),
        Code(Word),
        Proof(Word, Vec<Word>),
    }

    fn random_request(rng: &mut Rng) -> (Op, u8, u32, Word, Vec<OwnedItem>) {
        let op = Op::from_u8(1 + rng.below(4) as u8).unwrap();
        let flags = if op == Op::Account && rng.below(2) == 0 {
            FLAG_INLINE_CODE
        } else {
            0
        };
        let pin = if rng.below(4) == 0 {
            LATEST
        } else {
            rng.word()
        };
        let n = rng.count(op.max_items(flags));
        let mut slots_left = MAX_PROOF_SLOTS;
        let items = (0..n)
            .map(|_| match op {
                Op::Account => OwnedItem::Account(rng.addr()),
                Op::Storage => OwnedItem::Storage(rng.addr(), rng.word()),
                Op::Code => OwnedItem::Code(rng.word()),
                Op::Proof => {
                    // Mostly a few slots, sometimes many, never past the cap.
                    let want = match rng.below(20) {
                        0 => rng.below(600) as usize,
                        _ => rng.below(5) as usize,
                    };
                    let k = want.min(slots_left);
                    slots_left -= k;
                    OwnedItem::Proof(rng.word(), (0..k).map(|_| rng.word()).collect())
                }
            })
            .collect();
        (op, flags, rng.next() as u32, pin, items)
    }

    fn encode_request(op: Op, flags: u8, id: u32, pin: &Word, items: &[OwnedItem]) -> Vec<u8> {
        let mut b = RequestBuilder::with_capacity(op, flags, id, pin, items.len());
        for item in items {
            match item {
                OwnedItem::Account(a) => b.push_account(a),
                OwnedItem::Storage(a, s) => b.push_storage(a, s),
                OwnedItem::Code(h) => b.push_code(h),
                OwnedItem::Proof(a, slots) => b.push_proof_target(a, slots),
            };
        }
        b.finish()
    }

    fn body(frame: &[u8]) -> &[u8] {
        let len = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4, "length prefix covers the body");
        &frame[4..]
    }

    #[test]
    fn roundtrip_10k_random_requests() {
        let mut rng = Rng(0x5117_1009_5117_1009);
        for i in 0..10_000 {
            let (op, flags, id, pin, items) = random_request(&mut rng);
            let frame = encode_request(op, flags, id, &pin, &items);
            let req = RequestRef::decode(body(&frame)).unwrap_or_else(|e| panic!("{i}: {e}"));
            assert_eq!((req.op, req.flags, req.id, *req.pin), (op, flags, id, pin));
            assert_eq!(req.count(), items.len());
            assert_eq!(req.is_latest(), pin == LATEST);
            let back: Vec<OwnedItem> = req
                .items()
                .map(|item| match item {
                    Item::Account(a) => OwnedItem::Account(*a),
                    Item::Storage(a, s) => OwnedItem::Storage(*a, *s),
                    Item::Code(h) => OwnedItem::Code(*h),
                    Item::Proof(t) => {
                        assert_eq!(t.slots().len(), t.slot_count());
                        OwnedItem::Proof(*t.hashed_address, t.slots().copied().collect())
                    }
                })
                .collect();
            assert_eq!(back, items, "iteration {i}");
        }
    }

    /// Owned model of one result (test-only).
    #[derive(Clone, Debug, PartialEq)]
    enum OwnedResult {
        Account(ItemStatus, u64, Word, Word, Option<Vec<u8>>),
        Storage(ItemStatus, Word),
        Code(ItemStatus, Vec<u8>),
    }

    fn item_status(rng: &mut Rng) -> ItemStatus {
        ItemStatus::from_u8(rng.below(3) as u8).unwrap()
    }

    struct Model {
        op: Op,
        flags: u8,
        id: u32,
        status: Status,
        server_us: u32,
        read_number: u64,
        read_hash: Word,
        results: Vec<OwnedResult>,
    }

    fn random_response(rng: &mut Rng) -> Model {
        let op = Op::from_u8(1 + rng.below(3) as u8).unwrap();
        let flags = if op == Op::Account && rng.below(2) == 0 {
            FLAG_INLINE_CODE
        } else {
            0
        };
        let status = if rng.below(5) == 0 {
            Status::from_u8(1 + rng.below(5) as u8).unwrap()
        } else {
            Status::Ok
        };
        let (read_number, read_hash, results) = if status == Status::Ok {
            let n = rng.count(op.max_items(flags));
            let results = (0..n)
                .map(|_| match op {
                    Op::Account => OwnedResult::Account(
                        item_status(rng),
                        rng.next(),
                        rng.word(),
                        rng.word(),
                        (flags != 0).then(|| rng.bytes(200)),
                    ),
                    Op::Storage => OwnedResult::Storage(item_status(rng), rng.word()),
                    Op::Code => OwnedResult::Code(item_status(rng), rng.bytes(300)),
                    Op::Proof => unreachable!("ops 1-3 only; proofs: random_proof"),
                })
                .collect();
            (rng.next(), rng.word(), results)
        } else {
            (0, [0; 32], Vec::new())
        };
        Model {
            op,
            flags,
            id: rng.next() as u32,
            status,
            server_us: rng.next() as u32,
            read_number,
            read_hash,
            results,
        }
    }

    fn encode_response(m: &Model) -> Vec<u8> {
        if m.status != Status::Ok {
            return error_response(m.op as u8, m.flags, m.id, m.status, m.server_us);
        }
        let mut b = ResponseBuilder::new(
            m.op,
            m.flags,
            m.id,
            m.read_number,
            &m.read_hash,
            m.results.len(),
        );
        for r in &m.results {
            match r {
                OwnedResult::Account(s, nonce, bal, ch, code) => {
                    b.push_account(*s, *nonce, bal, ch, code.as_deref().unwrap_or(&[]))
                }
                OwnedResult::Storage(s, v) => b.push_storage(*s, v),
                OwnedResult::Code(s, code) => b.push_code(*s, code),
            }
        }
        b.finish(m.server_us)
    }

    fn owned(r: ResultRef<'_>) -> OwnedResult {
        match r {
            ResultRef::Account {
                status,
                nonce,
                balance,
                code_hash,
                code,
            } => OwnedResult::Account(
                status,
                nonce,
                *balance,
                *code_hash,
                code.map(<[u8]>::to_vec),
            ),
            ResultRef::Storage { status, value } => OwnedResult::Storage(status, *value),
            ResultRef::Code { status, code } => OwnedResult::Code(status, code.to_vec()),
        }
    }

    #[test]
    fn roundtrip_10k_random_responses() {
        let mut rng = Rng(0xD9D9_0000_D9D9_0001);
        for i in 0..10_000 {
            let m = random_response(&mut rng);
            let frame = encode_response(&m);
            let resp = ResponseRef::decode(body(&frame)).unwrap_or_else(|e| panic!("{i}: {e}"));
            assert_eq!(
                (resp.op, resp.flags, resp.id, resp.status, resp.server_us),
                (m.op as u8, m.flags, m.id, m.status, m.server_us),
                "iteration {i}"
            );
            assert_eq!(
                (resp.read_number, *resp.read_hash),
                (m.read_number, m.read_hash)
            );
            assert_eq!(resp.count(), m.results.len());
            let back: Vec<OwnedResult> = resp.results().map(owned).collect();
            assert_eq!(back, m.results, "iteration {i}");
        }
    }

    /// Owned model of a proof (test-only): nodes as (nibbles, rlp).
    type OwnedNodes = Vec<(Vec<u8>, Vec<u8>)>;

    #[derive(Debug, PartialEq)]
    struct ProofModel {
        account: OwnedNodes,
        storages: Vec<(Word, Word, OwnedNodes)>,
    }

    fn random_nodes(rng: &mut Rng, max: usize) -> OwnedNodes {
        let n = rng.count(max);
        (0..n)
            .map(|_| {
                let len = match rng.below(8) {
                    0 => 0,
                    1 => MAX_PATH_NIBBLES,
                    _ => rng.below(MAX_PATH_NIBBLES as u64 + 1) as usize,
                };
                let path = (0..len).map(|_| rng.below(16) as u8).collect();
                let mut rlp = rng.bytes(120);
                if rlp.is_empty() {
                    rlp.push(0x80);
                }
                (path, rlp)
            })
            .collect()
    }

    fn random_proof(rng: &mut Rng) -> ProofModel {
        ProofModel {
            account: random_nodes(rng, 60),
            storages: (0..rng.count(12))
                .map(|_| (rng.word(), rng.word(), random_nodes(rng, 20)))
                .collect(),
        }
    }

    fn encode_proof(m: &ProofModel, id: u32, read_number: u64, read_hash: &Word) -> Vec<u8> {
        let mut b = ProofBuilder::new(0, id, read_number, read_hash, 0);
        for (path, rlp) in &m.account {
            b.push_account_node(path.len(), &pack_path(path), rlp);
        }
        for (addr, root, nodes) in &m.storages {
            b.begin_storage(addr, root);
            for (path, rlp) in nodes {
                b.push_storage_node(path.len(), &pack_path(path), rlp);
            }
        }
        b.finish(42)
    }

    fn owned_nodes(nodes: Nodes<'_>) -> OwnedNodes {
        nodes
            .map(|n| {
                assert_eq!(n.nibbles().len(), n.path_len as usize);
                (n.nibbles().collect(), n.rlp.to_vec())
            })
            .collect()
    }

    fn owned_proof(p: ProofRef<'_>) -> ProofModel {
        ProofModel {
            account: owned_nodes(p.account_nodes()),
            storages: p
                .storages()
                .map(|s| (*s.hashed_address, *s.storage_root, owned_nodes(s.nodes)))
                .collect(),
        }
    }

    #[test]
    fn roundtrip_3k_random_proofs() {
        let mut rng = Rng(0xD10D_10D1_0D10_D10D);
        for i in 0..3_000 {
            let m = random_proof(&mut rng);
            let (id, number, hash) = (rng.next() as u32, rng.next(), rng.word());
            let frame = encode_proof(&m, id, number, &hash);
            let resp = ResponseRef::decode(body(&frame)).unwrap_or_else(|e| panic!("{i}: {e}"));
            assert_eq!(
                (resp.op, resp.flags, resp.id, resp.status, resp.server_us),
                (Op::Proof as u8, 0, id, Status::Ok, 42)
            );
            assert_eq!((resp.read_number, *resp.read_hash), (number, hash));
            assert_eq!(resp.count(), m.storages.len());
            assert_eq!(resp.results().count(), 0, "no per-item results");
            assert_eq!(owned_proof(resp.proof().unwrap()), m, "iteration {i}");
        }
    }

    /// Cut points for a body of `len` bytes: every cut through the first
    /// and last 256 bytes (headers, first/last items) plus 64 random ones.
    fn cuts(rng: &mut Rng, len: usize) -> Vec<usize> {
        let mut cuts: Vec<usize> = (0..len.min(256))
            .chain(len.saturating_sub(256)..len)
            .collect();
        cuts.extend((0..64).map(|_| rng.below(len as u64) as usize));
        cuts
    }

    /// Strict prefixes and one-byte extensions of valid frames are rejected,
    /// in both directions.
    #[test]
    fn truncated_and_extended_frames_are_rejected() {
        let mut rng = Rng(0x7EAC_7EAC_7EAC_7EAC);
        for i in 0..1000 {
            let (op, flags, id, pin, items) = random_request(&mut rng);
            let frame = encode_request(op, flags, id, &pin, &items);
            let b = body(&frame);
            for cut in cuts(&mut rng, b.len()) {
                assert!(RequestRef::decode(&b[..cut]).is_err(), "req {i} cut {cut}");
            }
            let mut long = b.to_vec();
            long.push(rng.next() as u8);
            assert_eq!(
                RequestRef::decode(&long).unwrap_err(),
                DecodeError::TrailingBytes
            );

            let m = random_response(&mut rng);
            let frame = encode_response(&m);
            let b = body(&frame);
            for cut in cuts(&mut rng, b.len()) {
                assert!(
                    ResponseRef::decode(&b[..cut]).is_err(),
                    "resp {i} cut {cut}"
                );
            }
            let mut long = b.to_vec();
            long.push(rng.next() as u8);
            assert!(ResponseRef::decode(&long).is_err(), "resp {i} extended");

            let frame = encode_proof(&random_proof(&mut rng), 1, 2, &[3; 32]);
            let b = body(&frame);
            for cut in cuts(&mut rng, b.len()) {
                assert!(
                    ResponseRef::decode(&b[..cut]).is_err(),
                    "proof {i} cut {cut}"
                );
            }
            let mut long = b.to_vec();
            long.push(rng.next() as u8);
            assert!(ResponseRef::decode(&long).is_err(), "proof {i} extended");
        }
    }

    #[test]
    fn oversized_counts_and_lengths_are_rejected() {
        // count one past the cap, with a body long enough to hold it.
        for (op, flags) in [
            (Op::Account, 0),
            (Op::Account, FLAG_INLINE_CODE),
            (Op::Storage, 0),
            (Op::Code, 0),
            (Op::Proof, 0),
        ] {
            let cap = op.max_items(flags);
            let mut b = RequestBuilder::new(op, flags, 1, &LATEST).finish();
            b[6..8].copy_from_slice(&((cap + 1) as u16).to_le_bytes());
            b.resize(4 + REQUEST_HEADER + (cap + 1) * op.item_len(), 0);
            assert_eq!(
                RequestRef::decode(&b[4..]).unwrap_err(),
                DecodeError::TooManyItems(cap as u16 + 1)
            );
        }
        assert_eq!(MAX_REQUEST_BYTES, 167_976);
        assert!(check_len(MAX_REQUEST_BYTES as u32, MAX_REQUEST_BYTES).is_ok());
        assert!(check_len(MAX_REQUEST_BYTES as u32 + 1, MAX_REQUEST_BYTES).is_err());
        assert!(check_len(PREFIX as u32 - 1, MAX_REQUEST_BYTES).is_err());
        assert!(check_len(u32::MAX, MAX_RESPONSE_BYTES).is_err());
        // A full storage frame fits; a full proof frame is exactly the cap.
        let mut b = RequestBuilder::new(Op::Storage, 0, 1, &LATEST);
        while !b.is_full() {
            b.push_storage(&[1; 20], &[2; 32]);
        }
        assert_eq!(b.finish().len() - 4, 53_288);
        let mut b = RequestBuilder::new(Op::Proof, 0, 1, &LATEST);
        let slots = vec![[2; 32]; MAX_PROOF_SLOTS / MAX_ITEMS];
        while b.fits_proof_target(slots.len()) {
            b.push_proof_target(&[1; 32], &slots);
        }
        assert!(b.is_full());
        let full = b.finish();
        assert_eq!(full.len() - 4, MAX_REQUEST_BYTES);
        assert_eq!(RequestRef::decode(&full[4..]).unwrap().count(), MAX_ITEMS);
        // One slot past the cap: the last target claims an extra slot.
        let mut b = RequestBuilder::new(Op::Proof, 0, 1, &LATEST);
        b.push_proof_target(&[1; 32], &vec![[2; 32]; MAX_PROOF_SLOTS]);
        assert!(!b.fits_proof_target(1));
        let mut over = b.finish()[4..].to_vec();
        over[2..4].copy_from_slice(&2u16.to_le_bytes()); // body: count at 2..4
        over.extend_from_slice(&[9; 32]);
        over.extend_from_slice(&1u16.to_le_bytes());
        over.extend_from_slice(&[0, 0]);
        over.extend_from_slice(&[7; 32]);
        assert_eq!(
            RequestRef::decode(&over).unwrap_err(),
            DecodeError::TooManySlots(MAX_PROOF_SLOTS + 1)
        );
        // A proof answer may carry at most MAX_ITEMS storage tries.
        let mut r = ProofBuilder::new(0, 1, 1, &[1; 32], 0).finish(0)[4..].to_vec();
        r[2..4].copy_from_slice(&(MAX_ITEMS as u16 + 1).to_le_bytes());
        assert_eq!(
            ResponseRef::decode(&r).unwrap_err(),
            DecodeError::TooManyItems(MAX_ITEMS as u16 + 1)
        );
    }

    #[test]
    fn invalid_op_flags_status_reserved_are_rejected() {
        let good = RequestBuilder::new(Op::Storage, 0, 9, &LATEST).finish();
        let mut b = good[4..].to_vec();
        b[0] = 0;
        assert_eq!(RequestRef::decode(&b).unwrap_err(), DecodeError::BadOp(0));
        b[0] = 5;
        assert_eq!(RequestRef::decode(&b).unwrap_err(), DecodeError::BadOp(5));
        let mut b = good[4..].to_vec();
        b[1] = FLAG_INLINE_CODE; // inline code on a Storage op
        assert_eq!(
            RequestRef::decode(&b).unwrap_err(),
            DecodeError::BadFlags(FLAG_INLINE_CODE)
        );
        let mut b = RequestBuilder::new(Op::Account, 0, 9, &LATEST).finish()[4..].to_vec();
        b[1] = 0x80; // unknown flag
        assert_eq!(
            RequestRef::decode(&b).unwrap_err(),
            DecodeError::BadFlags(0x80)
        );

        let ok = ResponseBuilder::new(Op::Storage, 0, 9, 1, &[1; 32], 0).finish(5);
        let mut r = ok[4..].to_vec();
        r[8] = 9;
        assert_eq!(
            ResponseRef::decode(&r).unwrap_err(),
            DecodeError::BadStatus(9)
        );
        let mut r = ok[4..].to_vec();
        r[10] = 1;
        assert_eq!(
            ResponseRef::decode(&r).unwrap_err(),
            DecodeError::BadReserved
        );
        let mut b = ResponseBuilder::new(Op::Storage, 0, 9, 1, &[1; 32], 1);
        b.push_storage(ItemStatus::Ok, &[3; 32]);
        let mut r = b.finish(5)[4..].to_vec();
        r[RESPONSE_HEADER] = 3; // item status
        assert_eq!(
            ResponseRef::decode(&r).unwrap_err(),
            DecodeError::BadItemStatus(3)
        );
        // A non-Ok response must not carry items.
        let mut r = error_response(2, 0, 9, Status::Busy, 0)[4..].to_vec();
        r[2] = 1;
        assert_eq!(
            ResponseRef::decode(&r).unwrap_err(),
            DecodeError::ErrorWithItems
        );

        // Proof requests: no flags, zero reserved bytes.
        let mut b = RequestBuilder::new(Op::Proof, 0, 9, &LATEST);
        b.push_proof_target(&[1; 32], &[[2; 32]]);
        let good = b.finish()[4..].to_vec();
        let mut b = good.clone();
        b[1] = FLAG_INLINE_CODE;
        assert_eq!(
            RequestRef::decode(&b).unwrap_err(),
            DecodeError::BadFlags(FLAG_INLINE_CODE)
        );
        let mut b = good.clone();
        b[REQUEST_HEADER + 34] = 1;
        assert_eq!(
            RequestRef::decode(&b).unwrap_err(),
            DecodeError::BadReserved
        );

        // Proof answers: path ≤ 64 nibbles, zero padding nibble, non-empty
        // RLP. Body after the header: u32 count, u8 path_len, path, u32
        // rlp_len, rlp.
        let mut p = ProofBuilder::new(0, 9, 1, &[1; 32], 0);
        p.push_account_node(3, &pack_path(&[1, 2, 3]), &[0xC0]);
        let good = p.finish(0)[4..].to_vec();
        let node = RESPONSE_HEADER + 4;
        assert_eq!(&good[node..node + 3], &[3, 0x12, 0x30]);
        let mut r = good.clone();
        r[node + 2] = 0x31; // padding nibble
        assert_eq!(ResponseRef::decode(&r).unwrap_err(), DecodeError::BadPath);
        let mut r = good.clone();
        r[node] = 65;
        assert_eq!(ResponseRef::decode(&r).unwrap_err(), DecodeError::BadPath);
        let mut r = good.clone();
        r[node + 3..node + 7].copy_from_slice(&0u32.to_le_bytes());
        r.remove(node + 7);
        assert_eq!(ResponseRef::decode(&r).unwrap_err(), DecodeError::EmptyNode);
        // Status 5 is TooLarge, 6 is unknown.
        let r = error_response(4, 0, 9, Status::TooLarge, 0);
        assert_eq!(
            ResponseRef::decode(&r[4..]).unwrap().status,
            Status::TooLarge
        );
        let mut r = r[4..].to_vec();
        r[8] = 6;
        assert_eq!(
            ResponseRef::decode(&r).unwrap_err(),
            DecodeError::BadStatus(6)
        );
    }

    /// Golden bytes: the layout is a contract (PLAN §2.1); this pins every
    /// offset so an accidental reorder fails loudly.
    #[test]
    fn fixture_exact_layout() {
        let mut b = RequestBuilder::new(Op::Storage, 0, 0x0403_0201, &[0xAB; 32]);
        b.push_storage(&[0x11; 20], &[0x22; 32]);
        let f = b.finish();
        assert_eq!(&f[..4], &92u32.to_le_bytes()); // 40 + 52
        assert_eq!(&f[4..12], &[2, 0, 1, 0, 1, 2, 3, 4]);
        assert_eq!(&f[12..44], &[0xAB; 32]);
        assert_eq!(&f[44..64], &[0x11; 20]);
        assert_eq!(&f[64..96], &[0x22; 32]);

        let mut b = ResponseBuilder::new(Op::Account, FLAG_INLINE_CODE, 7, 0x0102, &[0xCD; 32], 2);
        b.push_account(ItemStatus::Ok, 5, &[0x33; 32], &[0x44; 32], &[0x60, 0x00]);
        b.push_account_absent();
        let f = b.finish(0x0A0B);
        let body = &f[4..];
        assert_eq!(
            body.len(),
            RESPONSE_HEADER + (ACCOUNT_RESULT + 4 + 2) + (ACCOUNT_RESULT + 4)
        );
        assert_eq!(&body[..8], &[1, FLAG_INLINE_CODE, 2, 0, 7, 0, 0, 0]);
        assert_eq!(&body[8..12], &[0, 0, 0, 0]); // Ok + reserved
        assert_eq!(&body[12..16], &0x0A0Bu32.to_le_bytes());
        assert_eq!(&body[16..24], &0x0102u64.to_le_bytes());
        assert_eq!(&body[24..56], &[0xCD; 32]);
        let r = &body[56..];
        assert_eq!(r[0], ItemStatus::Ok as u8);
        assert_eq!(&r[1..9], &5u64.to_le_bytes());
        assert_eq!(&r[9..41], &[0x33; 32]);
        assert_eq!(&r[41..73], &[0x44; 32]);
        assert_eq!(&r[73..77], &2u32.to_le_bytes());
        assert_eq!(&r[77..79], &[0x60, 0x00]);
        let absent = &r[79..];
        assert_eq!(absent[0], ItemStatus::Absent as u8);
        assert_eq!(
            &absent[41..73],
            &KECCAK_EMPTY,
            "no code is KECCAK_EMPTY, never zero"
        );
        assert_eq!(&absent[73..77], &0u32.to_le_bytes());

        // Proof (D10): target = hashed address, u16 slot count, 2 reserved
        // bytes, slots; answer = account nodes, then storage tries.
        let mut b = RequestBuilder::new(Op::Proof, 0, 5, &LATEST);
        b.push_proof_target(&[0x11; 32], &[[0x22; 32], [0x33; 32]]);
        b.push_proof_target(&[0x44; 32], &[]);
        let f = b.finish();
        assert_eq!(&f[..4], &(40u32 + 36 + 64 + 36).to_le_bytes());
        assert_eq!(&f[4..12], &[4, 0, 2, 0, 5, 0, 0, 0]);
        let t = &f[44..];
        assert_eq!(&t[..32], &[0x11; 32]);
        assert_eq!(&t[32..36], &[2, 0, 0, 0]);
        assert_eq!(&t[36..68], &[0x22; 32]);
        assert_eq!(&t[68..100], &[0x33; 32]);
        assert_eq!(&t[100..132], &[0x44; 32]);
        assert_eq!(&t[132..136], &[0, 0, 0, 0]);

        let mut p = ProofBuilder::new(0, 5, 0x0102, &[0xCD; 32], 0);
        p.push_account_node(0, &[], &[0xF8, 0x51]);
        p.push_account_node(1, &pack_path(&[0xA]), &[0xC2]);
        p.begin_storage(&[0x11; 32], &[0x55; 32]);
        p.push_storage_node(2, &pack_path(&[0x1, 0x2]), &[0xE1]);
        let f = p.finish(7);
        let body = &f[4..];
        assert_eq!(&body[..8], &[4, 0, 1, 0, 5, 0, 0, 0]); // count = 1 storage trie
        let r = &body[RESPONSE_HEADER..];
        assert_eq!(&r[..4], &2u32.to_le_bytes()); // account nodes
        assert_eq!(&r[4..11], &[0, 2, 0, 0, 0, 0xF8, 0x51]); // root: no path
        assert_eq!(&r[11..18], &[1, 0xA0, 1, 0, 0, 0, 0xC2]); // odd: low nibble 0
        assert_eq!(&r[18..50], &[0x11; 32]);
        assert_eq!(&r[50..82], &[0x55; 32]);
        assert_eq!(&r[82..86], &1u32.to_le_bytes());
        assert_eq!(&r[86..93], &[2, 0x12, 1, 0, 0, 0, 0xE1]);
        assert_eq!(r.len(), 93);

        let e = error_response(9, 0x80, 3, Status::BadRequest, 1);
        assert_eq!(e.len(), 4 + RESPONSE_HEADER);
        let resp = ResponseRef::decode(&e[4..]).unwrap();
        assert_eq!((resp.op, resp.flags, resp.id), (9, 0x80, 3));
        assert_eq!(resp.status, Status::BadRequest);
        assert_eq!(resp.results().count(), 0);
        assert_eq!(peek_prefix(&e[4..]), Some((9, 0x80, 3)));
        assert_eq!(peek_prefix(&e[4..11]), None);
    }

    /// Fast path, backlog under a stalled reader, ordering preserved, guards
    /// released only once written, and the deadline closing a stuck writer.
    #[tokio::test]
    async fn frame_writer_direct_backlog_order_and_deadline() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Guard(Arc<AtomicUsize>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        let mut client = client.await.unwrap();
        let (_rd, wr) = server.into_split();
        let writer = Arc::new(FrameWriter::new(wr));
        let flusher = {
            let w = writer.clone();
            tokio::spawn(async move { w.run(Duration::from_millis(500)).await })
        };
        let released = Arc::new(AtomicUsize::new(0));
        // 64 frames × 256 KiB = 16 MiB: far beyond loopback socket buffers,
        // sent from a plain OS thread while nobody reads.
        let frames: Vec<Vec<u8>> = (0..64u8)
            .map(|i| {
                let mut b = RequestBuilder::new(Op::Code, 0, i as u32, &LATEST);
                b.push_code(&[i; 32]);
                let mut f = b.finish();
                f.resize(f.len() + 256 * 1024, i); // payload padding
                let len = (f.len() - 4) as u32;
                f[..4].copy_from_slice(&len.to_le_bytes());
                f
            })
            .collect();
        let expect = frames.clone();
        {
            let (w, r) = (writer.clone(), released.clone());
            std::thread::spawn(move || {
                for f in frames {
                    w.send(f, Guard(r.clone())).unwrap();
                }
            })
            .join()
            .unwrap();
        }
        assert!(
            released.load(Ordering::SeqCst) < 64,
            "some frames must be queued"
        );
        // Now read everything: complete, in order, all guards released.
        let mut buf = Vec::new();
        for f in &expect {
            assert!(read_body(&mut client, &mut buf, 1 << 20).await.unwrap());
            assert_eq!(buf, f[4..]);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(released.load(Ordering::SeqCst), 64);
        // Stop reading again: the flusher's deadline closes the writer.
        let big = vec![0u8; 16 << 20];
        let _ = writer.send(big, Guard(released.clone()));
        let err = tokio::time::timeout(Duration::from_secs(5), flusher)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert!(writer.is_closed());
        assert!(writer.send(vec![1], Guard(released.clone())).is_err());
        assert_eq!(
            released.load(Ordering::SeqCst),
            66,
            "queued guard dropped on close"
        );
    }

    #[tokio::test]
    async fn read_body_roundtrip_eof_and_oversized() {
        let (mut a, mut b) = tokio::io::duplex(1 << 16);
        let mut req = RequestBuilder::new(Op::Code, 0, 1, &LATEST);
        req.push_code(&[5; 32]);
        let frame = req.finish();
        tokio::io::AsyncWriteExt::write_all(&mut a, &frame)
            .await
            .unwrap();
        drop(a);
        let mut buf = Vec::new();
        assert!(
            read_body(&mut b, &mut buf, MAX_REQUEST_BYTES)
                .await
                .unwrap()
        );
        assert_eq!(buf, frame[4..]);
        assert!(
            !read_body(&mut b, &mut buf, MAX_REQUEST_BYTES)
                .await
                .unwrap()
        );

        let (mut a, mut b) = tokio::io::duplex(64);
        tokio::spawn(async move {
            let len = (MAX_REQUEST_BYTES as u32 + 1).to_le_bytes();
            let _ = tokio::io::AsyncWriteExt::write_all(&mut a, &len).await;
        });
        let err = read_body(&mut b, &mut buf, MAX_REQUEST_BYTES)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
