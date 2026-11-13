//! D9/D10: decoding lookup frames never allocates. A counting global
//! allocator (this test binary only) watches decode + full iteration of
//! requests and responses, proofs included.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use silt_wire::lookup::{
    FLAG_INLINE_CODE, Item, ItemStatus, LATEST, Op, ProofBuilder, RequestBuilder, RequestRef,
    ResponseBuilder, ResponseRef, ResultRef, pack_path,
};

struct Counting;

thread_local! {
    // const-initialized: touching it never allocates.
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.with(|n| n.set(n.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocs_during<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOCS.with(Cell::get);
    let out = f();
    (out, ALLOCS.with(Cell::get) - before)
}

#[test]
fn request_and_response_decoding_do_not_allocate() {
    let mut req = RequestBuilder::new(Op::Storage, 0, 42, &LATEST);
    for i in 0..256u32 {
        let mut slot = [0u8; 32];
        slot[28..].copy_from_slice(&i.to_be_bytes());
        req.push_storage(&[7; 20], &slot);
    }
    let frame = req.finish();

    let (sum, allocs) = allocs_during(|| {
        let req = RequestRef::decode(&frame[4..]).unwrap();
        req.items()
            .map(|item| match item {
                Item::Storage(addr, slot) => addr[0] as u64 + slot[31] as u64,
                _ => unreachable!(),
            })
            .sum::<u64>()
    });
    assert_eq!(allocs, 0, "request decode allocated");
    assert!(sum > 0);

    let mut resp = ResponseBuilder::new(Op::Account, FLAG_INLINE_CODE, 42, 1, &[1; 32], 64);
    for i in 0..64u8 {
        resp.push_account(ItemStatus::Ok, i as u64, &[i; 32], &[i; 32], &[0x60, i]);
    }
    let frame = resp.finish(10);

    let (nonces, allocs) = allocs_during(|| {
        let resp = ResponseRef::decode(&frame[4..]).unwrap();
        resp.results()
            .map(|r| match r {
                ResultRef::Account { nonce, code, .. } => nonce + code.unwrap().len() as u64,
                _ => unreachable!(),
            })
            .sum::<u64>()
    });
    assert_eq!(allocs, 0, "response decode allocated");
    assert_eq!(nonces, (0..64).sum::<u64>() + 128);
}

#[test]
fn proof_request_and_response_decoding_do_not_allocate() {
    let mut req = RequestBuilder::new(Op::Proof, 0, 42, &LATEST);
    for i in 0..64u8 {
        req.push_proof_target(&[i; 32], &vec![[i; 32]; i as usize % 5]);
    }
    let frame = req.finish();
    let (slots, allocs) = allocs_during(|| {
        let req = RequestRef::decode(&frame[4..]).unwrap();
        req.items()
            .map(|item| match item {
                Item::Proof(t) => t.slots().map(|s| s[0] as usize).sum::<usize>() + 1,
                _ => unreachable!(),
            })
            .sum::<usize>()
    });
    assert_eq!(allocs, 0, "proof request decode allocated");
    assert!(slots > 64);

    let mut p = ProofBuilder::new(0, 42, 1, &[1; 32], 0);
    let paths: Vec<Vec<u8>> = (0..65u8).map(|n| vec![n % 16; n as usize]).collect();
    for path in &paths {
        p.push_account_node(path.len(), &pack_path(path), &[0xC0, path.len() as u8]);
    }
    for i in 0..8u8 {
        p.begin_storage(&[i; 32], &[i; 32]);
        for path in &paths[..9] {
            p.push_storage_node(path.len(), &pack_path(path), &[0x80 + i]);
        }
    }
    let frame = p.finish(10);
    let (total, allocs) = allocs_during(|| {
        let resp = ResponseRef::decode(&frame[4..]).unwrap();
        let proof = resp.proof().unwrap();
        let nibbles =
            |n: silt_wire::lookup::NodeRef<'_>| n.nibbles().map(usize::from).sum::<usize>();
        let account: usize = proof.account_nodes().map(nibbles).sum();
        let storage: usize = proof
            .storages()
            .map(|s| s.nodes.map(|n| n.rlp.len() + nibbles(n)).sum::<usize>())
            .sum();
        account + storage
    });
    assert_eq!(allocs, 0, "proof response decode allocated");
    assert!(total > 0);
}
