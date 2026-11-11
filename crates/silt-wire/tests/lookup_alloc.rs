//! D9: decoding lookup frames never allocates. A counting global allocator
//! (this test binary only) watches decode + full iteration of a request
//! and a response.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use silt_wire::lookup::{
    FLAG_INLINE_CODE, Item, ItemStatus, LATEST, Op, RequestBuilder, RequestRef, ResponseBuilder,
    ResponseRef, ResultRef,
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
