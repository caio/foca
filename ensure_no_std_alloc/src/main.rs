#![no_std]
#![no_main]
#![feature(alloc_error_handler)]

extern crate alloc;

use core::{
    alloc::{GlobalAlloc, Layout},
    panic::PanicInfo,
};

#[allow(unused_imports)]
use foca;

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

struct Allocator;
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, _: Layout) -> *mut u8 {
        todo!()
    }

    unsafe fn dealloc(&self, _: *mut u8, _: Layout) {
        todo!()
    }
}

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}

#[no_mangle]
pub extern "C" fn _start() -> ! {
    loop {}
}

#[alloc_error_handler]
fn alloc_fail(_: Layout) -> ! {
    todo!();
}
