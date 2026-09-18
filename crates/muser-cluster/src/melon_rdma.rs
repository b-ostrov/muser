//! Receiver half of the MelonDMA bulk lane (`native/melon_rdma/melon_rdma_bulk.c`,
//! compiled by `build.rs` only with the `melon-rdma` feature).
//!
//! The lane carries segment payload bytes only. The producer RDMA-writes each
//! payload into a ring registered here and announces it with a segment header
//! on the mTLS stream; everything that decides whether a payload is accepted —
//! the descriptor sha256, the HMAC seal, replay admission — still travels over
//! TLS and is checked exactly as for an inline payload. Endpoints are exchanged
//! over that same TLS stream, so no queue pair is ever connected to a peer that
//! has not passed mTLS and the leaf pin.

use std::ffi::{c_char, c_int, c_void, CStr, CString};

use crate::config::RdmaReceiverConfigV1;

pub const ENDPOINT_BYTES: usize = 64;

const ROLE_RECEIVER: c_int = 1;
const BULK_OK: c_int = 0;
const BULK_TIMEOUT: c_int = -2;

#[allow(non_camel_case_types)]
enum melon_bulk_t {}

extern "C" {
    fn melon_bulk_create(
        dev_name: *const c_char,
        gid_index: c_int,
        role: c_int,
        ring_bytes: u64,
        roce_local_ip: *const c_char,
        roce_remote_mac: *const c_char,
    ) -> *mut melon_bulk_t;
    fn melon_bulk_local_endpoint(b: *mut melon_bulk_t, out: *mut u8) -> c_int;
    fn melon_bulk_connect(b: *mut melon_bulk_t, remote: *const u8) -> c_int;
    fn melon_bulk_gid_index(b: *const melon_bulk_t) -> c_int;
    fn melon_bulk_max_put(b: *const melon_bulk_t) -> u64;
    fn melon_bulk_accept(
        b: *mut melon_bulk_t,
        index: u32,
        position: u64,
        len: u64,
        data: *mut *const c_void,
        timeout_ms: c_int,
    ) -> c_int;
    fn melon_bulk_release(b: *mut melon_bulk_t) -> c_int;
    fn melon_bulk_close(b: *mut melon_bulk_t);
    fn melon_bulk_last_error() -> *const c_char;
}

fn last_error() -> String {
    // Per-thread in the C side, read on the thread that just failed.
    unsafe {
        let ptr = melon_bulk_last_error();
        if ptr.is_null() {
            "unknown melon bulk error".to_string()
        } else {
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

/// One handoff's receiving end of the bulk lane.
pub struct BulkReceiver {
    handle: *mut melon_bulk_t,
    timeout_ms: c_int,
}

// The C state is touched only through `&mut self`.
unsafe impl Send for BulkReceiver {}

impl BulkReceiver {
    /// Opens the device, programs this process's RoCE slot from the configured
    /// profile, and registers the ring. Not yet connected.
    pub fn create(config: &RdmaReceiverConfigV1) -> Result<Self, String> {
        let device = CString::new(config.device.as_str())
            .map_err(|_| "RDMA device name contains a NUL".to_string())?;
        let local_ip = config
            .local_address
            .as_deref()
            .map(CString::new)
            .transpose()
            .map_err(|_| "RoCE local address contains a NUL".to_string())?;
        let peer_mac = config
            .peer_mac
            .as_deref()
            .map(CString::new)
            .transpose()
            .map_err(|_| "RoCE peer MAC contains a NUL".to_string())?;
        let handle = unsafe {
            melon_bulk_create(
                device.as_ptr(),
                config.gid_index,
                ROLE_RECEIVER,
                config.ring_bytes(),
                local_ip.as_ref().map_or(std::ptr::null(), |value| value.as_ptr()),
                peer_mac.as_ref().map_or(std::ptr::null(), |value| value.as_ptr()),
            )
        };
        if handle.is_null() {
            return Err(format!(
                "RDMA bulk receiver on {}: {}",
                config.device,
                last_error()
            ));
        }
        Ok(Self {
            handle,
            timeout_ms: 30_000,
        })
    }

    pub fn endpoint(&mut self) -> [u8; ENDPOINT_BYTES] {
        let mut out = [0u8; ENDPOINT_BYTES];
        unsafe { melon_bulk_local_endpoint(self.handle, out.as_mut_ptr()) };
        out
    }

    pub fn connect(&mut self, sender_endpoint: &[u8]) -> Result<(), String> {
        if sender_endpoint.len() != ENDPOINT_BYTES {
            return Err(format!(
                "RDMA bulk endpoint is {} bytes, not {ENDPOINT_BYTES}",
                sender_endpoint.len()
            ));
        }
        if unsafe { melon_bulk_connect(self.handle, sender_endpoint.as_ptr()) } != BULK_OK {
            return Err(format!("RDMA bulk connect: {}", last_error()));
        }
        Ok(())
    }

    pub fn gid_index(&self) -> i32 {
        unsafe { melon_bulk_gid_index(self.handle) }
    }

    pub fn max_put(&self) -> u64 {
        unsafe { melon_bulk_max_put(self.handle) }
    }

    /// How long one payload may take to land after its header arrives. The
    /// header is only written once the producer has posted the write, so in
    /// practice this is microseconds; the bound exists so a peer that
    /// announces a payload it never sends fails the transfer instead of
    /// hanging it.
    pub fn set_timeout(&mut self, timeout: std::time::Duration) {
        self.timeout_ms = timeout.as_millis().min(i32::MAX as u128) as c_int;
    }

    /// Waits for put `index`, copies it out of the ring and gives its space
    /// back. The copy is not an optimisation to remove: until release the
    /// peer can still write those ring bytes, so anything verified in place
    /// could change between the sha256 check and the install.
    pub fn take(&mut self, index: u32, position: u64, length: u64) -> Result<Vec<u8>, String> {
        let len = usize::try_from(length)
            .map_err(|_| "bulk payload length exceeds the platform".to_string())?;
        let mut data: *const c_void = std::ptr::null();
        let rc = unsafe {
            melon_bulk_accept(
                self.handle,
                index,
                position,
                length,
                &mut data,
                self.timeout_ms,
            )
        };
        if rc == BULK_TIMEOUT {
            return Err(format!("RDMA bulk payload {index} never landed: {}", last_error()));
        }
        if rc != BULK_OK || data.is_null() {
            return Err(format!("RDMA bulk payload {index}: {}", last_error()));
        }
        let mut payload = Vec::with_capacity(len);
        unsafe {
            std::ptr::copy_nonoverlapping(data.cast::<u8>(), payload.as_mut_ptr(), len);
            payload.set_len(len);
        }
        if unsafe { melon_bulk_release(self.handle) } != BULK_OK {
            return Err(format!("RDMA bulk release {index}: {}", last_error()));
        }
        Ok(payload)
    }
}

impl Drop for BulkReceiver {
    fn drop(&mut self) {
        unsafe { melon_bulk_close(self.handle) };
    }
}

impl crate::producer::BulkPayloadSource for BulkReceiver {
    fn take(&mut self, bulk: &crate::transport::BulkRefV2) -> Result<Vec<u8>, String> {
        BulkReceiver::take(self, bulk.index, bulk.position, bulk.length)
    }
}
