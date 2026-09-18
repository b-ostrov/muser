/* melon_rdma_bulk.h — one-directional RDMA bulk lane for Handoff V2 payloads.
 *
 * The control conversation (Begin, segment headers, Seal, Ack) stays on the
 * existing mTLS stream. Only segment payload bytes move here: the sender
 * RDMA-WRITEs each payload into a ring the receiver registered, and the
 * receiver learns of it through the immediate data on the same operation.
 * Integrity does not depend on this lane: every payload is bound by the
 * descriptor sha256 and the HMAC seal that travel over TLS, and the receiver
 * copies a payload out of the ring before verifying it, so a peer rewriting
 * the ring can only make verification fail.
 *
 * Endpoints are exchanged as opaque MELON_BULK_ENDPOINT_BYTES blobs by the
 * caller — over the already-authenticated TLS stream, so no RDMA state is
 * ever set up with a peer that has not passed mTLS and the leaf pin.
 *
 * Flow control is credit-based and cannot overrun either side: the sender
 * never has more bytes in flight than the ring holds, nor more operations in
 * flight than the receiver has receive WQEs posted, and the receiver returns
 * both counters with every release.
 *
 * Every blocking call takes a timeout in milliseconds (negative waits
 * forever) and waits by polling briefly and then sleeping, so an idle lane
 * does not hold a core for the length of a prefill.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct melon_bulk melon_bulk_t;

#define MELON_BULK_ENDPOINT_BYTES 64

enum melon_bulk_role {
    MELON_BULK_RECEIVER = 1,
    MELON_BULK_SENDER = 2,
};

/* Return codes. Every failure also sets melon_bulk_last_error(). */
#define MELON_BULK_OK 0
#define MELON_BULK_ERROR (-1)
#define MELON_BULK_TIMEOUT (-2)

/* Opens `dev_name`, resolves a readable GID (trying `gid_index` first; see
 * the .c file for why a fixed index is not enough on MelonDMA), and creates
 * the queue pair. A receiver also allocates and registers its ring of
 * `ring_bytes`; a sender ignores `ring_bytes` and sizes its staging area from
 * the receiver's endpoint at connect time.
 *
 * `roce_local_ip` / `roce_remote_mac` are MelonDMA's RoCE profile: the
 * address this end answers on over the RDMA link, and the peer's MAC on it.
 * The DriverKit extension owns the port and there is no system ARP for that
 * link, so the provider cannot learn either on its own; without them it
 * programs no GID slot and nothing works. The local MAC is derived from the
 * card's node GUID. Pass NULL for both to fall back to MELONDMA_LOCAL_IP /
 * MELONDMA_LOCAL_MAC / MELONDMA_REMOTE_MAC in the environment. Ignored on
 * Linux, where the kernel owns the GID table. */
melon_bulk_t *melon_bulk_create(const char *dev_name, int gid_index, int role,
                                uint64_t ring_bytes, const char *roce_local_ip,
                                const char *roce_remote_mac);

/* Serializes this side's endpoint into `out`. */
int melon_bulk_local_endpoint(melon_bulk_t *b, uint8_t out[MELON_BULK_ENDPOINT_BYTES]);

/* Validates the peer's endpoint, brings the queue pair to RTS and, on the
 * sender, registers a staging area the size of the peer's ring. */
int melon_bulk_connect(melon_bulk_t *b, const uint8_t remote[MELON_BULK_ENDPOINT_BYTES]);

/* The GID index actually in use after create (it may differ from the one
 * requested), for logs and receipts. */
int melon_bulk_gid_index(const melon_bulk_t *b);

/* Largest payload one put may carry: half the ring, so a put that has to
 * skip the ring's tail can always fit once the receiver catches up. */
uint64_t melon_bulk_max_put(const melon_bulk_t *b);

/* Sender. Waits for credit, copies `len` bytes into staging and posts one
 * RDMA WRITE WITH IMMEDIATE. Returns without waiting for the write to
 * complete. `*out_index` / `*out_position` identify the put for the control
 * frame that announces it. */
int melon_bulk_put(melon_bulk_t *b, const void *data, uint64_t len, uint32_t *out_index,
                   uint64_t *out_position, int timeout_ms);

/* Sender. Waits until every posted write has completed locally. */
int melon_bulk_flush(melon_bulk_t *b, int timeout_ms);

/* Sender. Time the link had this lane's bytes in flight: the union of
 * [post, completion] over every completed write. `*hardware` says whether the
 * completions carried NIC timestamps (exact) or were stamped when reaped
 * (late, so the time overstates and any rate derived from it understates).
 * Call after melon_bulk_flush() for a whole transfer's figure. */
uint64_t melon_bulk_wire_ns(const melon_bulk_t *b, int *hardware);

/* Receiver. Waits for put `index` — which must be the next one — to land,
 * checks that `position`/`len` are exactly where the ring discipline says it
 * must be, and returns a pointer into the ring. The bytes stay the peer's to
 * overwrite until melon_bulk_release(), so copy them before trusting them. */
int melon_bulk_accept(melon_bulk_t *b, uint32_t index, uint64_t position, uint64_t len,
                      const void **data, int timeout_ms);

/* Receiver. Returns the most recently accepted put's space to the sender. */
int melon_bulk_release(melon_bulk_t *b);

void melon_bulk_close(melon_bulk_t *b);

/* Per-thread. */
const char *melon_bulk_last_error(void);

#ifdef __cplusplus
}
#endif
