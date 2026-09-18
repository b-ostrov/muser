/* melon_rdma_bulk.c — see melon_rdma_bulk.h.
 *
 * Ring discipline, shared by both sides so each can check the other:
 *   positions are monotonic u64 byte counters; a put of `len` starting at
 *   logical position `pos` occupies ring[pos % ring, pos % ring + len). If
 *   that would run past the end of the ring, the put starts at the next ring
 *   boundary instead and the skipped tail counts as consumed. The receiver
 *   recomputes every position independently and refuses a put that is not
 *   exactly where this rule puts it.
 *
 * Credit: the receiver's release sends {tail, released} — the logical
 * position everything before which is consumed, and the count of puts
 * released. The sender admits a put only if it fits in the ring behind
 * `tail` and leaves the receiver at least one posted receive WQE, since every
 * WRITE WITH IMMEDIATE consumes one.
 */
#include "melon_rdma_bulk.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <infiniband/verbs.h>

#define BULK_MAGIC 0x4d424b31u /* "MBK1" */
#define BULK_IB_PORT 1
/* Receive WQEs the receiver keeps posted for WRITE WITH IMMEDIATE. The sender
 * bounds operations in flight by this, so it is also the deepest pipeline a
 * run of small puts can build. */
#define BULK_IMM_RECV_DEPTH 256
/* Credit messages in flight each way. Releases are one per put, so this only
 * has to cover the receiver getting ahead of its own send completions. */
#define BULK_CREDIT_DEPTH 64
#define BULK_SEND_DEPTH 256
#define BULK_MIN_RING (1u << 20)
#define BULK_MAX_RING (1ull << 30)

static _Thread_local char g_error[512];

static void set_error(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_error, sizeof(g_error), fmt, ap);
    va_end(ap);
}

const char *melon_bulk_last_error(void) { return g_error; }

struct credit_wire {
    uint64_t tail;
    uint64_t released;
};

struct endpoint_wire {
    uint32_t magic;
    uint32_t qpn;
    uint32_t psn;
    uint8_t role;
    uint8_t mtu;
    uint16_t imm_recv_depth;
    uint8_t gid[16];
    uint64_t ring_addr;
    uint64_t ring_bytes;
    uint32_t ring_rkey;
    uint8_t reserved[12];
} __attribute__((packed));

_Static_assert(sizeof(struct endpoint_wire) == MELON_BULK_ENDPOINT_BYTES,
               "endpoint wire layout drifted");

/* Big-endian u64 through untyped pointers: the wire structs are packed, and
 * a packed field must never be addressed as a uint64_t*. */
static void put_be64(void *field, uint64_t v) {
    uint8_t *p = field;
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (56 - 8 * i));
}

static uint64_t get_be64(const void *field) {
    const uint8_t *p = field;
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = (v << 8) | p[i];
    return v;
}

struct melon_bulk {
    int role;
    int gid_index;
    uint8_t port;
    enum ibv_mtu mtu;
    union ibv_gid gid;
    int connected;
    int broken;

    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *send_cq;
    struct ibv_cq *recv_cq;
    struct ibv_qp *qp;
    uint32_t psn;

    /* Receiver: the ring. Sender: staging mirroring the peer's ring. */
    uint8_t *ring;
    uint64_t ring_bytes;
    struct ibv_mr *ring_mr;

    /* Credit messages: the receiver sends from tx, the sender lands in rx. */
    struct credit_wire *credit_tx;
    struct ibv_mr *credit_tx_mr;
    unsigned credit_tx_next;
    struct credit_wire *credit_rx;
    struct ibv_mr *credit_rx_mr;

    /* Receiver: a small landing buffer the immediate receives point at. */
    uint64_t *imm_sink;
    struct ibv_mr *imm_sink_mr;

    /* Peer (sender only). */
    uint64_t remote_addr;
    uint32_t remote_rkey;
    uint32_t remote_recv_depth;

    /* Sender state. */
    uint64_t head;          /* next logical position */
    uint32_t next_index;    /* next put index */
    uint64_t credit_tail;   /* receiver's consumed position */
    uint64_t credit_released;
    unsigned sends_outstanding;

    /* Sender wire time: the union of [post, completion] over every write,
     * which is the time the link actually had this lane's bytes in flight.
     * Stamps are HCA clock cycles when the NIC timestamps completions, or
     * CLOCK_MONOTONIC ns taken at reap time otherwise — late, so the busy
     * time it yields can only overstate, and the rate only understate. */
    int hw_stamps;
    uint64_t hca_khz;
#ifndef __APPLE__
    struct ibv_cq_ex *send_cq_ex;
#endif
    uint64_t post_stamp[BULK_SEND_DEPTH];
    int busy_open;
    uint64_t busy_start;
    uint64_t busy_end;
    uint64_t busy_total;

    /* Receiver state. */
    uint64_t expect_position;
    uint32_t expect_index;
    uint32_t landed;        /* completions polled but not yet accepted */
    uint32_t landed_imm[BULK_IMM_RECV_DEPTH];
    uint32_t landed_head;
    int have_accepted;
    uint64_t accepted_end;
    uint64_t released;
};

/* --- time ---------------------------------------------------------------- */

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* Waits in the shape a handoff actually needs: a producer mid-prefill can be
 * silent for minutes, and a busy poll across that holds a whole core. Spin
 * for the first 100 us — the case where the next put is already on the wire
 * — then sleep, backing off to 1 ms. */
struct waiter {
    uint64_t started;
    uint64_t deadline; /* 0 = forever */
    unsigned sleep_us;
};

static void waiter_init(struct waiter *w, int timeout_ms) {
    w->started = now_ns();
    w->deadline = timeout_ms < 0 ? 0 : w->started + (uint64_t)timeout_ms * 1000000ull;
    w->sleep_us = 0;
}

/* Returns 0 to keep waiting, MELON_BULK_TIMEOUT once the deadline passes. */
static int waiter_pause(struct waiter *w) {
    uint64_t now = now_ns();
    if (w->deadline && now >= w->deadline) return MELON_BULK_TIMEOUT;
    if (now - w->started < 100000ull) return 0;
    w->sleep_us = w->sleep_us ? (w->sleep_us * 2 > 1000 ? 1000 : w->sleep_us * 2) : 20;
    usleep(w->sleep_us);
    return 0;
}

/* --- ring reuse ---------------------------------------------------------- */

/* Every handoff is its own connection, and an RC queue pair cannot outlive
 * one — but the ring can. Keeping one released ring per process means the
 * next handoff registers the same pages again, which MelonDMA's verbs_compat
 * answers from its MR cache as a lease rather than a fresh pin (64 MiB costs
 * ~20 ms to pin cold on driver 0.547). Only one ring is kept: a receiver runs
 * one handoff at a time. */
static pthread_mutex_t g_stash_lock = PTHREAD_MUTEX_INITIALIZER;
static uint8_t *g_stash;
static uint64_t g_stash_bytes;

static uint8_t *stash_take(uint64_t bytes) {
    uint8_t *ring = NULL;
    pthread_mutex_lock(&g_stash_lock);
    if (g_stash && g_stash_bytes == bytes) {
        ring = g_stash;
        g_stash = NULL;
    }
    pthread_mutex_unlock(&g_stash_lock);
    if (ring) return ring;
    long page = sysconf(_SC_PAGESIZE);
    if (posix_memalign((void **)&ring, (size_t)page, bytes) != 0) return NULL;
    memset(ring, 0, bytes);
    return ring;
}

static void stash_give(uint8_t *ring, uint64_t bytes) {
    if (!ring) return;
    pthread_mutex_lock(&g_stash_lock);
    uint8_t *old = g_stash;
    g_stash = ring;
    g_stash_bytes = bytes;
    pthread_mutex_unlock(&g_stash_lock);
    free(old);
}

/* --- device -------------------------------------------------------------- */

/* A fixed GID index is only correct on a provider whose GID table belongs to
 * the port. MelonDMA's does not: it programs one RoCE slot per client from
 * MELONDMA_LOCAL_IP/_LOCAL_MAC/_REMOTE_MAC and answers ibv_query_gid only for
 * the slot that client owns, handing every further client the next free one.
 * Try the configured index, and only if the provider will not answer for it,
 * take the slot this process does own. Never scan first: on a Linux port
 * every index answers and index 0 is the link-local RoCE v1 GID, which would
 * put the two ends on different RoCE versions without a word.
 *
 * ibv_query_gid returns an errno value and does not set errno. */
static int resolve_gid(melon_bulk_t *b, const struct ibv_port_attr *pa) {
    int rc = -1;
    if (b->gid_index >= 0) {
        rc = ibv_query_gid(b->ctx, b->port, b->gid_index, &b->gid);
        if (rc == 0) return 0;
    }
#ifndef __APPLE__
    /* On Linux the kernel owns the table and every index answers, so there is
     * no "slot this process owns" to find — only the wrong GID to pick. The
     * index has to come from configuration (`muser node add` reads it from
     * sysfs as the RoCE v2 entry for the cabled address). */
    set_error("GID index %d is not usable (%s); on Linux the RoCE v2 index for the "
              "RDMA link address must be configured explicitly",
              b->gid_index, rc > 0 ? strerror(rc) : "none given");
    (void)pa;
    return -1;
#endif
    int table_len = pa->gid_tbl_len;
    if (table_len <= 0 || table_len > 256) table_len = 256;
    for (int i = 0; i < table_len; i++) {
        if (i == b->gid_index) continue;
        if (ibv_query_gid(b->ctx, b->port, i, &b->gid) != 0) continue;
        if (b->gid_index >= 0)
            fprintf(stderr,
                    "melon_bulk: GID index %d is not readable by this process; "
                    "using index %d, the slot it owns\n", b->gid_index, i);
        b->gid_index = i;
        return 0;
    }
    set_error("no readable GID (requested index %d: %s) in a %d-entry table; on macOS "
              "this is what missing MELONDMA_LOCAL_IP/MELONDMA_LOCAL_MAC/"
              "MELONDMA_REMOTE_MAC looks like — the provider programs no slot for a "
              "client that did not configure RoCE",
              b->gid_index, rc > 0 ? strerror(rc) : "not tried", table_len);
    return -1;
}

static int open_device(melon_bulk_t *b, const char *dev_name) {
    int n = 0;
    struct ibv_device **list = ibv_get_device_list(&n);
    if (!list) {
        set_error("ibv_get_device_list failed: %s", strerror(errno));
        return -1;
    }
    struct ibv_device *dev = NULL;
    for (int i = 0; i < n; i++)
        if (strcmp(ibv_get_device_name(list[i]), dev_name) == 0) dev = list[i];
    if (!dev) {
        set_error("RDMA device '%s' not found (%d present)", dev_name, n);
        ibv_free_device_list(list);
        return -1;
    }
    b->ctx = ibv_open_device(dev);
    ibv_free_device_list(list);
    if (!b->ctx) {
        set_error("ibv_open_device('%s') failed: %s", dev_name, strerror(errno));
        return -1;
    }
    struct ibv_port_attr pa;
    if (ibv_query_port(b->ctx, b->port, &pa) != 0) {
        set_error("ibv_query_port failed: %s", strerror(errno));
        return -1;
    }
    if (pa.state != IBV_PORT_ACTIVE) {
        set_error("RDMA port %u on '%s' is not active (state %d)", b->port, dev_name,
                  (int)pa.state);
        return -1;
    }
    b->mtu = pa.active_mtu;
    return 0;
}

#ifdef __APPLE__
static int parse_mac(const char *text, uint8_t out[6]) {
    unsigned v[6];
    char tail;
    if (sscanf(text, "%x:%x:%x:%x:%x:%x%c", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &tail) != 6)
        return -1;
    for (int i = 0; i < 6; i++) {
        if (v[i] > 0xff) return -1;
        out[i] = (uint8_t)v[i];
    }
    return 0;
}

/* An mlx5 node GUID is the port MAC with two bytes spliced into the middle —
 * 03:00 on ConnectX, ff:fe in the EUI-64 convention. Anything else is not a
 * GUID this can be read back out of. */
static int mac_from_node_guid(melon_bulk_t *b, uint8_t out[6]) {
    struct ibv_device_attr attr;
    if (ibv_query_device(b->ctx, &attr) != 0) return -1;
    const uint8_t *g = (const uint8_t *)&attr.node_guid;
    int spliced = (g[3] == 0x03 && g[4] == 0x00) || (g[3] == 0xff && g[4] == 0xfe);
    if (!spliced) return -1;
    out[0] = g[0];
    out[1] = g[1];
    out[2] = g[2];
    out[3] = g[5];
    out[4] = g[6];
    out[5] = g[7];
    return 0;
}

static int configure_roce(melon_bulk_t *b, const char *local_ip, const char *remote_mac) {
    struct ibv_mlx5_roce_config c;
    memset(&c, 0, sizeof(c));
    c.hop_limit = 1;
    struct in_addr v4;
    if (inet_pton(AF_INET, local_ip, &v4) == 1) {
        c.local_gid.raw[10] = 0xff;
        c.local_gid.raw[11] = 0xff;
        memcpy(&c.local_gid.raw[12], &v4, 4);
        c.l3_type = 0;
    } else if (inet_pton(AF_INET6, local_ip, c.local_gid.raw) == 1) {
        c.l3_type = 1;
    } else {
        set_error("RoCE local address '%s' is neither IPv4 nor IPv6", local_ip);
        return -1;
    }
    if (parse_mac(remote_mac, c.peer_mac) != 0) {
        set_error("RoCE peer MAC '%s' is not aa:bb:cc:dd:ee:ff", remote_mac);
        return -1;
    }
    if (mac_from_node_guid(b, c.local_mac) != 0) {
        set_error("cannot derive this card's MAC from its node GUID; set "
                  "MELONDMA_LOCAL_MAC and use the environment profile instead");
        return -1;
    }
    int rc = ibv_mlx5_configure_roce(b->ctx, &c);
    if (rc != 0) {
        set_error("ibv_mlx5_configure_roce(%s) failed: %s", local_ip, strerror(rc));
        return -1;
    }
    /* The provider just handed this context its own slot; whatever index was
     * configured belongs to some other process now. */
    b->gid_index = -1;
    return 0;
}
#endif

static struct ibv_mr *reg(melon_bulk_t *b, void *addr, size_t len, int access,
                          const char *what) {
    struct ibv_mr *mr = ibv_reg_mr(b->pd, addr, len, access);
    if (!mr)
        set_error("ibv_reg_mr(%s, %zu bytes) failed: %s%s", what, len, strerror(errno),
                  errno == ENOMEM ? " — inside a container this is almost always "
                                    "RLIMIT_MEMLOCK; run it with --ulimit memlock=-1"
                                  : "");
    return mr;
}

static int post_imm_recv(melon_bulk_t *b, uint64_t slot) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)&b->imm_sink[slot % BULK_IMM_RECV_DEPTH],
        .length = sizeof(uint64_t),
        .lkey = b->imm_sink_mr->lkey,
    };
    struct ibv_recv_wr wr = {.wr_id = slot, .sg_list = &sge, .num_sge = 1};
    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(b->qp, &wr, &bad) != 0) {
        set_error("ibv_post_recv (immediate) failed: %s", strerror(errno));
        return -1;
    }
    return 0;
}

static int post_credit_recv(melon_bulk_t *b, unsigned slot) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)&b->credit_rx[slot],
        .length = sizeof(struct credit_wire),
        .lkey = b->credit_rx_mr->lkey,
    };
    struct ibv_recv_wr wr = {.wr_id = slot, .sg_list = &sge, .num_sge = 1};
    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(b->qp, &wr, &bad) != 0) {
        set_error("ibv_post_recv (credit) failed: %s", strerror(errno));
        return -1;
    }
    return 0;
}

melon_bulk_t *melon_bulk_create(const char *dev_name, int gid_index, int role,
                                uint64_t ring_bytes, const char *roce_local_ip,
                                const char *roce_remote_mac) {
    if (role != MELON_BULK_RECEIVER && role != MELON_BULK_SENDER) {
        set_error("unknown bulk role %d", role);
        return NULL;
    }
    melon_bulk_t *b = calloc(1, sizeof(*b));
    if (!b) {
        set_error("out of memory");
        return NULL;
    }
    b->role = role;
    b->gid_index = gid_index;
    b->port = BULK_IB_PORT;
    if (open_device(b, dev_name) != 0) goto fail;
#ifdef __APPLE__
    if (roce_local_ip || roce_remote_mac) {
        if (!roce_local_ip || !roce_remote_mac) {
            set_error("a RoCE profile needs both the local address and the peer MAC");
            goto fail;
        }
        if (configure_roce(b, roce_local_ip, roce_remote_mac) != 0) goto fail;
    }
#else
    (void)roce_local_ip;
    (void)roce_remote_mac;
#endif
    {
        struct ibv_port_attr pa;
        if (ibv_query_port(b->ctx, b->port, &pa) != 0) {
            set_error("ibv_query_port failed: %s", strerror(errno));
            goto fail;
        }
        if (resolve_gid(b, &pa) != 0) goto fail;
    }

    b->pd = ibv_alloc_pd(b->ctx);
    if (!b->pd) {
        set_error("ibv_alloc_pd failed: %s", strerror(errno));
        goto fail;
    }
#ifndef __APPLE__
    if (role == MELON_BULK_SENDER) {
        struct ibv_device_attr_ex attr;
        memset(&attr, 0, sizeof(attr));
        if (ibv_query_device_ex(b->ctx, NULL, &attr) == 0 && attr.hca_core_clock &&
            attr.completion_timestamp_mask) {
            struct ibv_cq_init_attr_ex cq_attr = {
                .cqe = BULK_SEND_DEPTH + BULK_CREDIT_DEPTH,
                .wc_flags = IBV_WC_EX_WITH_COMPLETION_TIMESTAMP,
            };
            b->send_cq_ex = ibv_create_cq_ex(b->ctx, &cq_attr);
            if (b->send_cq_ex) {
                b->send_cq = ibv_cq_ex_to_cq(b->send_cq_ex);
                b->hw_stamps = 1;
                b->hca_khz = attr.hca_core_clock;
            }
        }
    }
#endif
    if (!b->send_cq)
        b->send_cq = ibv_create_cq(b->ctx, BULK_SEND_DEPTH + BULK_CREDIT_DEPTH, NULL, NULL, 0);
    b->recv_cq = ibv_create_cq(b->ctx, BULK_IMM_RECV_DEPTH + BULK_CREDIT_DEPTH, NULL, NULL, 0);
    if (!b->send_cq || !b->recv_cq) {
        set_error("ibv_create_cq failed: %s", strerror(errno));
        goto fail;
    }
    struct ibv_qp_init_attr init = {
        .send_cq = b->send_cq,
        .recv_cq = b->recv_cq,
        .qp_type = IBV_QPT_RC,
        .cap = {
            .max_send_wr = role == MELON_BULK_SENDER ? BULK_SEND_DEPTH : BULK_CREDIT_DEPTH,
            .max_recv_wr = role == MELON_BULK_RECEIVER ? BULK_IMM_RECV_DEPTH : BULK_CREDIT_DEPTH,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
    };
    b->qp = ibv_create_qp(b->pd, &init);
    if (!b->qp) {
        set_error("ibv_create_qp failed: %s", strerror(errno));
        goto fail;
    }
    b->psn = b->qp->qp_num & 0xffffff;

    if (role == MELON_BULK_RECEIVER) {
        if (ring_bytes < BULK_MIN_RING || ring_bytes > BULK_MAX_RING) {
            set_error("ring of %llu bytes is outside [%u, %llu]",
                      (unsigned long long)ring_bytes, BULK_MIN_RING,
                      (unsigned long long)BULK_MAX_RING);
            goto fail;
        }
        long page = sysconf(_SC_PAGESIZE);
        ring_bytes = (ring_bytes + (uint64_t)page - 1) & ~((uint64_t)page - 1);
        b->ring = stash_take(ring_bytes);
        if (!b->ring) {
            set_error("could not allocate a %llu-byte ring", (unsigned long long)ring_bytes);
            goto fail;
        }
        b->ring_bytes = ring_bytes;
        b->ring_mr = reg(b, b->ring, ring_bytes,
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE, "ring");
        if (!b->ring_mr) goto fail;
        b->imm_sink = calloc(BULK_IMM_RECV_DEPTH, sizeof(uint64_t));
        b->credit_tx = calloc(BULK_CREDIT_DEPTH, sizeof(struct credit_wire));
        if (!b->imm_sink || !b->credit_tx) {
            set_error("out of memory");
            goto fail;
        }
        b->imm_sink_mr = reg(b, b->imm_sink, BULK_IMM_RECV_DEPTH * sizeof(uint64_t),
                             IBV_ACCESS_LOCAL_WRITE, "immediate sink");
        b->credit_tx_mr = reg(b, b->credit_tx, BULK_CREDIT_DEPTH * sizeof(struct credit_wire),
                              IBV_ACCESS_LOCAL_WRITE, "credit tx");
        if (!b->imm_sink_mr || !b->credit_tx_mr) goto fail;
    } else {
        b->credit_rx = calloc(BULK_CREDIT_DEPTH, sizeof(struct credit_wire));
        if (!b->credit_rx) {
            set_error("out of memory");
            goto fail;
        }
        b->credit_rx_mr = reg(b, b->credit_rx, BULK_CREDIT_DEPTH * sizeof(struct credit_wire),
                              IBV_ACCESS_LOCAL_WRITE, "credit rx");
        if (!b->credit_rx_mr) goto fail;
    }

    struct ibv_qp_attr a = {
        .qp_state = IBV_QPS_INIT,
        .port_num = b->port,
        .pkey_index = 0,
        .qp_access_flags = role == MELON_BULK_RECEIVER
                               ? (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE)
                               : IBV_ACCESS_LOCAL_WRITE,
    };
    int rc = ibv_modify_qp(b->qp, &a,
                           IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (rc != 0) {
        set_error("RESET->INIT failed: %s", strerror(rc));
        goto fail;
    }
    /* Receive WQEs must be posted before the peer can reach RTS against us,
     * and ibv_post_recv is only valid from INIT onward. */
    if (role == MELON_BULK_RECEIVER) {
        for (uint64_t i = 0; i < BULK_IMM_RECV_DEPTH; i++)
            if (post_imm_recv(b, i) != 0) goto fail;
    } else {
        for (unsigned i = 0; i < BULK_CREDIT_DEPTH; i++)
            if (post_credit_recv(b, i) != 0) goto fail;
    }
    return b;

fail:
    melon_bulk_close(b);
    return NULL;
}

int melon_bulk_gid_index(const melon_bulk_t *b) { return b ? b->gid_index : -1; }

uint64_t melon_bulk_max_put(const melon_bulk_t *b) { return b ? b->ring_bytes / 2 : 0; }

int melon_bulk_local_endpoint(melon_bulk_t *b, uint8_t out[MELON_BULK_ENDPOINT_BYTES]) {
    struct endpoint_wire w;
    memset(&w, 0, sizeof(w));
    w.magic = htonl(BULK_MAGIC);
    w.qpn = htonl(b->qp->qp_num);
    w.psn = htonl(b->psn);
    w.role = (uint8_t)b->role;
    w.mtu = (uint8_t)b->mtu;
    memcpy(w.gid, b->gid.raw, 16);
    if (b->role == MELON_BULK_RECEIVER) {
        w.imm_recv_depth = htons(BULK_IMM_RECV_DEPTH);
        put_be64((uint8_t *)&w + offsetof(struct endpoint_wire, ring_addr),
                 (uint64_t)(uintptr_t)b->ring);
        put_be64((uint8_t *)&w + offsetof(struct endpoint_wire, ring_bytes), b->ring_bytes);
        w.ring_rkey = htonl(b->ring_mr->rkey);
    }
    memcpy(out, &w, sizeof(w));
    return MELON_BULK_OK;
}


int melon_bulk_connect(melon_bulk_t *b, const uint8_t remote[MELON_BULK_ENDPOINT_BYTES]) {
    if (b->connected) {
        set_error("bulk lane is already connected");
        return MELON_BULK_ERROR;
    }
    struct endpoint_wire w;
    memcpy(&w, remote, sizeof(w));
    int want_role = b->role == MELON_BULK_RECEIVER ? MELON_BULK_SENDER : MELON_BULK_RECEIVER;
    if (ntohl(w.magic) != BULK_MAGIC || w.role != want_role) {
        set_error("peer endpoint is not a melon bulk %s",
                  want_role == MELON_BULK_SENDER ? "sender" : "receiver");
        return MELON_BULK_ERROR;
    }
    enum ibv_mtu mtu = (enum ibv_mtu)w.mtu < b->mtu ? (enum ibv_mtu)w.mtu : b->mtu;

    if (b->role == MELON_BULK_SENDER) {
        uint64_t ring = get_be64(remote + offsetof(struct endpoint_wire, ring_bytes));
        uint32_t depth = ntohs(w.imm_recv_depth);
        if (ring < BULK_MIN_RING || ring > BULK_MAX_RING || depth < 2) {
            set_error("peer ring of %llu bytes / %u receives is unusable",
                      (unsigned long long)ring, depth);
            return MELON_BULK_ERROR;
        }
        b->ring = stash_take(ring);
        if (!b->ring) {
            set_error("could not allocate %llu bytes of staging", (unsigned long long)ring);
            return MELON_BULK_ERROR;
        }
        b->ring_bytes = ring;
        b->ring_mr = reg(b, b->ring, ring, IBV_ACCESS_LOCAL_WRITE, "staging");
        if (!b->ring_mr) return MELON_BULK_ERROR;
        b->remote_addr = get_be64(remote + offsetof(struct endpoint_wire, ring_addr));
        b->remote_rkey = ntohl(w.ring_rkey);
        b->remote_recv_depth = depth;
    }

    struct ibv_qp_attr a = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = mtu,
        .dest_qp_num = ntohl(w.qpn),
        .rq_psn = ntohl(w.psn),
        .max_dest_rd_atomic = 1,
        .min_rnr_timer = 12,
        .ah_attr = {
            .is_global = 1,
            .port_num = b->port,
            .grh = {.hop_limit = 1, .sgid_index = (uint8_t)b->gid_index},
        },
    };
    memcpy(a.ah_attr.grh.dgid.raw, w.gid, 16);
    int rc = ibv_modify_qp(b->qp, &a,
                           IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                               IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                               IBV_QP_MIN_RNR_TIMER);
    if (rc != 0) {
        set_error("INIT->RTR failed: %s (mtu=%d dest_qpn=%u gid_index=%d)", strerror(rc),
                  (int)mtu, ntohl(w.qpn), b->gid_index);
        return MELON_BULK_ERROR;
    }
    struct ibv_qp_attr r = {
        .qp_state = IBV_QPS_RTS,
        .timeout = 14,
        .retry_cnt = 7,
        .rnr_retry = 7,
        .sq_psn = b->psn,
        .max_rd_atomic = 1,
    };
    rc = ibv_modify_qp(b->qp, &r,
                       IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                           IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
    if (rc != 0) {
        set_error("RTR->RTS failed: %s", strerror(rc));
        return MELON_BULK_ERROR;
    }
    b->connected = 1;
    return MELON_BULK_OK;
}

static int fail_broken(melon_bulk_t *b, const char *what, const struct ibv_wc *wc) {
    b->broken = 1;
    set_error("%s completion failed: %s (status %d, vendor 0x%x)", what,
              ibv_wc_status_str(wc->status), (int)wc->status, wc->vendor_err);
    return MELON_BULK_ERROR;
}

static int usable(melon_bulk_t *b, int role) {
    if (!b || !b->connected) {
        set_error("bulk lane is not connected");
        return 0;
    }
    if (b->broken) return 0; /* keep the error that broke it */
    if (b->role != role) {
        set_error("operation is for the %s side",
                  role == MELON_BULK_SENDER ? "sender" : "receiver");
        return 0;
    }
    return 1;
}

/* --- sender -------------------------------------------------------------- */

static uint64_t stamp_now(melon_bulk_t *b) {
#ifdef __APPLE__
    (void)b;
#else
    if (b->hw_stamps) {
        struct ibv_values_ex values = {.comp_mask = IBV_VALUES_MASK_RAW_CLOCK};
        if (ibv_query_rt_values_ex(b->ctx, &values) == 0)
            return (uint64_t)values.raw_clock.tv_sec * 1000000000ull +
                   (uint64_t)values.raw_clock.tv_nsec;
        /* A clock that stops answering mid-transfer cannot be mixed with the
         * monotonic fallback; the wire time is then unknown, not guessed. */
        b->hw_stamps = 0;
        b->busy_total = 0;
        b->busy_open = 0;
    }
#endif
    return now_ns();
}

/* Writes complete in post order on one RC queue pair, so each completed
 * interval starts no earlier than the previous one: the union is kept as one
 * open interval plus everything already closed. */
static void account_write(melon_bulk_t *b, uint64_t posted, uint64_t completed) {
    if (completed < posted) completed = posted;
    if (b->busy_open && posted <= b->busy_end) {
        if (completed > b->busy_end) b->busy_end = completed;
        return;
    }
    if (b->busy_open) b->busy_total += b->busy_end - b->busy_start;
    b->busy_open = 1;
    b->busy_start = posted;
    b->busy_end = completed;
}

static int write_completed(melon_bulk_t *b, enum ibv_wc_status status, uint64_t wr_id,
                           uint32_t vendor_err, uint64_t completed) {
    if (status != IBV_WC_SUCCESS) {
        struct ibv_wc wc = {.status = status, .vendor_err = vendor_err};
        return fail_broken(b, "RDMA write", &wc);
    }
    account_write(b, b->post_stamp[wr_id % BULK_SEND_DEPTH], completed);
    b->sends_outstanding--;
    return MELON_BULK_OK;
}

static int sender_reap_writes(melon_bulk_t *b) {
#ifndef __APPLE__
    if (b->send_cq_ex) {
        struct ibv_poll_cq_attr attr = {0};
        int rc = ibv_start_poll(b->send_cq_ex, &attr);
        if (rc == ENOENT) return MELON_BULK_OK;
        if (rc != 0) {
            b->broken = 1;
            set_error("ibv_start_poll (send) failed: %s", strerror(rc));
            return MELON_BULK_ERROR;
        }
        int result = MELON_BULK_OK;
        do {
            struct ibv_cq_ex *cq = b->send_cq_ex;
            uint64_t completed = b->hw_stamps ? ibv_wc_read_completion_ts(cq) : now_ns();
            if (result == MELON_BULK_OK)
                result = write_completed(b, cq->status, cq->wr_id, ibv_wc_read_vendor_err(cq),
                                         completed);
            rc = ibv_next_poll(b->send_cq_ex);
        } while (rc == 0);
        ibv_end_poll(b->send_cq_ex);
        if (rc != ENOENT && result == MELON_BULK_OK) {
            b->broken = 1;
            set_error("ibv_next_poll (send) failed: %s", strerror(rc));
            return MELON_BULK_ERROR;
        }
        return result;
    }
#endif
    struct ibv_wc wc[16];
    for (;;) {
        int n = ibv_poll_cq(b->send_cq, 16, wc);
        if (n < 0) {
            b->broken = 1;
            set_error("ibv_poll_cq (send) failed");
            return MELON_BULK_ERROR;
        }
        uint64_t reaped = n > 0 ? now_ns() : 0;
        for (int i = 0; i < n; i++)
            if (write_completed(b, wc[i].status, wc[i].wr_id, wc[i].vendor_err, reaped) !=
                MELON_BULK_OK)
                return MELON_BULK_ERROR;
        if (n < 16) break;
    }
    return MELON_BULK_OK;
}

uint64_t melon_bulk_wire_ns(const melon_bulk_t *b, int *hardware) {
    if (hardware) *hardware = b ? b->hw_stamps : 0;
    if (!b) return 0;
    uint64_t busy = b->busy_total + (b->busy_open ? b->busy_end - b->busy_start : 0);
    if (b->hw_stamps) return b->hca_khz ? busy * 1000000ull / b->hca_khz : 0;
    return busy;
}

static int sender_reap(melon_bulk_t *b) {
    if (sender_reap_writes(b) != MELON_BULK_OK) return MELON_BULK_ERROR;
    struct ibv_wc wc[16];
    for (;;) {
        int n = ibv_poll_cq(b->recv_cq, 16, wc);
        if (n < 0) {
            b->broken = 1;
            set_error("ibv_poll_cq (credit) failed");
            return MELON_BULK_ERROR;
        }
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) return fail_broken(b, "credit receive", &wc[i]);
            const struct credit_wire *c = &b->credit_rx[wc[i].wr_id];
            uint64_t tail = get_be64(&c->tail);
            uint64_t released = get_be64(&c->released);
            /* Credits only move forward, and never past what was sent. */
            if (tail > b->head || released > b->next_index) {
                b->broken = 1;
                set_error("peer credited %llu bytes / %llu puts beyond what was sent",
                          (unsigned long long)tail, (unsigned long long)released);
                return MELON_BULK_ERROR;
            }
            if (tail > b->credit_tail) b->credit_tail = tail;
            if (released > b->credit_released) b->credit_released = released;
            if (post_credit_recv(b, (unsigned)wc[i].wr_id) != 0) {
                b->broken = 1;
                return MELON_BULK_ERROR;
            }
        }
        if (n < 16) break;
    }
    return MELON_BULK_OK;
}

int melon_bulk_put(melon_bulk_t *b, const void *data, uint64_t len, uint32_t *out_index,
                   uint64_t *out_position, int timeout_ms) {
    if (!usable(b, MELON_BULK_SENDER)) return MELON_BULK_ERROR;
    if (len == 0 || len > b->ring_bytes / 2 || len > UINT32_MAX) {
        set_error("put of %llu bytes is outside (0, %llu]", (unsigned long long)len,
                  (unsigned long long)(b->ring_bytes / 2));
        return MELON_BULK_ERROR;
    }
    uint64_t pos = b->head;
    uint64_t offset = pos % b->ring_bytes;
    if (offset + len > b->ring_bytes) {
        pos += b->ring_bytes - offset;
        offset = 0;
    }
    struct waiter w;
    waiter_init(&w, timeout_ms);
    for (;;) {
        if (sender_reap(b) != MELON_BULK_OK) return MELON_BULK_ERROR;
        int room = pos + len - b->credit_tail <= b->ring_bytes;
        int wqes = (uint64_t)b->next_index - b->credit_released < b->remote_recv_depth;
        int sq = b->sends_outstanding < BULK_SEND_DEPTH;
        if (room && wqes && sq) break;
        if (waiter_pause(&w) == MELON_BULK_TIMEOUT) {
            set_error("no credit for a %llu-byte put within %d ms (%llu bytes and %llu puts "
                      "unreleased)", (unsigned long long)len, timeout_ms,
                      (unsigned long long)(b->head - b->credit_tail),
                      (unsigned long long)(b->next_index - b->credit_released));
            return MELON_BULK_TIMEOUT;
        }
    }

    /* Everything at these staging bytes belongs to a put the receiver has
     * already released, so the NIC is done reading it. */
    memcpy(b->ring + offset, data, len);
    struct ibv_sge sge = {
        .addr = (uintptr_t)(b->ring + offset),
        .length = (uint32_t)len,
        .lkey = b->ring_mr->lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id = b->next_index,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = IBV_SEND_SIGNALED,
        .imm_data = htonl(b->next_index),
        .wr.rdma = {.remote_addr = b->remote_addr + offset, .rkey = b->remote_rkey},
    };
    struct ibv_send_wr *bad = NULL;
    b->post_stamp[b->next_index % BULK_SEND_DEPTH] = stamp_now(b);
    if (ibv_post_send(b->qp, &wr, &bad) != 0) {
        b->broken = 1;
        set_error("ibv_post_send (RDMA write) failed: %s", strerror(errno));
        return MELON_BULK_ERROR;
    }
    b->sends_outstanding++;
    if (out_index) *out_index = b->next_index;
    if (out_position) *out_position = pos;
    b->next_index++;
    b->head = pos + len;
    return MELON_BULK_OK;
}

int melon_bulk_flush(melon_bulk_t *b, int timeout_ms) {
    if (!usable(b, MELON_BULK_SENDER)) return MELON_BULK_ERROR;
    struct waiter w;
    waiter_init(&w, timeout_ms);
    for (;;) {
        if (sender_reap(b) != MELON_BULK_OK) return MELON_BULK_ERROR;
        if (b->sends_outstanding == 0) return MELON_BULK_OK;
        if (waiter_pause(&w) == MELON_BULK_TIMEOUT) {
            set_error("%u RDMA writes still outstanding after %d ms", b->sends_outstanding,
                      timeout_ms);
            return MELON_BULK_TIMEOUT;
        }
    }
}

/* --- receiver ------------------------------------------------------------ */

static int receiver_poll(melon_bulk_t *b) {
    struct ibv_wc wc[16];
    int n = ibv_poll_cq(b->recv_cq, 16, wc);
    if (n < 0) {
        b->broken = 1;
        set_error("ibv_poll_cq (immediate) failed");
        return MELON_BULK_ERROR;
    }
    for (int i = 0; i < n; i++) {
        if (wc[i].status != IBV_WC_SUCCESS) return fail_broken(b, "immediate receive", &wc[i]);
        /* Keyed on the flag, not the opcode: rdma-core reports a landed
         * write as IBV_WC_RECV_RDMA_WITH_IMM, but MelonDMA's compat layer
         * has no such opcode and reports IBV_WC_RECV with IBV_WC_WITH_IMM.
         * Either way the peer only ever writes with an immediate, so a
         * receive without one is a message that has no business here. */
        if (!(wc[i].wc_flags & IBV_WC_WITH_IMM)) {
            b->broken = 1;
            set_error("peer sent a plain message where only RDMA writes belong");
            return MELON_BULK_ERROR;
        }
        if (b->landed >= BULK_IMM_RECV_DEPTH) {
            b->broken = 1;
            set_error("peer ran past its own receive credit");
            return MELON_BULK_ERROR;
        }
        b->landed_imm[(b->landed_head + b->landed) % BULK_IMM_RECV_DEPTH] = ntohl(wc[i].imm_data);
        b->landed++;
        if (post_imm_recv(b, wc[i].wr_id) != 0) {
            b->broken = 1;
            return MELON_BULK_ERROR;
        }
    }
    /* Credit sends: nothing to learn from them but failure. */
    n = ibv_poll_cq(b->send_cq, 16, wc);
    if (n < 0) {
        b->broken = 1;
        set_error("ibv_poll_cq (credit send) failed");
        return MELON_BULK_ERROR;
    }
    for (int i = 0; i < n; i++) {
        if (wc[i].status != IBV_WC_SUCCESS) return fail_broken(b, "credit send", &wc[i]);
        b->sends_outstanding--;
    }
    return MELON_BULK_OK;
}

int melon_bulk_accept(melon_bulk_t *b, uint32_t index, uint64_t position, uint64_t len,
                      const void **data, int timeout_ms) {
    if (!usable(b, MELON_BULK_RECEIVER)) return MELON_BULK_ERROR;
    if (b->have_accepted) {
        set_error("release the previous put before accepting the next");
        return MELON_BULK_ERROR;
    }
    if (index != b->expect_index) {
        set_error("control frame names put %u where put %u is next", index, b->expect_index);
        return MELON_BULK_ERROR;
    }
    if (len == 0 || len > b->ring_bytes / 2) {
        set_error("put of %llu bytes is outside (0, %llu]", (unsigned long long)len,
                  (unsigned long long)(b->ring_bytes / 2));
        return MELON_BULK_ERROR;
    }
    uint64_t expect = b->expect_position;
    uint64_t offset = expect % b->ring_bytes;
    if (offset + len > b->ring_bytes) {
        expect += b->ring_bytes - offset;
        offset = 0;
    }
    if (position != expect) {
        set_error("put %u claims position %llu; the ring puts it at %llu", index,
                  (unsigned long long)position, (unsigned long long)expect);
        return MELON_BULK_ERROR;
    }
    struct waiter w;
    waiter_init(&w, timeout_ms);
    while (b->landed == 0) {
        if (receiver_poll(b) != MELON_BULK_OK) return MELON_BULK_ERROR;
        if (b->landed) break;
        if (waiter_pause(&w) == MELON_BULK_TIMEOUT) {
            set_error("put %u did not land within %d ms", index, timeout_ms);
            return MELON_BULK_TIMEOUT;
        }
    }
    uint32_t imm = b->landed_imm[b->landed_head];
    if (imm != index) {
        b->broken = 1;
        set_error("RDMA write %u landed where put %u was announced", imm, index);
        return MELON_BULK_ERROR;
    }
    b->landed_head = (b->landed_head + 1) % BULK_IMM_RECV_DEPTH;
    b->landed--;
    b->expect_index++;
    b->expect_position = position + len;
    b->accepted_end = position + len;
    b->have_accepted = 1;
    *data = b->ring + offset;
    return MELON_BULK_OK;
}

int melon_bulk_release(melon_bulk_t *b) {
    if (!usable(b, MELON_BULK_RECEIVER)) return MELON_BULK_ERROR;
    if (!b->have_accepted) {
        set_error("nothing accepted to release");
        return MELON_BULK_ERROR;
    }
    b->have_accepted = 0;
    b->released++;
    /* A full send queue only means our own credit sends have not been reaped
     * yet; reap before posting another. */
    struct waiter w;
    waiter_init(&w, 5000);
    while (b->sends_outstanding >= BULK_CREDIT_DEPTH) {
        if (receiver_poll(b) != MELON_BULK_OK) return MELON_BULK_ERROR;
        if (b->sends_outstanding < BULK_CREDIT_DEPTH) break;
        if (waiter_pause(&w) == MELON_BULK_TIMEOUT) {
            set_error("credit sends are not completing");
            b->broken = 1;
            return MELON_BULK_ERROR;
        }
    }
    struct credit_wire *c = &b->credit_tx[b->credit_tx_next];
    b->credit_tx_next = (b->credit_tx_next + 1) % BULK_CREDIT_DEPTH;
    put_be64(&c->tail, b->accepted_end);
    put_be64(&c->released, b->released);
    struct ibv_sge sge = {
        .addr = (uintptr_t)c,
        .length = sizeof(*c),
        .lkey = b->credit_tx_mr->lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id = b->released,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };
    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(b->qp, &wr, &bad) != 0) {
        b->broken = 1;
        set_error("ibv_post_send (credit) failed: %s", strerror(errno));
        return MELON_BULK_ERROR;
    }
    b->sends_outstanding++;
    return MELON_BULK_OK;
}

void melon_bulk_close(melon_bulk_t *b) {
    if (!b) return;
    if (b->qp) ibv_destroy_qp(b->qp);
    if (b->send_cq) ibv_destroy_cq(b->send_cq); /* also the _ex view, which it is */
    if (b->recv_cq) ibv_destroy_cq(b->recv_cq);
    if (b->ring_mr) ibv_dereg_mr(b->ring_mr);
    if (b->imm_sink_mr) ibv_dereg_mr(b->imm_sink_mr);
    if (b->credit_tx_mr) ibv_dereg_mr(b->credit_tx_mr);
    if (b->credit_rx_mr) ibv_dereg_mr(b->credit_rx_mr);
    if (b->pd) ibv_dealloc_pd(b->pd);
    if (b->ctx) ibv_close_device(b->ctx);
    stash_give(b->ring, b->ring_bytes);
    free(b->imm_sink);
    free(b->credit_tx);
    free(b->credit_rx);
    free(b);
}
