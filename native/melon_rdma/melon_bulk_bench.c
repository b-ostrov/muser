/* melon_bulk_bench — the bulk lane on its own, shaped like the handoff uses it:
 * each put is announced by a small control record on the TCP socket (standing
 * in for the TLS segment header), and the receiver copies every payload out of
 * the ring and checks it before releasing, exactly as the Rust receiver must.
 *
 *   receiver:  melon_bulk_bench --listen 0.0.0.0:29200 --dev mlx5_0 --gid -1
 *   sender:    melon_bulk_bench --connect host:29200 --dev rocep1s0f1 --gid 3 --send
 *
 * Same source on both hosts, so neither side's number comes from a different
 * implementation.
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "melon_rdma_bulk.h"

struct announce {
    uint32_t index;
    uint32_t len;
    uint64_t position;
} __attribute__((packed));

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + ts.tv_nsec / 1e9;
}

static int io_all(int fd, void *buf, size_t len, int sending) {
    uint8_t *p = buf;
    while (len) {
        ssize_t n = sending ? send(fd, p, len, 0) : recv(fd, p, len, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return -1;
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int split(const char *hostport, char *host, size_t cap, int *port) {
    const char *colon = strrchr(hostport, ':');
    if (!colon || (size_t)(colon - hostport) >= cap) return -1;
    memcpy(host, hostport, (size_t)(colon - hostport));
    host[colon - hostport] = 0;
    *port = atoi(colon + 1);
    return 0;
}

static int tcp_open(const char *listen_at, const char *connect_to) {
    char host[128];
    int port;
    int one = 1;
    struct sockaddr_in sa = {.sin_family = AF_INET};
    if (split(listen_at ? listen_at : connect_to, host, sizeof(host), &port) != 0) return -1;
    sa.sin_port = htons((uint16_t)port);
    sa.sin_addr.s_addr = strcmp(host, "0.0.0.0") ? inet_addr(host) : INADDR_ANY;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_at) {
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) || listen(fd, 1)) {
            perror("listen");
            return -1;
        }
        fprintf(stderr, "listening on %s\n", listen_at);
        int c = accept(fd, NULL, NULL);
        close(fd);
        fd = c;
    } else if (connect(fd, (struct sockaddr *)&sa, sizeof(sa))) {
        perror("connect");
        return -1;
    }
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

int main(int argc, char **argv) {
    const char *listen_at = NULL, *connect_to = NULL, *dev = "mlx5_0";
    const char *roce_ip = NULL, *roce_peer = NULL;
    int gid = -1, sending = 0, reps = 3, gap_us = 0;
    uint64_t bytes = 954190848ull, segment = 6815744ull, ring = 64ull << 20;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--listen")) listen_at = argv[++i];
        else if (!strcmp(argv[i], "--connect")) connect_to = argv[++i];
        else if (!strcmp(argv[i], "--dev")) dev = argv[++i];
        else if (!strcmp(argv[i], "--gid")) gid = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--bytes")) bytes = strtoull(argv[++i], NULL, 0);
        else if (!strcmp(argv[i], "--segment")) segment = strtoull(argv[++i], NULL, 0);
        else if (!strcmp(argv[i], "--ring")) ring = strtoull(argv[++i], NULL, 0);
        else if (!strcmp(argv[i], "--reps")) reps = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--roce-ip")) roce_ip = argv[++i];
        else if (!strcmp(argv[i], "--roce-peer-mac")) roce_peer = argv[++i];
        else if (!strcmp(argv[i], "--gap-us")) gap_us = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--send")) sending = 1;
        else {
            fprintf(stderr, "unknown argument %s\n", argv[i]);
            return 2;
        }
    }
    if (!listen_at == !connect_to) {
        fprintf(stderr, "need exactly one of --listen / --connect\n");
        return 2;
    }
    int fd = tcp_open(listen_at, connect_to);
    if (fd < 0) return 1;

    melon_bulk_t *b = melon_bulk_create(dev, gid, sending ? MELON_BULK_SENDER : MELON_BULK_RECEIVER,
                                        ring, roce_ip, roce_peer);
    if (!b) {
        fprintf(stderr, "create: %s\n", melon_bulk_last_error());
        return 1;
    }
    uint8_t mine[MELON_BULK_ENDPOINT_BYTES], theirs[MELON_BULK_ENDPOINT_BYTES];
    melon_bulk_local_endpoint(b, mine);
    if (io_all(fd, mine, sizeof(mine), 1) || io_all(fd, theirs, sizeof(theirs), 0)) {
        fprintf(stderr, "endpoint exchange failed\n");
        return 1;
    }
    if (melon_bulk_connect(b, theirs) != MELON_BULK_OK) {
        fprintf(stderr, "connect: %s\n", melon_bulk_last_error());
        return 1;
    }
    fprintf(stderr, "bulk lane up: %s dev=%s gid=%d max_put=%llu\n",
            sending ? "sender" : "receiver", dev, melon_bulk_gid_index(b),
            (unsigned long long)melon_bulk_max_put(b));

    uint8_t *src = malloc(segment), *copy = malloc(segment);
    uint64_t segments = (bytes + segment - 1) / segment;
    for (int r = 0; r < reps; r++) {
        double t0 = now_s();
        uint64_t moved = 0, bad = 0;
        for (uint64_t s = 0; s < segments; s++) {
            uint64_t len = bytes - moved < segment ? bytes - moved : segment;
            uint8_t fill = (uint8_t)(s * 131 + r);
            if (sending) {
                memset(src, fill, len);
                if (gap_us) usleep((useconds_t)gap_us);
                struct announce a;
                uint32_t index;
                uint64_t position;
                int rc = melon_bulk_put(b, src, len, &index, &position, 30000);
                if (rc != MELON_BULK_OK) {
                    fprintf(stderr, "put: %s\n", melon_bulk_last_error());
                    return 1;
                }
                a.index = index;
                a.len = (uint32_t)len;
                a.position = position;
                if (io_all(fd, &a, sizeof(a), 1)) return 1;
            } else {
                struct announce a;
                if (io_all(fd, &a, sizeof(a), 0)) return 1;
                const void *data;
                int rc = melon_bulk_accept(b, a.index, a.position, a.len, &data, 30000);
                if (rc != MELON_BULK_OK) {
                    fprintf(stderr, "accept: %s\n", melon_bulk_last_error());
                    return 1;
                }
                memcpy(copy, data, a.len);
                if (melon_bulk_release(b) != MELON_BULK_OK) {
                    fprintf(stderr, "release: %s\n", melon_bulk_last_error());
                    return 1;
                }
                if (copy[0] != fill || copy[a.len - 1] != fill || copy[a.len / 2] != fill)
                    bad++;
            }
            moved += len;
        }
        uint8_t ack = 1;
        if (sending) {
            if (melon_bulk_flush(b, 30000) != MELON_BULK_OK) {
                fprintf(stderr, "flush: %s\n", melon_bulk_last_error());
                return 1;
            }
            if (io_all(fd, &ack, 1, 0)) return 1;
        } else if (io_all(fd, &ack, 1, 1)) {
            return 1;
        }
        double dt = now_s() - t0;
        if (sending) {
            int hw = 0;
            uint64_t wire = melon_bulk_wire_ns(b, &hw);
            static uint64_t wire_before;
            uint64_t rep_wire = wire - wire_before;
            wire_before = wire;
            printf("WIRE rep=%d wire_ns=%llu wire_gbit_s=%.3f hardware_stamps=%d\n", r,
                   (unsigned long long)rep_wire, rep_wire ? moved * 8.0 / (double)rep_wire : 0.0, hw);
        }
        printf("%s rep=%d bytes=%llu segment=%llu segments=%llu seconds=%.4f gbit_s=%.3f%s\n",
               sending ? "SEND" : "RECV", r, (unsigned long long)moved,
               (unsigned long long)segment, (unsigned long long)segments, dt,
               moved * 8.0 / dt / 1e9, bad ? " CORRUPT" : "");
        if (bad) printf("corrupt segments: %llu\n", (unsigned long long)bad);
        fflush(stdout);
    }
    melon_bulk_close(b);
    close(fd);
    return 0;
}
