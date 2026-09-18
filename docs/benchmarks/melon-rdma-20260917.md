# melon-rdma vs TCP, rerun 2026-09-17

> **Corrected 2026-09-18.** The 6.7–7.6% RDMA lead below does not reproduce.
> Paired on one producer the next day, TCP at 131,008 tokens was 134.47 s,
> not 143.32 s, and RDMA's advantage is 0.8–1.0% — what the wire arithmetic
> predicts. The 09-17 TCP arm was systematically slow for a reason not
> identified; the "~8 s the wire cannot account for" below was that, not the
> transport. The five fixes and the data-leg numbers stand. See
> [`melon-rdma-20260918.md`](melon-rdma-20260918.md).

The 2026-08-30 sweep reported the RDMA lane at parity with TCP, within about
1% either way, and slower at three of six depths. That reading was correct
for the code as it stood and wrong about the transport. Five separate things
kept the lane from running the way it was supposed to; with those fixed, the
same protocol on the same pair of machines puts RDMA ahead at every depth
above 2,048, by margins that do not overlap between the two arms.

## What was actually broken

1. **The GID index was hard-coded.** MelonDMA programs one RoCE slot per
   *client* and answers `ibv_query_gid` only for the slot that client owns.
   Index 0 was free in August; it is not free now, so the pipe failed at its
   first query and the transport could not come up at all.
2. **No `LC_RPATH` on any binary that links the shim.** `muser`,
   `muser-remote-qualify` and the examples all died in dyld. Under the
   accelerator wrapper this surfaced as "accelerator wrapper exit differs
   from its retained receipt", which names nothing.
3. **The receiver's entitlement still carried the old DriverKit bundle id.**
4. **The RX ring was four slots against a sixteen-deep TX window**, so a
   burst ran the receive queue dry and paid an RNR NAK for the rest of it.
5. **The producer container had docker's default 8 MiB `RLIMIT_MEMLOCK`.**
   Every RDMA registration is locked memory: 16 TX generations at 256 KiB is
   4 MiB, and the RX ring is the rest, so `ibv_reg_mr` returns ENOMEM exactly
   when the RX ring is made deep enough to match the TX window. The GX10 host
   and the systemd unit were already unlimited; only the container was not.
   This matters for reading the original commit, which recorded the ceiling
   as a macOS DriverKit story: on the producer side it is a container limit,
   not a NIC, a driver or a verbs limit.

## Context sweep

Same protocol as the published run: 2,048 through 131,008 tokens, one
discarded warm-up handoff then five counted repetitions, 32 decode tokens,
`--prefix-cache off`, identical token fixtures across both arms. Per-request
rows in `melon-rdma-20260917-sweep-{tcp,rdma}.csv`; folded comparison in
`melon-rdma-vs-tcp-context-sweep-20260917.csv`.

With the prefix cache off the counted repetitions are not cache hits — every
one is a full handoff — so each cell below is five samples that each crossed
the wire, not a single cold sample.

| Depth | TCP p50 | RDMA p50 | RDMA vs TCP | Saved | Ranges overlap? |
|---:|---:|---:|---:|---:|---|
| 2,048 | 1.714 s | 1.729 s | +0.88% | −0.015 s | yes |
| 8,192 | 6.769 s | 6.670 s | −1.46% | 0.099 s | no |
| 16,384 | 13.482 s | 13.123 s | −2.66% | 0.359 s | no |
| 32,768 | 28.958 s | 26.948 s | −6.94% | 2.010 s | no |
| 65,536 | 62.374 s | 57.636 s | −7.60% | 4.737 s | no |
| 131,008 | 143.317 s | 133.716 s | −6.70% | 9.601 s | no |

CV is 0.09–0.78% in every cell, against muser's own ≤2% gate. From 8,192 up,
the min-to-max range of one arm's five repetitions does not reach the other
arm's: at 131,008, TCP spans 143.119–143.913 s and RDMA spans
133.434–134.494 s.

At 2,048 the two arms are indistinguishable, which is the honest shape of
this: the payload is too small for the wire to be worth anything.

## The data leg on its own

`producer_payload_gbps` from muser's own handoff receipt, same payload bytes
on both arms:

| Handoff | Payload | TCP | RDMA | Ratio |
|---|---:|---:|---:|---|
| 2,048 tokens | 108,998,656 B | 7.390 Gbit/s | 9.521 Gbit/s | 1.29× |
| 16,384 tokens | 299,879,424 B | 7.382 Gbit/s | 17.441 Gbit/s | 2.36× |

And the byte pipe with nothing above it, GX10 → Mac, on a payload the size of
a full 65,536-token handoff (954,190,848 B):

| Write size | TCP (10GbE) | RDMA | Ratio |
|---|---:|---:|---|
| 256 KiB | 0.772 s — 9.88 Gbit/s | 0.331 s — 23.08 Gbit/s | 2.33× |
| 64 KiB | 0.773 s — 9.88 Gbit/s | 0.375 s — 20.47 Gbit/s | 2.06× |

The 2,048 cell's 1.29× is fixed per-handoff overhead, not a rate ceiling: the
same lane reaches 2.36× one depth up, which is the raw pipe's own ratio.

The RX ring depth shows up only on small writes, which is the pattern muser
actually posts: at 64 KiB, depth 4 opens at 13.7 Gbit/s and settles at
18.6–21.6, while depth 16 holds 22.85 with no ramp. At 256 KiB both saturate
the link.

## One thing this does not explain

The 131,008 cell saves 9.6 s, and the wire arithmetic does not account for
it. That payload is roughly 1.9 GB: 2.07 s of TCP against 0.88 s of RDMA, so
about 1.2 s is all the transfer itself can be worth. The other ~8 s is
unexplained by these measurements, and the producer phase receipt that would
show where it goes is exactly what stops arriving at this depth — see below.
It should not be claimed as a transport result until it is isolated.

## Known gap: the payload receipt disappears at depth

`ProducerPhaseReceiptV1` is read under a 250 ms deadline after the generation
is already live. At 16,384 it arrives; at 65,536 it reproducibly does not,
and `muser-remote-qualify --operational-probe` then fails with "operational
probe omits the authenticated producer payload timing receipt". So muser's
own link-quality instrument goes quiet above some depth between the two, and
the `node smoke` netqual number only ever describes a 2,048-token handoff.
That is why the data-leg table above stops at 16,384.

## Reproducing

Both sides need the provider configured or the Mac programs no GID slot at
all and the pipe cannot start:

    export MELONDMA_LOCAL_IP=…  MELONDMA_LOCAL_MAC=…  MELONDMA_REMOTE_MAC=…

The producer's transport is the operator-set env file on the node,
`~/.config/muser/gx10-prefilld.env`; the receiver's is its own process
environment. Both must agree. `MUSER_RDMA_GID` on the GX10 wants the RoCE v2
index for the cabled address — index 2 and index 3 there are the same IP as
v1 and v2 respectively, and the Mac side only ever has v2 — and on the Mac
`-1` asks the pipe to find whichever slot it owns.
