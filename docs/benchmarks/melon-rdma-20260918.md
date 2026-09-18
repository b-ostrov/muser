# melon-rdma bulk lane, 2026-09-18

The RDMA path is no longer TLS over an RDMA byte pipe. Control — Begin,
segment headers, Seal, ACK — stays on mTLS over TCP; segment payloads are
RDMA-written into a ring the receiver registered, with credit flow control.
The lane is negotiated per transfer and falls back to inline TLS, loudly,
whenever either side cannot bring it up. `muser node add --transport rdma`
discovers and enrolls it; nothing is configured by hand. See the commit
"Replace TLS-over-RDMA with an RDMA bulk lane under mTLS control" for the
design and its security argument.

## The data leg

`producer_payload_gbps` from muser's own handoff receipt, identical payload
bytes in every column. On the bulk lane this is the union of [post,
completion] over every RDMA write from NIC completion timestamps — TCP_INFO
busy time's counterpart — so it is the link's rate while it carried this
transfer, not the transfer's wall-clock rate.

| Handoff | Payload | TCP | Old TLS-over-RDMA pipe | Bulk lane |
|---|---:|---:|---:|---:|
| 2,048 tokens | 108,998,656 B | 7.39 | 9.52 | **23.11** Gbit/s |
| 16,384 tokens | 299,879,424 B | 7.38 | 17.44 | **23.11** Gbit/s |

23.1 Gbit/s is the GX10 -> Mac ceiling of this link; the lane runs at it on
the smallest handoff as well as a large one.

## Context sweep, paired the same day

Same protocol as `melon-rdma-20260917.md` (one discarded warm-up, five
counted reps, 32 decode tokens, `--prefix-cache off`, identical fixtures).
The TCP arm is the same producer and receiver with the `rdma` section removed
from the receiver config, so every handoff went through the decline path and
carried its payload inline — 36 of 36 declines logged. Rows in
`melon-rdma-20260918-sweep-{bulk,tcp}.csv`.

| Depth | TCP p50 | Bulk p50 | Bulk vs TCP | Saved | Five-rep ranges |
|---:|---:|---:|---:|---:|---|
| 2,048 | 1.739 s | 1.737 s | −0.08% | 0.001 s | overlap |
| 8,192 | 6.754 s | 6.724 s | −0.43% | 0.029 s | overlap |
| 16,384 | 13.175 s | 13.040 s | −1.03% | 0.135 s | separated |
| 32,768 | 27.126 s | 26.899 s | −0.84% | 0.227 s | separated |
| 65,536 | 58.128 s | 57.582 s | −0.94% | 0.546 s | separated |
| 131,008 | 134.470 s | 133.431 s | −0.77% | 1.039 s | overlap |

CV 0.08–1.64%, inside muser's 2% gate in every cell.

**The end-to-end win is about 1%, and that is the right size.** A 131k
handoff is about 1.8 GB: roughly 2.0 s at 7.4 Gbit/s against 0.65 s at 23.1,
and muser streams those bytes during prefill, so most of the difference is
already hidden behind compute. The saved second at 131k is what the wire
arithmetic predicts.

The bulk lane and yesterday's TLS-over-RDMA pipe are indistinguishable end to
end (133.4 vs 133.7 s at 131k): the lane's 1.3x on the wire is spent inside
time that prefill already covers. What the lane changes is everything around
the number — negotiation instead of a mismatch hang, a fallback that cannot
be silent, enrollment instead of hand-set GIDs and environment, no core
busy-polled for the length of a prefill, a wire metric that measures the
wire, and headroom the pipe did not have.

## Correction to 2026-09-17

`melon-rdma-20260917.md` reported RDMA ahead by 6.7–7.6% from 32k up, with
non-overlapping ranges, and flagged ~8 s at 131k that the wire could not
account for. That lead does not reproduce. Paired today, TCP at 131k is
134.47 s, not 143.32 s; the RDMA arms of both days agree to 0.2%.

| Depth | TCP 09-17 | TCP 09-18 | Pipe 09-17 | Bulk 09-18 |
|---:|---|---|---|---|
| 16,384 | 13.46–13.49 | 13.16–13.19 | 13.09–13.14 | 13.02–13.07 |
| 32,768 | 28.87–29.06 | 27.08–27.24 | 26.91–27.01 | 26.83–26.92 |
| 65,536 | 62.09–62.45 | 58.03–58.28 | 57.53–57.85 | 57.20–57.80 |
| 131,008 | 143.12–143.91 | 133.91–135.22 | 133.43–134.49 | 133.16–134.16 |

The 09-17 TCP arm was systematically slow — tight ranges, growing with
depth — so it was not noise; but it was the TCP arm that moved, not the
transport comparison. Its cause is not identified. The 09-17 runs were
unpaired (RDMA and TCP arms on either side of a producer restart); the 09-18
runs share one producer and differ only in the receiver's config. Read the
09-17 sweep for what it measured — RDMA working — and this one for the size
of the difference.

## Still open

`ProducerPhaseReceiptV1` is read under a 250 ms deadline after the
generation is live, and at 65,536 tokens the producer reproducibly misses it,
on either transport. muser's own per-handoff payload receipt therefore stops
above some depth between 16k and 64k, which is why the data-leg table above
stops at 16,384.
