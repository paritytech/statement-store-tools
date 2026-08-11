# Statement Store Latency Benchmark

CLI tools for benchmarking statement store latency at scale. The ring-topology binary
(`statement-latency-bench`) measures aggregate cohort latency; the per-node binary
(`statement-ops-bench`) measures individual RPC operations on specific nodes.

This crate produces three binaries:
- **`setup-allowances`** — one-shot provisioning of on-chain statement allowances via Sudo
- **`statement-latency-bench`** — cohort/ring-topology latency benchmark
- **`statement-ops-bench`** — per-node operation benchmark (submit / propagation / subscribe / loop / query) plus account-quota admin (show-quota / set-quota)

## Building

```bash
cargo build --release
```

## Setup: Statement Allowances

Before running the benchmark, each account needs an on-chain statement allowance. Run this once
(or whenever you change `--num-clients`):

```bash
setup-allowances \
  --rpc-endpoints ws://localhost:9944 \
  --sudo-seed "//Alice" \
  --num-clients 100
```

This submits `Sudo(batch_all(set_storage(...)))` transactions to write allowances for all
deterministic benchmark accounts, then verifies each allowance exists at the finalized block.

### setup-allowances Arguments

| Argument                 | Description                                      | Default   |
| ------------------------ | ------------------------------------------------ | --------- |
| `--rpc-endpoints`        | Comma-separated WebSocket URLs (required)        | -         |
| `--sudo-seed`            | Sudo seed/SURI, e.g. "//Alice" (required)        | -         |
| `--num-clients`          | Number of accounts to provision                  | 100       |
| `--allowance-batch-size` | Accounts per `set_storage` call                  | 100       |
| `--allowance-max-count`  | Max statements allowed per account               | 100000    |
| `--allowance-max-size`   | Max total statement bytes per account            | 1000000   |
| `--max-batch-calls`      | Max calls per `batch_all` transaction            | 100       |

## Running the Benchmark

Basic example:

```bash
statement-latency-bench \
  --rpc-endpoints ws://localhost:9944,ws://localhost:9945 \
  --num-clients 10 \
  --messages-pattern "5:512"
```

Multi-round with custom settings:

```bash
statement-latency-bench \
  --rpc-endpoints ws://node1:9944,ws://node2:9944 \
  --num-clients 100 \
  --num-rounds 10 \
  --interval-ms 5000 \
  --messages-pattern "5:512,1:5120"
```

### statement-latency-bench Arguments

| Argument                | Description                                         | Default |
| ----------------------- | --------------------------------------------------- | ------- |
| `--rpc-endpoints`       | Comma-separated WebSocket URLs (required)           | -       |
| `--num-clients`         | Number of clients to spawn                          | 100     |
| `--messages-pattern`    | Message pattern "count:size" (e.g., "5:512,3:1024") | "5:512" |
| `--num-rounds`          | Number of benchmark rounds                          | 1       |
| `--interval-ms`         | Interval between rounds (ms)                        | 10000   |
| `--receive-timeout-ms`  | Timeout for receiving messages (ms)                 | 5000    |
| `--statement-expiry-ms` | Statement expiry time (ms)                          | 600000  |
| `--skip-sync`           | Skip time synchronization (for local testing)       | false   |

## How It Works

1. Clients are distributed round-robin across RPC endpoints
2. Each client sends statements with unique topics
3. Each client subscribes to statements from the next client in the ring
4. Latency is measured from submission to receipt via subscription

## Output

Results are logged with min/avg/max statistics for:
- Send duration
- Receive duration
- Full latency

Example output:
```
Benchmark Results: send_min=0.045s send_avg=0.123s send_max=0.234s receive_min=2.134s receive_avg=3.456s
receive_max=5.678s latency_min=2.234s latency_avg=3.567s latency_max=5.789s
```

## Per-Node Operation Benchmark (`statement-ops-bench`)

`statement-ops-bench` measures individual statement-store RPC operations on specific nodes.

Each benchmarking subcommand requires a signing key with an on-chain statement allowance;
`query` is read-only and needs none.

### Shared arguments

`submit`, `propagation`, `subscribe`, and `loop` all accept:

| Argument             | Description                                    | Default                             |
| -------------------- | ---------------------------------------------- | ----------------------------------- |
| `--seed`             | SURI/seed phrase used to sign statements       | derived from `//StatementClient//0` |
| `--message-size`     | Statement payload size (bytes)                 | 512                                 |
| `--base-expiry-secs` | Statement expiry offset from "now" (seconds)   | 600                                 |

### `submit` — per-node submit duration

Sequentially submits N statements to each endpoint, with a distinct channel per statement
and a strictly increasing expiry. Reports per-node min/avg/max.

```bash
statement-ops-bench submit \
  --rpc-endpoints ws://node1:9944,ws://node2:9944 \
  --iterations 100 \
  --message-size 512 \
  --seed "your account seed"
```

| Argument            | Description                                                | Default   |
| ------------------- | ---------------------------------------------------------- | --------- |
| `--rpc-endpoints`   | Comma-separated WebSocket URLs (required)                  | -         |
| `--iterations`      | Timing samples per endpoint                                | 1         |
| `--iteration-batch` | Submits issued in parallel per timing sample               | 1         |
| `--topic`           | 32-byte hex topic for every statement                      | derived   |

With `--iteration-batch B`, each timing sample kicks off B submits in parallel on the
same ws connection and records the wall-clock from kick-off to all-completed as one
sample — so a sample measures pipelined throughput rather than a single round-trip.
Total submissions per endpoint is `--iterations × --iteration-batch`. The default of `1`
gives one submit per sample.

```bash
# 20 samples of 50 pipelined submits each = 1000 submissions per endpoint
statement-ops-bench submit \
  --rpc-endpoints ws://node1:9944 \
  --iterations 20 \
  --iteration-batch 50 \
  --seed "your account seed"
```

### `propagation` — submit→subscribe latency for each pair

For every (submit-endpoint, subscribe-endpoint) pair in the Cartesian product, opens
**two separate** ws connections (one per side), submits a statement, and measures the
time until the subscription receives it. Same-node pairs (submit and subscribe on the
same node, on independent connections) are included.

```bash
statement-ops-bench propagation \
  --submit-endpoints ws://node1:9944,ws://node2:9944 \
  --subscribe-endpoints ws://node1:9944,ws://node3:9944 \
  --iterations 10 \
  --message-size 512 \
  --seed "your account seed"
```

| Argument                 | Description                                            | Default |
| ------------------------ | ------------------------------------------------------ | ------- |
| `--submit-endpoints`     | Comma-separated WebSocket URLs to submit on (required) | -       |
| `--subscribe-endpoints`  | Comma-separated WebSocket URLs to read on (required)   | -       |
| `--iterations`           | Iterations per (submit, subscribe) pair                | 1       |
| `--drain-timeout-ms`     | Max wait for the initial subscription dump (ms)        | 2000    |
| `--receive-timeout-ms`   | Max wait for the propagated statement (ms)             | 5000    |
| `--topic`                | 32-byte hex topic for every iteration                  | derived |

With `--topic`, the initial dump may legitimately contain prior matching statements;
those are drained and excluded from the propagation timer.

### `subscribe` — per-node retrieval latency

For each endpoint, ensures a matching seed statement exists and then opens
`--reads-per-node` subscriptions filtered to its topic. Latency is measured from
subscribe-open to receipt of the initial dump containing the seed.

Seed handling:
- **No `--topic`**: the topic is derived per run and is guaranteed unique, so
  the seed is always submitted.
- **With `--topic`**: the seed step is **skipped** (`seed=NotSeeded` in the log);
  the read step then succeeds only if a matching statement is already in the
  store (or arrives live within the drain timeout), and otherwise times out
  cleanly with `first_error="Timed out waiting..."`.

```bash
statement-ops-bench subscribe \
  --rpc-endpoints ws://node1:9944,ws://node2:9944 \
  --reads-per-node 10 \
  --message-size 512 \
  --seed "your account seed"

# Read an existing statement under a known topic without writing a new one
# (no seed is ever submitted when --topic is set; reads time out if nothing
# matches):
statement-ops-bench subscribe \
  --rpc-endpoints ws://node1:9944 \
  --reads-per-node 10 \
  --topic 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef \
  --seed "your account seed"
```

| Argument             | Description                                              | Default |
| -------------------- | -------------------------------------------------------- | ------- |
| `--rpc-endpoints`    | Comma-separated WebSocket URLs (required)                | -       |
| `--reads-per-node`   | Read operations per endpoint                             | 1       |
| `--settle-ms`        | Wait after seeding before issuing reads (ms)             | 100     |
| `--drain-timeout-ms` | Max wait for the initial subscription dump (ms)          | 2000    |
| `--topic`            | 32-byte hex topic to seed with and filter reads to       | derived |
| `--assert-once`      | Assert exactly-once delivery (requires `--topic`)        | false   |

#### `--assert-once` — check exactly-once delivery

Turns the read into a correctness check: each read drains the initial dump, then watches
one further `--drain-timeout-ms` idle window for a duplicate or a live re-delivery. A read
passes only if it received the statement exactly once, and the process exits non-zero
unless every read on every endpoint passed — so it can gate a CI step or a shell script.

Absent, duplicated, and multiple-distinct-statement reads all fail. Since it needs a known
topic to filter on, `--topic` is required.

```bash
statement-ops-bench subscribe \
  --rpc-endpoints ws://node1:9944,ws://node2:9944 \
  --reads-per-node 5 \
  --topic 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef \
  --assert-once \
  --seed "your account seed"
```

The summary line spells out the verdict:
```
subscribe endpoint=ws://node1:9944 ok=5 fail=0 min=0.0031s avg=0.0052s max=0.0114s n=5 seed=NotSeeded assert_once=PASS: received the statement exactly once on each of 5 reads
```

### `loop` — periodic execution

Periodically runs `submit`, `propagation`, and `subscribe` across the provided
endpoints (used as both submit and subscribe sides for propagation). Stops when
`--iterations`, `--duration-secs`, or Ctrl-C arrives first.

```bash
statement-ops-bench loop \
  --rpc-endpoints ws://node1:9944,ws://node2:9944 \
  --interval-secs 30 \
  --iterations 10 \
  --submit-iterations 50 \
  --propagation-iterations 5 \
  --reads-per-node 5 \
  --seed "your account seed"
```

| Argument                         | Description                                        | Default   |
| -------------------------------- | -------------------------------------------------- | --------- |
| `--rpc-endpoints`                | Comma-separated WebSocket URLs (required)          | -         |
| `--interval-secs`                | Interval between cycles                            | 30        |
| `--iterations`                   | Maximum number of cycles                           | unbounded |
| `--duration-secs`                | Maximum duration of the loop                       | unbounded |
| `--submit-iterations`            | Per-cycle submit iterations, per endpoint          | 1         |
| `--propagation-iterations`       | Per-cycle propagation iterations, per pair         | 1         |
| `--reads-per-node`               | Per-cycle reads per endpoint                       | 1         |
| `--drain-timeout-ms`             | Drain timeout for the propagation portion (ms)     | 2000      |
| `--receive-timeout-ms`           | Receive timeout for the propagation portion (ms)   | 5000      |
| `--subscribe-drain-timeout-ms`   | Drain timeout for the subscribe portion (ms)       | 2000      |
| `--settle-ms`                    | Settle time before reads in the subscribe portion  | 100       |
| `--new-connection-per-iteration` | Reconnect at the start of every cycle              | true      |

Topics are always derived per cycle; `loop` takes no `--topic`, and its subscribe portion
never runs the `--assert-once` check.

`--new-connection-per-iteration` takes an explicit boolean and defaults to **true**: each
cycle after the first opens a fresh WebSocket connection to every endpoint, which exercises
connection setup and keeps a long run from riding on one aging connection. Pass `false` to
reuse the connections opened at startup:

```bash
statement-ops-bench loop \
  --rpc-endpoints ws://node1:9944 \
  --duration-secs 3600 \
  --new-connection-per-iteration false \
  --seed "your account seed"
```

### `query` — list store contents sorted by expiry

Lists the statements currently held by each node, sorted by the time they will
expire (soonest first). Read-only: nothing is submitted and no seed is needed.

The public statement RPC has no dump/get method, so the command opens a
`statement_subscribeStatement` subscription per endpoint, collects the initial
dump (the replay of everything already in the store that matches the filter)
and unsubscribes at the dump boundary. The result is a point-in-time snapshot.

```bash
# All statements on each node, soonest-to-expire first
statement-ops-bench query \
  --rpc-endpoints ws://node1:9944,ws://node2:9944

# Only statements carrying a known topic
statement-ops-bench query \
  --rpc-endpoints ws://node1:9944 \
  --topic 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef
```

| Argument             | Description                                       | Default        |
| -------------------- | ------------------------------------------------- | -------------- |
| `--rpc-endpoints`    | Comma-separated WebSocket URLs (required)         | -              |
| `--topic`            | List only statements carrying this 32-byte hex topic | whole store |
| `--drain-timeout-ms` | Max gap between consecutive dump events (ms)      | 15000          |

Example output:
```
query endpoint=ws://node1:9944 filter=any n=2 undecodable=0 (sorted by expiry, soonest first)
  [0] hash=0x90b3... expires_at=1765432100 (expires in 9m32s) seq=3 data_len=512 account=0x1c5e... channel=0x7d01... topics=[0xdead...]
  [1] hash=0x44af... expires_at=1765432700 (expires in 19m32s) seq=0 data_len=512 account=0x1c5e... channel=none topics=[0xbeef...]
```

`--drain-timeout-ms` (default 15000) bounds the gap between consecutive dump
events, not the total dump duration. Dump events carry up to ~4 MiB of
statements each, so the first event of a large dump can take several seconds
to arrive on WAN links; raise the timeout if a busy node still times out.
Per-endpoint failures are logged and skipped; the command exits non-zero only
if every endpoint fails.

### Output format

Per-endpoint and per-pair lines are logged at INFO level. Examples:
```
submit endpoint=ws://A ok=100 fail=0 min=0.0042s avg=0.0061s max=0.0123s n=100
propagation submit_endpoint=ws://A subscribe_endpoint=ws://B ok=10 fail=0 prop_min=0.012s prop_avg=0.034s prop_max=0.087s submit_avg=0.005s n=10
subscribe endpoint=ws://A ok=10 fail=0 min=0.003s avg=0.005s max=0.011s n=10 seed=Submitted
```

## Managing Account Quotas (`admin`)

Each account's statement-store quota is an on-chain **allowance**
(`StatementAllowance { max_count, max_size }`) stored under the unhashed key
`":statement_allowance:" ++ account_id`. An account with **no allowance** (or a
zeroed one) has every statement submission rejected, and any statements it already
has in the store are automatically evicted by the nodes.

The `statement-ops-bench admin` subcommand reads and sets this quota:

- **`show-quota`** — read-only (`state_getStorage`); needs no key.
- **`set-quota`** — writes via a `Sudo(System.set_storage)` extrinsic, so it needs
  the chain's **sudo key**. It waits for finalization and reads the value back to verify.

The account is given as an SS58 address **or** a 32-byte hex id (with or without `0x`).

**`admin set-quota` vs `setup-allowances`** — both write the same
`":statement_allowance:" ++ account_id` storage key via Sudo, and differ only in who they
target. `setup-allowances` provisions the whole cohort of deterministic benchmark accounts
in batches, so it is the one to run before `statement-latency-bench`. `admin set-quota`
targets **one** account named on the command line, which makes it the tool for inspecting
or repairing a single account, for granting an allowance to an account outside the
benchmark cohort, and for the quota-zero eviction procedure below.

### 1. Read an account's quota

```bash
statement-ops-bench admin show-quota \
  --rpc-endpoint ws://localhost:9944 \
  --account 5DA1vuQ442HZQMkpk5idSUiQGrhJ6fkzW1rxETB7QviHvVRi
```

| Argument         | Description                                        | Default |
| ---------------- | -------------------------------------------------- | ------- |
| `--rpc-endpoint` | Single WebSocket URL (required)                    | -       |
| `--account`      | SS58 address or 32-byte hex id (required)          | -       |

Prints one of:
```
account=30494cb9… quota: max_count=100000 max_size=1000000
account=30494cb9… quota: max_count=0 max_size=0 (depleted)
account=30494cb9… has NO quota set
```

### 2. Set an account's quota

```bash
statement-ops-bench admin set-quota \
  --rpc-endpoint ws://localhost:9944 \
  --account 5DA1vuQ442HZQMkpk5idSUiQGrhJ6fkzW1rxETB7QviHvVRi \
  --sudo-seed //Alice \
  --max-count 100000 \
  --max-size 1000000
```

It submits the sudo extrinsic, waits for it to finalize, and re-reads the value;
a successful run prints the new quota.

| Argument         | Description                                        | Default |
| ---------------- | -------------------------------------------------- | ------- |
| `--rpc-endpoint` | Single WebSocket URL (required)                     | -       |
| `--account`      | SS58 address or 32-byte hex id (required)            | -       |
| `--max-count`    | Max statements allowed for the account (required)    | -       |
| `--max-size`     | Max total statement bytes for the account (required) | -       |

**Providing the sudo key** — exactly one source is required:

| Flag | Source |
| ---- | ------ |
| `--sudo-seed <SURI>`      | inline SURI (`//Alice`, a mnemonic, a `0x`-hex seed); also read from `STATEMENT_SUDO_SEED` |
| `--sudo-seed-file <PATH>` | a file holding a SURI, or a substrate node keystore key file |
| `--sudo-json <PATH>`      | a Polkadot-JS encrypted JSON account backup |

For `--sudo-json`, supply the unlock password with `--sudo-password`,
`STATEMENT_SUDO_PASSWORD`, `--sudo-password-file <PATH>`, or
`--sudo-password-interactive` (hidden prompt).

### 3. Clear all stored statements for an account

Nodes automatically evict any statements that exceed an account's quota, so setting the
quota to **0** effectively removes all of that account's statements from every node. The
procedure is to read and record the current quota, set it to 0, wait ~5 minutes for the
statements to be removed from all nodes, then **restore the original quota**.

> ⚠️ **The restore step (4) is mandatory.** While the quota is 0 the account cannot
> submit any statements — every submission is rejected. If you forget to restore it,
> the account is effectively bricked until a non-zero quota is set again. **Record the
> original values in step 1 before changing anything.**

**Step 1 — read and RECORD the current quota** (you need these to restore):
```bash
statement-ops-bench admin show-quota \
  --rpc-endpoint ws://localhost:9944 \
  --account "$ACCOUNT"
# Note the printed max_count and max_size, e.g. 100000 / 1000000.
```

**Step 2 — set the quota to 0** (causes each node to evict the account's statements):
```bash
statement-ops-bench admin set-quota \
  --rpc-endpoint ws://localhost:9944 \
  --account "$ACCOUNT" \
  --sudo-seed //Alice \
  --max-count 0 --max-size 0
```

**Step 3 — wait ~5 minutes** for the statements to be removed from all nodes. Optionally
confirm with `query` that the account no longer appears:
```bash
statement-ops-bench query --rpc-endpoints ws://localhost:9944
```

**Step 4 — restore the original quota (VERY IMPORTANT):**
```bash
statement-ops-bench admin set-quota \
  --rpc-endpoint ws://localhost:9944 \
  --account "$ACCOUNT" \
  --sudo-seed //Alice \
  --max-count 100000 --max-size 1000000   # the values recorded in step 1
```

After step 4, `show-quota` again reports the original `max_count` / `max_size` and
the account can submit statements again.
