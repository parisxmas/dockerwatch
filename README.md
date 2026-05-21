# dockerwatch

Realtime TUI for Docker container CPU & memory usage. Like `docker stats`, but
sortable, filterable, with a per-container detail dialog and a one-key `kill`
action.

## Features

- Live container table with **CPU%**, **CORES**, **MEM usage / limit**, **MEM%**
  refreshed every second.
- Per-container CPU is expressed as **% of total host** (not per-core), so
  values sum cleanly. The `CORES` column shows the equivalent core count in use
  (e.g. `0.42`).
- Header totals across all containers: CPU % of host + cores in use, MEM used
  vs host's physical memory.
- Sort by **CPU / MEM / NAME**, cycle or jump directly.
- Substring **filter** on container name and image (`/` to enter).
- **Enter** opens a detail popup with full `docker inspect` fields (image, tag,
  cmd, entrypoint, state, pid, started, networks + IPs, port bindings, restart
  policy) and a live CPU/MEM line. Press **`k`** to send `SIGKILL` after a
  `y/n` confirmation.
- Single ~2 MB static binary (musl) — no runtime deps, drop it on any Linux
  box that has access to the Docker socket.

## Keybindings

| Key            | Action                                  |
| -------------- | --------------------------------------- |
| `↑` / `↓`      | Move selection (also `k` / `j`)         |
| `Enter`        | Open detail popup for selected container|
| `s`            | Cycle sort (CPU → MEM → NAME)           |
| `c` / `m` / `n`| Sort by CPU / MEM / NAME directly       |
| `/`            | Filter by name or image                 |
| `q` / `Esc`    | Quit (or close popup / cancel filter)   |

Inside the detail popup:

| Key            | Action                                  |
| -------------- | --------------------------------------- |
| `k`            | Kill container (asks for confirmation)  |
| `y` / `Enter`  | Confirm kill                            |
| `n` / `Esc`    | Cancel confirmation, or close popup     |

## Install

### Download a prebuilt binary (Linux)

Static musl binaries are attached to each
[GitHub release](https://github.com/parisxmas/dockerwatch/releases/latest) — no
runtime dependencies.

```sh
# x86_64
curl -L -o /usr/local/bin/dockerwatch \
  https://github.com/parisxmas/dockerwatch/releases/latest/download/dockerwatch-x86_64-linux-musl
chmod +x /usr/local/bin/dockerwatch

# or aarch64 (ARM)
curl -L -o /usr/local/bin/dockerwatch \
  https://github.com/parisxmas/dockerwatch/releases/latest/download/dockerwatch-aarch64-linux-musl
chmod +x /usr/local/bin/dockerwatch
```

`SHA256SUMS` is published alongside each release if you want to verify.

### From source (native)

```sh
cargo install --git https://github.com/parisxmas/dockerwatch
```

or clone and build:

```sh
git clone https://github.com/parisxmas/dockerwatch.git
cd dockerwatch
cargo run --release
```

### Cross-compile for Linux from macOS

A static `x86_64-unknown-linux-musl` binary works on any Linux distro without
glibc concerns:

```sh
rustup target add x86_64-unknown-linux-musl
cargo install cargo-zigbuild      # one-time
cargo zigbuild --release --target x86_64-unknown-linux-musl
# -> target/x86_64-unknown-linux-musl/release/dockerwatch  (~2 MB, static)
```

### Deploy to a remote host

```sh
scp -P 22 target/x86_64-unknown-linux-musl/release/dockerwatch \
    user@host:/tmp/dockerwatch.new
ssh -p 22 user@host \
    'mv /tmp/dockerwatch.new /usr/local/bin/dockerwatch && chmod +x /usr/local/bin/dockerwatch'
```

The temp-then-`mv` pattern avoids `ETXTBSY` if anyone has the binary open in
another SSH session.

## Running on a remote host

`dockerwatch` needs a TTY for the TUI, so always pass `-t` to `ssh`:

```sh
ssh -t user@host dockerwatch
```

The current user must be able to read `/var/run/docker.sock` (typically root or
a member of the `docker` group).

## How the numbers are computed

- **CPU%** uses the Docker stats formula `(cpu_delta / system_cpu_delta) * 100`
  — i.e. share of the host's total CPU time. 100% means every core is pinned.
- **CORES** = `CPU% / 100 * host_ncpu`.
- **MEM usage** subtracts page cache (cgroup v1 `cache`, v2 `inactive_file`),
  matching `docker stats`.
- Host totals (`MemTotal`, `NCPU`) come from `docker info`.

## Built with

[ratatui](https://ratatui.rs) · [crossterm](https://docs.rs/crossterm) ·
[bollard](https://docs.rs/bollard) · [tokio](https://tokio.rs)

## License

MIT
