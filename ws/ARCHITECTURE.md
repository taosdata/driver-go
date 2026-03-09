# WS Architecture

This document describes the current websocket architecture and the intended direction.

## Package Layout

- `ws/client`: low-level websocket client, read/write pumps, send queue, lifecycle flags.
- `ws/stmt`: stmt protocol wrapper and reconnect around `WSConn`.
- `ws/schemaless`: schemaless protocol wrapper with auto-reconnect.
- `ws/tmq`: tmq consumer protocol wrapper with auto-reconnect.
- `ws/internal/reconnect`: shared reconnect safety helpers used by higher-level packages.

## Layering

1. Transport layer
   - Owned by `ws/client`.
   - Responsibilities: send queue, ping/pong, close signaling, last error tracking.
2. Protocol layer
   - Owned by `stmt`, `schemaless`, `tmq`.
   - Responsibilities: encode request, route response by request id, parse protocol payload.
3. Recovery layer
   - Auto-reconnect orchestration and client swap safety checks.
   - Shared safety invariants live in `ws/internal/reconnect`.
   - `tmq` and `schemaless` consume shared replacement/cleanup helpers.
   - `stmt` follows the same invariants with package-local implementation.

## Request Flow (schemaless/tmq)

1. Build request envelope.
2. Load current client pointer.
3. Register response channel keyed by request id.
4. Send envelope via client send queue.
5. Wait on response channel, close signal, client done, or timeout.
6. On close/network error with auto-reconnect enabled, reconnect and retry once.

## Reconnect Flow (schemaless/tmq)

1. Enter reconnect lock.
2. If object closed, return closed error.
3. Short-circuit only when a replacement client exists and is still running.
4. Retry dial/bootstrap up to configured count.
5. On successful new client:
   - Replace current pointer atomically.
   - Close replaced old client.
   - For tmq, resubscribe topics.
6. On failure, clear and close only if current client still matches failed client.

## Current Improvement Direction

1. Keep reconnect safety rules centralized in `ws/internal/reconnect`.
2. Move more duplicated send/retry skeleton into shared helpers where behavior is identical.
3. Keep package-specific protocol parsing isolated; do not over-abstract protocol semantics.
