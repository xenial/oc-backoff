# opencode-backoff

Exponential backoff resilience for [OpenCode V2](https://opencode.ai/v2/docs/). Prevents transient upstream inference-provider failures (429s, 5xx, network resets, overload) from bricking unattended agents.

OpenCode's built-in retry policy is hard-capped at a handful of attempts with a total budget of about a minute. That is right for interactive use, but a provider outage longer than ~60 seconds terminates the session and any unattended run — headless `opencode run`, subagents, swarm workers — is bricked until a human restarts it. This plugin adds three cooperating layers on top:

1. **Retry hook** — replaces OpenCode's proposed retry delays with full-jitter exponential backoff, honors `Retry-After`, and fails fast on terminal errors (auth, invalid request, context overflow) instead of masking them.
2. **Per-provider circuit breaker** — after 3 consecutive exhausted retry sequences against one provider, new requests fail fast for a cooldown (15 min, doubling to a 2 h ceiling) instead of every session burning its attempt budget against a dead provider. Sibling sessions reporting the same outage during a cooldown do not extend it.
3. **Watchdog re-dispatch** — when a session's retries are exhausted on a transient failure, the session is parked and re-prompted with exponential backoff (cooldown-based, capped at 30 min) until the provider recovers or the outage tolerance (default **12 h**) is exceeded. This covers outages far beyond OpenCode's attempt budget and applies to all sessions, including subagents.

## Install

```jsonc
// opencode.jsonc (project or ~/.config/opencode/opencode.jsonc)
{
  "$schema": "https://opencode.ai/config.json",
  "plugins": [
    {
      "package": "/absolute/path/to/opencode-backoff",
      "options": {
        "baseDelayMs": 2000,
        "maxDelayMs": 300000,
        "circuit": {
          "failureThreshold": 3,
          "baseCooldownMs": 900000,
          "maxCooldownMs": 7200000
        },
        "watchdog": {
          "enabled": true,
          "maxRedispatchDelayMs": 1800000,
          "outageToleranceMs": 43200000
        }
      }
    }
  ]
}
```

All options are optional; the values shown are the defaults.

## Behavior

| Failure | Classification | Policy |
|---|---|---|
| HTTP 429 | rate-limit | jittered exponential backoff, honor `Retry-After` (capped at `maxDelayMs`) |
| HTTP 5xx | server-error | jittered exponential backoff |
| `ECONNRESET`, timeouts, `fetch failed`, … | network | jittered exponential backoff |
| "overloaded" / "exhausted" / "unavailable" | overloaded | jittered exponential backoff |
| HTTP 402, "usage limit" / "quota" messages | quota | retry, but always at the provider-directed wait or `maxDelayMs` — usage windows resetting in an hour are allowed to exceed the transient cap |
| HTTP 400/401/403/404/422, context overflow, unknown | terminal | never retried, never parked — surfaced immediately |

Delays use **full jitter** (`random(0, base · 2ⁿ)` capped at `maxDelayMs`) so a fleet of parallel agents does not synchronize retries against a shared rate limit.

The watchdog only re-dispatches sessions whose terminal error is transient-classified. A session that fails for a real reason (bad request, auth, context overflow) is left alone. A parked session that keeps failing gives up after `outageToleranceMs` (12 h by default).

## Logging

Decisions are logged to stderr with the `[opencode-backoff]` prefix: classification, proposed vs. applied delay, circuit state changes, park/re-dispatch events.

## Development

```sh
npm install
npm run build
npm test
```

`src/backoff.ts` is pure logic (no plugin-API imports) and is unit-tested directly. `src/index.ts` wires it into the plugin hooks.
