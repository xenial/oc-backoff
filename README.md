# oc-backoff

Exponential backoff resilience for [OpenCode V2](https://opencode.ai/v2/docs/). Adds longer retry delays and session recovery for transient upstream inference-provider failures (429s, 5xx, network resets, overload). Targets the plugin API in OpenCode 2.0.26 and compatible 2.x releases.

OpenCode's built-in retry count is bounded. An outage that outlasts those attempts ends the execution. This plugin adds three cooperating layers while the OpenCode service and plugin remain running:

1. **Retry hook** — replaces OpenCode's proposed retry delays with full-jitter exponential backoff, honors `Retry-After`, and fails fast on terminal errors (auth, invalid request, context overflow) instead of masking them.
2. **Per-provider circuit breaker** — after 3 consecutive exhausted retry sequences against one provider, further retries and watchdog re-dispatches are blocked for a cooldown (15 min, doubling to a 2 h ceiling). The cooldown is anchored at the *first* failure of a run, so sibling reports during that cooldown do not extend it. One session at a time can reserve the recovery probe; a successful provider step resets the circuit. Circuit tracking works even with the watchdog disabled.
3. **Watchdog re-dispatch** — when a session's execution fails with a transient error, the session is parked and re-prompted with jittered exponential backoff (cooldown-based, capped at 30 min before provider waits and circuit cooldowns) until it succeeds or the outage tolerance (default **12 h**) is exceeded. The first re-dispatch also waits. Prompt admission is never counted as recovery; execution lifecycle events determine the outcome.

## Install

```sh
opencode plugin add github:xenial/oc-backoff
```

Or configure it directly in `opencode.jsonc` (project or `~/.config/opencode/opencode.jsonc`):

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "plugins": [
    {
      "package": "github:xenial/oc-backoff",
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

All options are optional; the values shown are the defaults. Git packages include TypeScript sources for OpenCode's Bun runtime, so loading does not depend on install scripts being enabled. Standard npm installs and packing also build the JavaScript entrypoint with `prepare`.

### Install with an AI agent

Paste this prompt into OpenCode (or any coding agent) to have it install the plugin for you:

```text
Install the OpenCode plugin @xenial/oc-backoff (repo: https://github.com/xenial/oc-backoff)
globally so it applies to all my sessions. Run `opencode plugin add github:xenial/oc-backoff`,
then restart the background service with `opencode service restart` and confirm the plugin
loads without errors by checking the service logs and `opencode plugin list`.
It is an exponential-backoff resilience plugin:
it overrides retry delays for transient provider failures (429/5xx/network/overload),
adds a per-provider circuit breaker, and re-dispatches parked sessions after outages
up to 12 hours while the service stays running. It cannot keep an already-failed
headless CLI process or parent subagent call waiting. If Git installation fails on a
compatible V2 release, clone the repo, run `npm install && npm run build` inside it,
and add the absolute path to that cloned directory to the `plugins` array in
~/.config/opencode/opencode.jsonc. Then restart the service and verify as above.
```

## Behavior

| Failure | Classification | Policy |
|---|---|---|
| HTTP 429 | rate-limit | jittered exponential backoff, honor `Retry-After` (capped at `maxDelayMs`) |
| HTTP 5xx | server-error | jittered exponential backoff |
| HTTP 408, provider transport/timeouts, `ECONNRESET`, `fetch failed`, … | network | jittered exponential backoff |
| "overloaded" / "exhausted" / "unavailable" | overloaded | jittered exponential backoff |
| HTTP 402, "usage limit" / "quota" messages | quota | retry, but always at the provider-directed wait or `maxDelayMs` — usage windows resetting in an hour are allowed to exceed the transient cap |
| Other HTTP 4xx, authentication, invalid request, context overflow, permission/tool errors, unknown | terminal | never retried, never parked — surfaced immediately |

Retry delays use **full jitter** (`random(0, base · 2ⁿ)` capped at `maxDelayMs`) so parallel agents do not synchronize retries against a shared rate limit. The first retry uses `n = 0`; OpenCode's corresponding physical attempt is `2`. `Retry-After` and `retry-after-ms` are captured from primary and compaction HTTP responses because the V2 retry error omits headers. HTTP date values and header names with different casing are supported. Background title and standalone generation requests do not replace the execution's provider attribution.

The watchdog only re-dispatches sessions whose terminal error is transient-classified. A session that fails for a real reason (bad request, auth, context overflow) is left alone. A parked session that keeps failing gives up after `outageToleranceMs` (12 h by default).

Provider waits also apply to watchdog scheduling; quota reset times may exceed both delay caps. Each session's tolerance deadline stays anchored at its first parked failure, including across failed re-dispatches. Manual prompts, agent/model changes, interruption, deletion, and plugin unload cancel its pending recovery. Unattributed failures are left alone rather than combined into an unknown-provider circuit.

### Scope and limits

The circuit controls the retry hook and watchdog dispatch. An ordinary user request can still send its initial provider call while the circuit is open; the retry API cannot veto that initial request. Circuit state and parked sessions are local to one plugin instance and are cleared on reload or service restart.

The watchdog can resume child sessions, but re-prompting a child does not reopen a parent subagent call that has already reported failure. Likewise, `opencode run` exits on an execution failure; later session recovery does not change that process's exit code or deliver its final output to the exited client. Long-lived automation needs a caller that waits through session recovery. See OpenCode's [headless failure handler](https://github.com/anomalyco/opencode/blob/v2/packages/cli/src/run/noninteractive.ts#L524) and [subagent result handling](https://github.com/anomalyco/opencode/blob/v2/packages/core/src/tool/plugin/subagent.ts#L245).

Re-dispatch appends a continuation prompt to the session history. Recovery is best effort and requires the service and plugin to stay active and lifecycle events to be delivered; the outage deadline bounds waiting, including a stuck dispatch. This plugin does not replay or roll back completed tool actions.

## Logging

Decisions are logged to stderr with the `[oc-backoff]` prefix: classification, proposed vs. applied delay, circuit state changes, park/re-dispatch events.

## Development

```sh
npm install
npm run build
npm test
```

`npm test` rebuilds before testing, so a clean checkout cannot silently test stale `dist` files. `src/backoff.ts` is pure logic (no plugin-API imports) and is unit-tested directly. `test/plugin.test.mjs` exercises `src/index.ts` through mocked SDK hooks and lifecycle streams with deterministic timers, including repeated outages, circuit recovery, provider waits, cancellation, and cleanup. These tests do not call a live inference provider.
