#!/usr/bin/env python3
"""Headless trigger for the dlq-medic agent: the same TrueForge API an alerting system would call.

  scripts/run-agent.py --msg "orders.DLT is filling up, sort it out"                # stops at the first approval
  scripts/run-agent.py --msg "..." --on-approval ask                                 # prompt on the terminal
  scripts/run-agent.py --session <id> --msg "..."                                     # continue a session

Prints every tool call and result, each approval pause, and the token usage of the run.
"""
import argparse
import json
import os
import sys
import time
import urllib.request

BASE = os.environ.get("TRUEFORGE_URL", "http://localhost:8790") + "/api/v1"
# USD per 1M tokens: (input, cached input, output)
PRICES = {"gpt-5-6-terra": (2.00, 0.20, 12.00), "gpt-5-6-luna": (0.20, 0.02, 1.20), "gpt-5-6-sol": (4.00, 0.40, 20.00)}


def request(method, path, body=None, stream=False):
    req = urllib.request.Request(BASE + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json",
                                          "Accept": "text/event-stream" if stream else "application/json"})
    resp = urllib.request.urlopen(req, timeout=1800)
    return resp if stream else json.load(resp)


def sse_events(resp):
    data = []
    for raw in resp:
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        if line.startswith("data:"):
            data.append(line[5:].lstrip())
        elif not line and data:
            try:
                event = json.loads("\n".join(data))
                yield event.get("event", event)
            except json.JSONDecodeError:
                pass
            data = []


class Usage:
    def __init__(self):
        self.input = self.cached = self.output = 0

    def add(self, usage):
        if not usage:
            return
        self.input += usage.get("input_tokens") or 0
        self.output += usage.get("output_tokens") or 0
        self.cached += usage.get("cache_read_tokens") or 0

    def cost(self, model):
        price = next((p for name, p in PRICES.items() if name in model), None)
        if price is None:
            return None
        return ((self.input - self.cached) * price[0] + self.cached * price[1] + self.output * price[2]) / 1e6


def print_turn(session_id, turn_id, usage):
    events = request("GET", f"/sessions/{session_id}/turns/{turn_id}/events")["data"]
    events = sorted((e.get("event", e) for e in events), key=lambda e: e.get("id", ""))
    names = {}
    for ev in events:
        kind = ev.get("type")
        tag = "" if (ev.get("thread_id") or "main") == "main" else f"[{ev['thread_id']}] "
        if kind == "sandbox.created":
            print(f"  {tag}[sandbox created]")
        elif kind == "model.message":
            usage.add(ev.get("usage"))
            for call in ev.get("tool_calls") or []:
                fn = call.get("function", call)
                names[call["id"]] = fn.get("name")
                print(f"  {tag}→ {fn.get('name')}({str(fn.get('arguments'))[:600]})")
            if ev.get("content"):
                print(f"  {tag}AGENT: {ev['content']}")
        elif kind == "tool.response":
            content = ev.get("content")
            content = content if isinstance(content, str) else json.dumps(content)
            print(f"  {tag}← {names.get(ev.get('tool_call_id'), '?')}: {content[:600]}")
        elif kind == "tool.approval_required":
            for ref in ev["tool_calls"]:
                print(f"  {tag}⏸  APPROVAL REQUIRED: {names.get(ref['id'], ref['id'])}")


def run_turn(session_id, inputs, previous_turn_id, usage):
    body = {"input": inputs, "stream": True}
    if previous_turn_id:
        body["previous_turn_id"] = previous_turn_id
    turn_id, pending, state = None, [], {}
    for ev in sse_events(request("POST", f"/sessions/{session_id}/turns", body, stream=True)):
        if ev.get("type") == "turn.created":
            turn_id = ev.get("turn_id")
        elif ev.get("type") == "tool.approval_required":
            pending += [(ev["thread_id"], ref["id"]) for ref in ev["tool_calls"]]
        elif ev.get("type") == "turn.done":
            state = ev.get("state", {})
    print_turn(session_id, turn_id, usage)
    print(f"  [turn {state.get('status')}] {state.get('message', '')[:300]}")
    return turn_id, pending, state


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--agent", default="dlq-medic")
    ap.add_argument("--session")
    ap.add_argument("--msg", required=True)
    ap.add_argument("--on-approval", default="stop", choices=["stop", "allow", "deny", "ask"])
    ap.add_argument("--deny-reason", default="Denied by operator.")
    args = ap.parse_args()

    session_id = args.session or request("POST", "/sessions", {"agent": {"name": args.agent}})["data"]["id"]
    model = request("GET", f"/sessions/{session_id}")["data"].get("agent", {}).get("manifest", {}).get("model", {}).get("name", "")
    print(f"session {session_id}  model {model or '?'}")
    usage, started = Usage(), time.time()
    turn_id, pending, _ = run_turn(session_id, [{"type": "user.message", "content": args.msg}], None, usage)
    while pending and args.on_approval != "stop":
        decision = args.on_approval
        reason = args.deny_reason
        if decision == "ask":
            answer = input("  approve? [y/N/reason to deny]: ").strip()
            decision, reason = ("allow", None) if answer.lower() in ("y", "yes") else ("deny", answer or "Denied by operator.")
        approval = {"status": "allow"} if decision == "allow" else {"status": "deny", "reason": reason}
        print(f"  ▶ {approval}")
        items = [{"type": "user.tool_approval", "thread_id": th, "tool_call_id": tc, "approval": approval} for th, tc in pending]
        turn_id, pending, _ = run_turn(session_id, items, turn_id, usage)
    if pending:
        print(f"  (waiting for approval: approve in the TrueForge UI, or re-run with --session {session_id})")
    cost = usage.cost(model)
    print(f"\nusage: {usage.input:,} input ({usage.cached:,} cached), {usage.output:,} output tokens"
          + (f"  ≈ ${cost:.3f}" if cost is not None else "") + f"  in {time.time() - started:.0f}s")


if __name__ == "__main__":
    sys.exit(main())
