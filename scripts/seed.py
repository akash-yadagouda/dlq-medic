#!/usr/bin/env python3
"""Emits the demo incident as kafka-console-producer lines: `headers<TAB>key<TAB>value`.

Story:
  1. checkout-service v2.3.0 publishes 500 healthy orders.
  2. v2.3.1 ships a serialization bug. 214 orders break the contract:
       150  amount sent as a string            ("amount": "1234.50")
        48  createdAt sent as epoch millis      ("createdAt": 1790150400000)
        16  customerId missing                  (not fixable: we don't know who to charge)
  3. v2.3.2 hotfix re-publishes 12 of the fixable orders correctly. They are processed,
     so replaying their DLT copies would charge those customers twice.

Deterministic (seeded) so every demo run produces the same incident.
"""
import json
import random
import sys
from datetime import datetime, timedelta, timezone

rng = random.Random(42)
IST = timezone(timedelta(hours=5, minutes=30))
T0 = datetime(2026, 9, 26, 2, 10, 0, tzinfo=IST)


def order(n):
    return {
        "orderId": f"ORD-{10000 + n}",
        "customerId": f"CUST-{rng.randint(1000, 9999)}",
        "amount": round(rng.uniform(199, 9999), 2),
        "currency": "INR",
        "createdAt": (T0 + timedelta(seconds=7 * n)).isoformat(),
    }


def line(evt, version):
    headers = f"producer:checkout-service,producer-version:{version}"
    return f"{headers}\t{evt['orderId']}\t{json.dumps(evt, separators=(',', ':'))}"


out = []
for n in range(1, 501):
    out.append(line(order(n), "2.3.0"))

buckets = ["amount_as_string"] * 150 + ["created_at_epoch_millis"] * 48 + ["missing_customer_id"] * 16
rng.shuffle(buckets)
fixable = []
for i, bucket in enumerate(buckets):
    good = order(501 + i)
    bad = dict(good)
    if bucket == "amount_as_string":
        bad["amount"] = f"{good['amount']:.2f}"
    elif bucket == "created_at_epoch_millis":
        bad["createdAt"] = int(datetime.fromisoformat(good["createdAt"]).timestamp() * 1000)
    else:
        del bad["customerId"]
    if bucket != "missing_customer_id":
        fixable.append(good)
    out.append(line(bad, "2.3.1"))

hotfixed = rng.sample(fixable, 12)
for good in hotfixed:
    out.append(line(good, "2.3.2"))

print("\n".join(out))
print(f"seeded {len(out)} messages: 500 healthy (v2.3.0), 214 broken (v2.3.1: "
      f"150 amount-as-string, 48 epoch createdAt, 16 missing customerId), "
      f"12 hotfix re-sends (v2.3.2): {', '.join(sorted(g['orderId'] for g in hotfixed))}", file=sys.stderr)
