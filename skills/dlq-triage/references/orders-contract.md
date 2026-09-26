# Orders contract and vetted fixes

## Contract (what order-consumer accepts on the `orders` topic)
| Field | Rule |
|---|---|
| orderId | non-blank string. Never changes during a fix. |
| customerId | non-blank string. Required: if missing it cannot be inferred, so the message is unfixable. |
| amount | JSON **number** > 0 (not a string) |
| currency | non-blank string of 3 letters (ISO-4217) |
| createdAt | ISO-8601 **string** with a UTC offset, e.g. `2026-09-26T03:08:27+05:30` |

A fix may change a value's **format**, never its meaning: the amount value, currency, customer and instant in time must stay the same.

## Vetted server fixes
The server applies these by name in `stage_replay`, then re-validates the result against the contract. Only these exist.

| Fix | Applies when | Result |
|---|---|---|
| `amount_string_to_number` | `amount` is a string holding a decimal, e.g. `"1214.29"` | `1214.29` (number) |
| `epoch_millis_to_iso8601` | `createdAt` is an integer of epoch milliseconds | ISO-8601 string. Needs `utcOffset`: take it from valid `createdAt` values in the DLT (the same producer). |

Anything else (for example a missing customerId) has no vetted fix: park it with a reason.
