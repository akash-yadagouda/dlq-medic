# Final report format

1. **Root cause**: the producer version and what changed in its messages (one line per error pattern, with counts).
2. **Outcome table**:

| Metric | Count |
|---|---:|
| DLT messages assessed | |
| Replayed (canary + bulk) | |
| Skipped as already processed | |
| Parked for the owning team | |
| Rejected at staging | |

3. **Batch id**, and whether the canary passed (every canary order present with exactly 1 charge).
4. **One recommendation** that would prevent a repeat (for example a contract test in the producer's CI).
