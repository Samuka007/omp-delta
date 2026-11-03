#!/bin/bash
# #11 differential probe: append one record as a dedicated fresh producer; verdict lands
# in probe-results.log next to this script (model-independent evidence).
# Usage: probe-append.sh <LEG> <THREAD_ID> [EXTERNAL_ID]
# Verdict: RECORDED | NOTFOUND | GAP | OTHER: <err>
export PATH="$PATH:/c/Users/Samuka007/AppData/Local/Programs/Delta/bin"
LEG="${1:?leg required}"; TID="${2:?thread id required}"; EXT="omp-probe11-${LEG}-$$"
OUT=$(printf '{"schemaVersion":1,"sequence":1,"update":{"sessionUpdate":"user_message","messageId":"u-p11-%s","content":[{"type":"text","text":"#11 probe"}],"_meta":{"delta":{"work_origin":"external_agent","interaction_surface":"cli","activity_kind":"user_message"}}}}\n' "$$" \
| delta --json thread append "$TID" --source omp --external-id "$EXT" --producer-name omp-probe 2>&1)
case "$OUT" in
  *"disposition"*|*"operation\":\"appended\"") V=RECORDED;;
  *"history is unavailable"*|*"not_visible"*) V=NOTFOUND;;
  *"sequence_gap"*) V=GAP;;
  *) V="OTHER: $(echo "$OUT" | tail -1 | head -c 200)";;
esac
echo "leg=$LEG verdict=$V ext=$EXT time=$(date +%H:%M:%S)" >> "$(dirname "$0")/probe-results.log"
echo "$V"
