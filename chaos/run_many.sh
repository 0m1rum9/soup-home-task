#!/usr/bin/env bash
set -u

count="${1:-20}"
pass=0 fail=0 invalid=0
declare -A broken=()
base_seed="$(date +%s)"

for ((number=1; number<=count; number++)); do
  seed="$((base_seed + number))"
  echo "=== run $number/$count seed=$seed ==="
  uv run --project chaos python chaos/harness.py --seed "$seed"
  code=$?
  case "$code" in
    0) ((pass+=1)) ;;
    1) ((fail+=1)) ;;
    *) ((invalid+=1)) ;;
  esac
  run="$(tail -n1 runs/INDEX.tsv | cut -f1)"
  result="runs/$run/result.json"
  if [[ -f "$result" ]]; then
    while read -r name; do broken["$name"]=1; done < <(
      awk '/"I[1-5]":/{name=$1; gsub(/[":]/, "", name)} /"status": "FAIL"/{print name}' "$result"
    )
  fi
done

echo "PASS=$pass FAIL=$fail INVALID=$invalid"
echo "failed invariants: ${!broken[*]:-none}"
