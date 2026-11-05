#!/bin/bash
# Merge per-module Go coverage profiles into a single report.
#
# Go cover profiles from different modules can be concatenated as-is: each
# block line references files by their full import path, so lines from
# different modules never collide. Only the "mode:" header needs deduping.
#
# Usage: merge-go-coverage.sh <output-file> <profile> [<profile>...]

set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "usage: $0 <output-file> <profile> [<profile>...]" >&2
    exit 1
fi

out="$1"
shift

echo "mode: atomic" > "$out"
for profile in "$@"; do
    if [ ! -f "$profile" ]; then
        echo "merge-go-coverage: skipping missing profile: $profile" >&2
        continue
    fi
    grep -v '^mode:' "$profile" >> "$out"
done

echo "merge-go-coverage: wrote $(grep -c -v '^mode:' "$out") coverage blocks to $out"
