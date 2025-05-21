#!/usr/bin/env bash

DIR_WITHIN_ROOT_DIR="src"


current_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"  # start from script directory

while [[ "$current_dir" != "/" ]]; do
    if [[ -d "$current_dir/$DIR_WITHIN_ROOT_DIR" ]]; then
        cd "$current_dir" || exit 1
        return 0 2>/dev/null || exit 0  # if sourced: return, else exit with success
    fi
    current_dir="$(dirname "$current_dir")"
done


echo "Project root directory not found. Exiting" >&2
exit 1
