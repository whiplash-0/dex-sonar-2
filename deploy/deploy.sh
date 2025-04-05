#!/bin/bash

scripts_dir="deploy"

project_root_path="$(dirname "$(realpath "$0")")/.."

if cd "$project_root_path"; then  # move to project level directory for reproducibility
  git add *
  are_there_changes=$(git diff --cached --quiet || echo true)
  [ "$are_there_changes" = "true" ] && git commit -m "Ops: Temporal commit for Heroku deployment"

  bash "$scripts_dir/branch_deploy.sh" "$(git branch --show-current)"

  [ "$are_there_changes" = "true" ] && git reset --soft HEAD~1
  [ "$are_there_changes" = "true" ] && git reset
else
    echo "Failed to change directory to project root"
    exit 1
fi
