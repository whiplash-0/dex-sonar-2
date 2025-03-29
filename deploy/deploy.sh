#!/bin/bash

scripts_dir="deploy"

project_root_path="$(dirname "$(realpath "$0")")/.."

if cd "$project_root_path"; then  # move to project level directory for reproducibility
  git add *
  git commit -m "Ops: Temporal commit for Heroku deployment"
  bash "$scripts_dir/branch_deploy.sh" "$(git branch --show-current)"
  git reset --soft HEAD~1
  git reset
else
    echo "Failed to change directory to project root"
    exit 1
fi
