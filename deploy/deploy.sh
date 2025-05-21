#!/usr/bin/env bash

SCRIPTS_DIR="deploy"


source "$(dirname "${BASH_SOURCE[0]}")/.cd_to_project_root.sh"

# create temporal commit to include uncommited changes
git add *
are_there_changes=$(git diff --cached --quiet || echo true)
[ "$are_there_changes" = "true" ] && git commit -m "Ops: Temporal commit for Heroku deployment"

bash "$SCRIPTS_DIR/branch_deploy.sh" "$(git branch --show-current)"

[ "$are_there_changes" = "true" ] && git reset --soft HEAD~1
[ "$are_there_changes" = "true" ] && git reset
