#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/.cd_to_project_root.sh"
git fetch heroku
git branch -f stable heroku/main
