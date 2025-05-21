#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/.cd_to_project_root.sh"
heroku config:set $(tr '\n' ' ' <<< "$(cat .env)")
git push -f heroku "${1:-main}":main
heroku ps:scale worker=1
