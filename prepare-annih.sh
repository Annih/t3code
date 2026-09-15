#!/usr/bin/env bash
# prepare-annih.sh — rebuild the annih branch from upstream + feature + customizations
set -euo pipefail

MAIN="${1:-origin/main}"
shift || true
FEATURES=("$@")

if [ ${#FEATURES[@]} -eq 0 ]; then
  FEATURES=(sidebar_ux_project_grouping glean_provider disable_pull_request_checks)
fi

git checkout annih
git reset --hard "$MAIN"
git merge "${FEATURES[@]}" customization_for_annih
echo "Ready. Review with: git log --oneline -10"
echo "Then: git push --force-with-lease annih annih"

