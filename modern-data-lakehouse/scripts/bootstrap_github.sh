#!/usr/bin/env bash
# =============================================================================
# scripts/bootstrap_github.sh
# Initialise a local git repo and push to a new GitHub repository.
#
# Usage:
#   chmod +x scripts/bootstrap_github.sh
#   ./scripts/bootstrap_github.sh <github-username> [repo-name]
#
# Requirements:
#   - git installed
#   - GitHub CLI (gh) installed and authenticated, OR a Personal Access Token
#     exported as GH_TOKEN
# =============================================================================

set -euo pipefail

GITHUB_USER="${1:?Usage: $0 <github-username> [repo-name]}"
REPO_NAME="${2:-modern-data-lakehouse}"
BRANCH="main"

echo "==> Initialising git repo..."
git init -b "$BRANCH"
git add .
git commit -m "feat: initial project scaffold — Modern Data Lakehouse Pipeline"

echo "==> Creating remote repo: ${GITHUB_USER}/${REPO_NAME}"

if command -v gh &> /dev/null; then
  gh repo create "${GITHUB_USER}/${REPO_NAME}" \
    --public \
    --description "PySpark ETL pipeline — Snowflake/BigQuery, Airflow orchestration" \
    --source=. \
    --remote=origin \
    --push
  echo "✅  Repository created and pushed via GitHub CLI."
else
  echo "GitHub CLI (gh) not found. Using git remote manually."
  echo "Make sure you have a Personal Access Token or SSH key configured."
  git remote add origin "https://github.com/${GITHUB_USER}/${REPO_NAME}.git"
  git push -u origin "$BRANCH"
  echo "✅  Pushed to https://github.com/${GITHUB_USER}/${REPO_NAME}"
fi

echo ""
echo "🚀  Next steps:"
echo "    1. cd $(pwd)"
echo "    2. cp .env.example .env  && fill in credentials"
echo "    3. make install"
echo "    4. make up   # start Airflow + Spark"
echo "    5. Open http://localhost:8080  (admin / admin)"
