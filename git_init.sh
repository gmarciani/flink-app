#!/bin/bash

GITLAB_USERNAME="$1"
GITLAB_REPO="$2"

GITHUB_USERNAME="$3"
GITHUB_REPO="$4"

if [ -z ${GITLAB_USERNAME} ]; then
  echo "ERROR: Missing gitlab username"
  echo "USAGE: [gitlab_username] [gitlab_repo] [github_username] [github_repo]"
  exit -1
fi

if [ -z ${GITLAB_REPO} ]; then
  echo "ERROR: Missing gitlab repo"
  echo "Usage: [gitlab_username] [gitlab_repo] [github_username] [github_repo]"
  exit -1
fi

if [ -z ${GITHUB_USERNAME} ]; then
  echo "ERROR: Missing github username"
  echo "Usage: [gitlab_username] [gitlab_repo] [github_username] [github_repo]"
  exit -1
fi

if [ -z ${GITHUB_REPO} ]; then
  echo "ERROR: Missing github repo"
  echo "Usage: [gitlab_username] [gitlab_repo] [github_username] [github_repo]"
  exit -1
fi

git init

git remote add gitlab git@gitlab.com:${GITLAB_USERNAME}/${GITLAB_REPO}.git

git remote add github git@github.com:${GITHUB_USERNAME}/${GITHUB_REPO}.git

git remote add repos git@gitlab.com:${GITLAB_USERNAME}/${GITLAB_REPO}.git

git remote set-url --add --push repos git@gitlab.com:${GITLAB_USERNAME}/${GITLAB_REPO}.git

git remote set-url --add --push repos git@github.com:${GITHUB_USERNAME}/${GITHUB_REPO}.git

touch .gitignore

touch LICENSE

touch README.md

git add --all

git commit -m "first commit"

git push repos master
