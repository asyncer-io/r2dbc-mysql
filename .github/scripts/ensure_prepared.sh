#!/bin/zsh

set -e

TAG=$(grep scm.tag= release.properties | cut -d'=' -f2)
echo "checkout tag $TAG"
git checkout "$TAG"
exit 0