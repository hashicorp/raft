#!/usr/bin/env bash
set -e

# The version must be supplied from the environment. Do not include the
# leading "v".
if [ -z $VERSION ]; then
    echo "Please specify a version."
    exit 1
fi

# Generate the tag.
echo "==> Tagging version $VERSION..."
git commit --allow-empty -a --gpg-sign=0A50EAFC -m "Release v$VERSION"
git tag -a -m "Version $VERSION" -s -u 0A50EAFC "v${VERSION}" master

exit 0
