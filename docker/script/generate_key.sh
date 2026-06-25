#!/usr/bin/env bash

set -euo pipefail

# Generate a Fernet key. A Fernet key is simply the URL-safe base64 encoding of
# 32 random bytes, so we generate it with the Python standard library only. This
# avoids depending on the `cryptography` package being importable by the build's
# root python3 (it is installed into the airflow user's site-packages, not root's),
# which previously produced an empty key.
FERNET_KEY="$(python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")"

if [ -z "$FERNET_KEY" ]; then
  echo "ERROR: failed to generate a Fernet key" >&2
  exit 1
fi

# Store it in the image so that it can be used in entrypoint
mkdir -p /usr/local/etc
echo "$FERNET_KEY" > /usr/local/etc/airflow_fernet_key
