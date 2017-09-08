#!/bin/bash

set -e
set -o pipefail

ERRORS=()

# find exec files to run `shellshock`
for f in $(find . -maxdepth 1 -type f -not -iwholename '*.git*' -name '*\.sh' | sort -u); do
	if file "$f" | grep --quiet shell; then
		shellcheck "$f" && echo "[OK]: sucessfully linted $f"
	else
		ERRORS+=("$f")
	fi
done

if [ ${#ERRORS[@]} -eq 0 ]; then
	echo "No errors, hooray"
else
	echo "These files filed shellcheck: ${ERRORS[*]}"
	exit 1
fi
