#!/bin/sh -e

if [ -z "$GOPATH" ]; then
	echo "GOPATH not set"
	exit 1
fi

for vcssave in $(find $GOPATH -name .vcspack); do
	cd $(dirname $vcssave); tar xzvf .vcspack
done

