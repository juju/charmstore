#!/bin/sh -e

if [ -z "$GOPATH" ]; then
	echo "GOPATH not set"
	exit 1
fi

for vcsdir in .bzr .hg .git; do
	for vcspack in $(find $GOPATH -name $vcsdir); do
		cd $(dirname $vcspack)
		tar czvf .vcspack ${vcsdir}
		rm -rf ${vcsdir}
	done
done

