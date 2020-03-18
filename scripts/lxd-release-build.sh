#!/bin/sh
# lxd-release-build.sh - build charmstore releases in a clean LXD environment

set -eu

image=${image:-ubuntu:18.04}
container=${container:-charmstore-release-`uuidgen`}

lxd_exec() {
	lxc exec \
		--env http_proxy=${http_proxy:-} \
		--env https_proxy=${https_proxy:-${http_proxy:-}} \
		--env no_proxy=${no_proxy:-} \
		$container -- "$@"
}

lxd_exec_ubuntu() {
	lxc exec \
		--env HOME=/home/ubuntu \
		--env http_proxy=${http_proxy:-} \
		--env https_proxy=${https_proxy:-${http_proxy:-}} \
		--env no_proxy=${no_proxy:-} \
		--user 1000 \
		--group 1000 \
		--cwd=${cwd:-/home/ubuntu} \
		$container -- "$@"
}

lxc launch -e $image $container
trap "lxc delete --force $container" EXIT

lxd_exec sh -c 'while [ ! -f /var/lib/cloud/instance/boot-finished ]; do sleep 0.1; done'
lxd_exec apt-get update -y
lxd_exec apt-get install -y build-essential bzr git make mongodb
if [ -n "${http_proxy:-}" ]; then
	lxd_exec snap set system proxy.http=${http_proxy:-}
	lxd_exec snap set system proxy.https=${https_proxy:-${http_proxy:-}}
	lxd_exec_ubuntu git config --global http.proxy ${http_proxy:-}
fi
lxd_exec snap install go --classic

lxc file push --uid 1000 --gid 1000 --mode 600 ${NETRC:-$HOME/.netrc} $container/home/ubuntu/.netrc
lxd_exec_ubuntu mkdir -p /home/ubuntu/src
tar c . | cwd=/home/ubuntu/src lxd_exec_ubuntu tar x
cwd=/home/ubuntu/src lxd_exec_ubuntu go mod download

cwd=/home/ubuntu/src lxd_exec_ubuntu env JUJU_TEST_ELASTICSEARCH=none make check
cwd=/home/ubuntu/src lxd_exec_ubuntu make release

tarfile=`lxd_exec_ubuntu find /home/ubuntu/src -name "charmstore-*.tar.xz"| head -1`
lxc file pull $container$tarfile .
