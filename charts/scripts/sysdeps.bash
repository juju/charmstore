#!/bin/bash

HERE=$(cd $(dirname $0); pwd)

set -eu

sudo snap install microk8s --classic --edge
microk8s.enable dns registry storage
sudo snap install helm

# Install and configure kubectl
sudo snap install kubectl --classic
mkdir -p $HOME/.kube
if [ ! -e "$HOME/.kube/config" ]; then
	microk8s.config > $HOME/.kube/config
fi

# Install and configure helm
helm 2>&1 >/dev/null || true
microk8s.config > $HOME/snap/helm/common/kube/config
helm init || helm init --upgrade
$HERE/helm-rbac-fix.bash

