# Makefile for the charm store.

GIT_COMMIT := $(shell git rev-parse --verify HEAD)
VERSION := $(shell git describe --dirty)

define DEPENDENCIES
  build-essential
  bzr
  juju-mongodb
  mongodb-server
  $(GO_C)
  openjdk-7-jre-headless
  elasticsearch
endef

ifeq ($(VERSION),no)
	VERSIONDEPS :=
else
	VERSIONDEPS := version/init.go
endif

default: build

build: $(VERSIONDEPS)
	go build ./...

check: $(VERSIONDEPS)
	go test ./...

install: $(VERSIONDEPS)
	go install $(INSTALL_FLAGS) -v ./...

release: charmstore-$(VERSION).tar.xz

clean:
	go clean ./...
	-rm charmstore-*.tar.xz

# Reformat source files.
format:
	gofmt -w -l .

# Reformat and simplify source files.
simplify:
	gofmt -w -l -s .

# Run the charmd server.
server: install
	charmd -logging-config '<root>=DEBUG;mgo=INFO;bakery=INFO;httpbakery=INFO' cmd/charmd/config.yaml

# Update the project Go dependencies to the required revision.
deps: go.mod
	go mod download

# Generate version information
version/init.go: version/init.go.tmpl FORCE
	gofmt -r "unknownVersion -> Version{GitCommit: \"${GIT_COMMIT}\", Version: \"${VERSION}\",}" $< | tee $@ > /dev/null

# Install packages required to develop the charm store and run tests.
APT_BASED := $(shell command -v apt-get >/dev/null; echo $$?)
sysdeps:
ifeq ($(APT_BASED),0)
ifeq ($(shell lsb_release -cs|sed -r 's/precise|quantal|raring/old/'),old)
	@echo Adding PPAs for golang and mongodb
	@sudo apt-add-repository --yes ppa:ubuntu-lxc/lxd-stable
	@sudo apt-add-repository --yes ppa:juju/stable
endif
	@echo Installing dependencies
	[ "x$(apt-key export D88E42B4 2>&1 1>/dev/null)" = "x" ] || { curl -s http://packages.elasticsearch.org/GPG-KEY-elasticsearch | sudo apt-key add -;}
	repo="http://packages.elasticsearch.org/elasticsearch/1.3/debian" file=/etc/apt/sources.list.d/packages_elasticsearch_org_elasticsearch_1_3_debian.list ; grep "$$repo" $$file || echo "deb $$repo stable main" | sudo tee $$file > /dev/null
	sudo apt-get update
	@sudo apt-get --force-yes install $(strip $(DEPENDENCIES)) \
	$(shell apt-cache madison juju-mongodb mongodb-server | head -1 | cut -d '|' -f1)
else
	@echo sysdeps runs only on systems with apt-get
	@echo on OS X with homebrew try: brew install bazaar mongodb elasticsearch
endif

# Build a release tarball
charmstore-$(VERSION).tar.xz: $(VERSIONDEPS)
	mkdir -p charmstore-release/bin
	GOBIN=$(CURDIR)/charmstore-release/bin go install $(INSTALL_FLAGS) -v ./...
	tar cv -C charmstore-release . | xz > $@
	-rm -r charmstore-release

help:
	@echo -e 'Charmstore - list of make targets:\n'
	@echo 'make - Build the package.'
	@echo 'make check - Run tests.'
	@echo 'make install - Install the package.'
	@echo 'make release - Build a binary tarball of the package.'
	@echo 'make server - Start the charmd server.'
	@echo 'make clean - Remove object files from package source directories.'
	@echo 'make sysdeps - Install the development environment system packages.'
	@echo 'make deps - Set up the project Go dependencies.'
	@echo 'make create-deps - Generate the Go dependencies file.'
	@echo 'make format - Format the source files.'
	@echo 'make simplify - Format and simplify the source files.'

.PHONY: build check clean format gopkg help install simplify sysdeps release FORCE
