FROM ubuntu:16.04
COPY dist/bin /srv/charmstore/bin
VOLUME ["/srv/charmstore/etc"]
