// Copyright 2012, 2013 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/juju/charm.v2"
	charmtesting "gopkg.in/juju/charm.v2/testing"
	gc "launchpad.net/gocheck"

	"github.com/juju/charmstore"
)

func (s *StoreSuite) dummyBranch(c *gc.C, suffix string) bzrDir {
	tmpDir := c.MkDir()
	if suffix != "" {
		tmpDir = filepath.Join(tmpDir, suffix)
		err := os.MkdirAll(tmpDir, 0755)
		c.Assert(err, gc.IsNil)
	}
	branch := bzrDir(tmpDir)
	branch.init()

	copyCharmDir(branch.path(), charmtesting.Charms.CharmDir("dummy"))
	branch.add()
	branch.commit("Imported charm.")
	return branch
}

var urls = []*charm.URL{
	charm.MustParseURL("cs:~joe/oneiric/dummy"),
	charm.MustParseURL("cs:oneiric/dummy"),
}

type fakePlugin struct {
	oldEnv string
}

func (p *fakePlugin) install(dir string, content string) {
	p.oldEnv = os.Getenv("BZR_PLUGINS_AT")
	err := ioutil.WriteFile(filepath.Join(dir, "__init__.py"), []byte(content), 0644)
	if err != nil {
		panic(err)
	}
	os.Setenv("BZR_PLUGINS_AT", "fakePlugin@"+dir)
}

func (p *fakePlugin) uninstall() {
	os.Setenv("BZR_PLUGINS_AT", p.oldEnv)
}

func (s *StoreSuite) TestPublish(c *gc.C) {
	branch := s.dummyBranch(c, "")

	// Ensure that the streams are parsed separately by inserting
	// garbage on stderr. The wanted information is still there.
	plugin := fakePlugin{}
	plugin.install(c.MkDir(), `import sys; sys.stderr.write("STDERR STUFF FROM TEST\n")`)
	defer plugin.uninstall()

	err := charmstore.PublishBazaarBranch(s.store, urls, branch.path(), "wrong-rev")
	c.Assert(err, gc.IsNil)

	for _, url := range urls {
		info, rc, err := s.store.OpenCharm(url)
		c.Assert(err, gc.IsNil)
		defer rc.Close()
		c.Assert(info.Revision(), gc.Equals, 0)
		c.Assert(info.Meta().Name, gc.Equals, "dummy")

		data, err := ioutil.ReadAll(rc)
		c.Assert(err, gc.IsNil)

		bundle, err := charm.ReadCharmArchiveBytes(data)
		c.Assert(err, gc.IsNil)
		c.Assert(bundle.Revision(), gc.Equals, 0)
		c.Assert(bundle.Meta().Name, gc.Equals, "dummy")
	}

	// Attempt to publish the same content again while providing the wrong
	// tip revision. It must pick the real revision from the branch and
	// note this was previously published.
	err = charmstore.PublishBazaarBranch(s.store, urls, branch.path(), "wrong-rev")
	c.Assert(err, gc.Equals, charmstore.ErrRedundantUpdate)

	// Bump the content revision and lie again about the known tip revision.
	// This time, though, pretend it's the same as the real branch revision
	// previously published. It must error and not publish the new revision
	// because it will use the revision provided as a parameter to check if
	// publishing was attempted before. This is the mechanism that enables
	// stopping fast without having to download every single branch. Real
	// revision is picked in the next scan.
	digest1 := branch.digest()
	branch.change()
	err = charmstore.PublishBazaarBranch(s.store, urls, branch.path(), digest1)
	c.Assert(err, gc.Equals, charmstore.ErrRedundantUpdate)

	// Now allow it to publish the new content by providing an unseen revision.
	err = charmstore.PublishBazaarBranch(s.store, urls, branch.path(), "wrong-rev")
	c.Assert(err, gc.IsNil)
	digest2 := branch.digest()

	info, err := s.store.CharmInfo(urls[0])
	c.Assert(err, gc.IsNil)
	c.Assert(info.Revision(), gc.Equals, 1)
	c.Assert(info.Meta().Name, gc.Equals, "dummy")

	// There are two events published, for each of the successful attempts.
	// The failures are ignored given that they are artifacts of the
	// publishing mechanism rather than actual problems.
	_, err = s.store.CharmEvent(urls[0], "wrong-rev")
	c.Assert(err, gc.Equals, charmstore.ErrNotFound)
	for i, digest := range []string{digest1, digest2} {
		event, err := s.store.CharmEvent(urls[0], digest)
		c.Assert(err, gc.IsNil)
		c.Assert(event.Kind, gc.Equals, charmstore.EventPublished)
		c.Assert(event.Revision, gc.Equals, i)
		c.Assert(event.Errors, gc.IsNil)
		c.Assert(event.Warnings, gc.IsNil)
	}
}

func (s *StoreSuite) TestPublishErrorFromBzr(c *gc.C) {
	branch := s.dummyBranch(c, "")

	// In TestPublish we ensure that the streams are parsed
	// separately by inserting garbage on stderr. Now make
	// sure that stderr isn't simply trashed, as we want to
	// know about what a real error tells us.
	plugin := fakePlugin{}
	plugin.install(c.MkDir(), `import sys; sys.stderr.write("STDERR STUFF FROM TEST\n"); sys.exit(1)`)
	defer plugin.uninstall()

	err := charmstore.PublishBazaarBranch(s.store, urls, branch.path(), "wrong-rev")
	c.Assert(err, gc.ErrorMatches, "(?s).*STDERR STUFF.*")
}

func (s *StoreSuite) TestPublishErrorInCharm(c *gc.C) {
	branch := s.dummyBranch(c, "")

	// Corrupt the charm.
	branch.remove("metadata.yaml")
	branch.commit("Removed metadata.yaml.")

	// Attempt to publish the erroneous content.
	err := charmstore.PublishBazaarBranch(s.store, urls, branch.path(), "wrong-rev")
	c.Assert(err, gc.ErrorMatches, ".*/metadata.yaml: no such file or directory")

	// The event should be logged as well, since this was an error in the charm
	// that won't go away and must be communicated to the author.
	event, err := s.store.CharmEvent(urls[0], branch.digest())
	c.Assert(err, gc.IsNil)
	c.Assert(event.Kind, gc.Equals, charmstore.EventPublishError)
	c.Assert(event.Revision, gc.Equals, 0)
	c.Assert(event.Errors, gc.NotNil)
	c.Assert(event.Errors[0], gc.Matches, ".*/metadata.yaml: no such file or directory")
	c.Assert(event.Warnings, gc.IsNil)
}

type bzrDir string

func (dir bzrDir) path(args ...string) string {
	return filepath.Join(append([]string{string(dir)}, args...)...)
}

func (dir bzrDir) run(args ...string) []byte {
	cmd := exec.Command("bzr", args...)
	oldemail := os.Getenv("EMAIL")
	defer os.Setenv("EMAIL", oldemail)
	// bzr will complain if bzr whoami has not been run previously,
	// avoid this by passing $EMAIL into the environment.
	os.Setenv("EMAIL", "nobody@testing.invalid")
	cmd.Dir = string(dir)
	output, err := cmd.Output()
	if err != nil {
		panic(fmt.Sprintf("command failed: bzr %s\n%s", strings.Join(args, " "), output))
	}
	return output
}

func (dir bzrDir) init() {
	dir.run("init")
}

func (dir bzrDir) add(paths ...string) {
	dir.run(append([]string{"add"}, paths...)...)
}

func (dir bzrDir) remove(paths ...string) {
	dir.run(append([]string{"rm"}, paths...)...)
}

func (dir bzrDir) commit(msg string) {
	dir.run("commit", "-m", msg)
}

func (dir bzrDir) write(path string, data string) {
	err := ioutil.WriteFile(dir.path(path), []byte(data), 0644)
	if err != nil {
		panic(err)
	}
}

func (dir bzrDir) change() {
	t := time.Now().String()
	dir.write("timestamp", t)
	dir.add("timestamp")
	dir.commit("Revision bumped at " + t)
}

func (dir bzrDir) digest() string {
	output := dir.run("revision-info")
	f := bytes.Fields(output)
	if len(f) != 2 {
		panic("revision-info returned bad output: " + string(output))
	}
	return string(f[1])
}

func copyCharmDir(dst string, dir *charm.CharmDir) {
	var b bytes.Buffer
	err := dir.ArchiveTo(&b)
	if err != nil {
		panic(err)
	}
	bundle, err := charm.ReadCharmArchiveBytes(b.Bytes())
	if err != nil {
		panic(err)
	}
	err = bundle.ExpandTo(dst)
	if err != nil {
		panic(err)
	}
}
