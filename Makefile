## Generic Go Makefile

## Project name and parent are used in conjunction to create a pkg path
project = gbus
project_parent = github.com/zinic


## project_pkg combines the project parent and the project into a single go package name
project_pkg = $(project_parent)/$(project)

## Workspace root directory. This defines a place to put downloaded dependencies in addition to acting as
## an installation sandbox for development.
w_root = $(CURDIR)/build

## Workspace specific directories
w_bin = $(w_root)/bin
w_pkg = $(w_root)/pkg
w_src = $(w_root)/src

## Export the GOPATH to include our workspace.
export GOPATH = $(w_root)


## Init and build
all: init get


## Initializes the libswarm w_root.
init: init-dirs


## Creates w_root directories and links.
init-dirs:
	test -d $(w_root) || mkdir $(w_root)
	test -d $(w_pkg) || mkdir $(w_pkg)
	test -d $(w_bin) || mkdir $(w_bin)
	test -d $(w_src) || (mkdir -p $(w_src)/$(project_parent) && ln -s $(CURDIR) $(w_src)/$(project_pkg))


## Get uses "go get" since this will sandbox the built binary to the
## bin directory of the w_root as well as grab all relevant dependencies.
get: clean-pkg
	go get $(project_pkg)


## Clean the swarmd object code
clean-pkg:
	rm -rf $(w_root)/pkg/$(project_pkg)


## Remove the w_root - ezpz.
clean:
	rm -rf $(w_root)
