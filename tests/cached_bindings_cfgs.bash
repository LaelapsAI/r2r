#!/bin/bash
# A rerun of r2r's build script that takes its cached bindings must still set
# the `r2r__<package>__<msg|srv|action>__<Name>` cfgs. Cargo keeps only the
# latest run's directives, so without them every item gated on a message
# (`r2r::lifecycle`, the clock's time source) is compiled out.
#
# The bindings are cached per environment (r2r_common::get_env_hash), so the
# rerun happens whenever the environment returns to one built before: build
# with one message filter, then another, then the first again, and check the
# last run took the cache and still set the cfgs.
#
#   tests/cached_bindings_cfgs.bash      # with ROS sourced, from the repository
set -euo pipefail

target=$(mktemp -d)
trap 'rm -rf "$target"' EXIT

build() {
  IDL_PACKAGE_FILTER="$1" cargo build -p r2r --target-dir "$target" "${@:2}"
}
build 'std_msgs;lifecycle_msgs' --quiet
build 'std_msgs;lifecycle_msgs;rosgraph_msgs' --quiet
# Back in the first environment, the build script runs again and takes the
# cache. -vv echoes what it prints as "[r2r <version>] <line>", wherever
# this cargo keeps its build directories (their layout changes across
# versions).
log=$(build 'std_msgs;lifecycle_msgs' -vv 2>&1)
grep -q '^\[r2r [0-9.]*\] Used cached files' <<<"$log" ||
  { echo "the last build didn't take the cached bindings: nothing tested" >&2; exit 1; }
grep -q '^\[r2r [0-9.]*\] cargo:rustc-cfg=r2r__lifecycle_msgs__msg__State$' <<<"$log" ||
  { echo "the cached build set no r2r__lifecycle_msgs__msg__State cfg" >&2; exit 1; }
echo "cached bindings keep their cfgs"
