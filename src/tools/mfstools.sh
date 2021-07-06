#!/bin/bash

tool="$(basename $0)"
case "$tool" in
  (lizardfs*) tool="${tool#lizardfs}" ;;
  (mfs*) tool="${tool#mfs}" ;;
esac
exec lizardfs "${tool}" "$@"
