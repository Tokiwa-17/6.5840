#! /bin/sh
set -x

go build -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go


