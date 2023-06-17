#! /bin/sh
set -x

go build -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go

go build -buildmode=plugin -gcflags="all=-N -l" ../mrapps/early_exit.go

go build -buildmode=plugin -gcflags="all=-N -l" ../mrapps/crash.go


