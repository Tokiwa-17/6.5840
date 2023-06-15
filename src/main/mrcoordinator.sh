#! /bin/sh

set -x

rm mr-out*

go run mrcoordinator.go pg-*.txt