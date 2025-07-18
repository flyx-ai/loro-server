#!/usr/bin/env just --justfile

set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]
set dotenv-load := true

export CLICOLOR_FORCE := "1"

default:
  just --list

dev:
  ./node_modules/.bin/concurrently \
    --names "CRDT,NATS,GO" \
    --prefix-colors "bgBlue.bold,bgMagenta.bold,bgGreen.bold" \
    '{{just_executable()}} --justfile {{justfile()}} dev-crdt' \
    '{{just_executable()}} --justfile {{justfile()}} dev-nats' \
  	'{{just_executable()}} --justfile {{justfile()}} dev-local-server'

dev-crdt:
  cargo run

dev-nats:
  nats-server --jetstream --store_dir ./.cache/

build-server:
  go build -buildvcs=false -o {{"." / ".cache" / "server.exe ." / "lorogo" / "localserver" / "."}}

dev-local-server:
  go tool air \
    --build.cmd '{{just_executable()}} --justfile {{justfile()}} build-server' \
    --build.bin '{{join(".", ".cache", "server.exe")}}' \
    --build.exclude_dir '.cache,.direnv,.git,node_modules,src,target' \
