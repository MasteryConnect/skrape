#!/bin/bash

minimal() {
  # Minimal docker build
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags '-s' -installsuffix cgo -o skrape

#  sudo docker build -t docker.mstry.io/etl/skrape -f Dockerfile .
}

echo "Building static linked go app"
minimal
