#!/usr/bin/env bash

INPUT_IP=$1
IP=${INPUT_IP:-localhost}

echo "Creating certificate for -> $IP"

go run generate_cert.go -ca --host "$IP"

mkdir -p CAs

mv cert.pem CAs/public.crt
mv key.pem CAs/private.key
