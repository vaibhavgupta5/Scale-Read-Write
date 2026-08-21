#!/bin/bash
npx autocannon \
  -c 10 \
  -d 30 \
  -p 1 \
  --renderStatusCodes \
  http://localhost:3000/order