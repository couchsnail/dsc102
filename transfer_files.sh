#!/bin/bash

scp -i DSCKEY.pem subset.csv ubuntu@$1:~
scp -i DSCKEY.pem PA0.py ubuntu@$1:~
