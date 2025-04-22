#!/bin/bash

scp -i DSCKEY.pem subset.csv ubuntu@$1:~
scp -i DSCKEY.pem PA0_0421.py ubuntu@$1:~
