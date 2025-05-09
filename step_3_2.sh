#!/bin/bash

ssh -i "DSCKEY.pem" ubuntu@$1
scp step_3_2.sh ubuntu@$1:~
aws s3 sync s3://dsc102-pa1-public .
