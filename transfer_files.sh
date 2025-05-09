#!/bin/bash

scp -i DSCKEY.pem PA1/starter_notebook.ipynb ubuntu@$1:~
