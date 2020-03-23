#!/bin/bash
echo Merging groups...
python merge.py groups
echo 
echo Merging users...
python merge.py users
echo Success!
