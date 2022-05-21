#!/bin/sh

cd ~/rrdata

git add .
read -p "input commit -m context: " context

echo $context
git commit -m "$context"


git push -u origin master


