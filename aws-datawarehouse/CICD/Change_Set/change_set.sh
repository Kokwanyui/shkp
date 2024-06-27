#!/bin/bash

d=`date +%Y%m%d%H%M%S`

echo "https://$2-artifacts$6.s3.$5.amazonaws.com/$1$2-$3-stack.yml"
aws cloudformation validate-template \
--template-url "https://$2-artifacts$6.s3.$5.amazonaws.com/$1$2-$3-stack.yml" \
--profile $4 \
--region $5


aws cloudformation create-change-set \
--stack-name "$1$2-$3-stack" \
--change-set-name "$1$2-$3-stack-VER$d" \
--template-url "https://$2-artifacts$6.s3.$5.amazonaws.com/$1$2-$3-stack.yml" \
--no-use-previous-template \
--include-nested-stacks \
--profile $4 \
--region $5


aws cloudformation wait change-set-create-complete \
--stack-name "$1$2-$3-stack" \
--change-set-name "$1$2-$3-stack-VER$d" \
--profile $4 \
--region $5


aws cloudformation describe-change-set \
--stack-name "$1$2-$3-stack" \
--change-set-name "$1$2-$3-stack-VER$d" \
--profile $4 \
--region $5


aws cloudformation execute-change-set \
--change-set-name "$1$2-$3-stack-VER$d" \
--stack-name "$1$2-$3-stack" \
--profile $4 \
--region $5

