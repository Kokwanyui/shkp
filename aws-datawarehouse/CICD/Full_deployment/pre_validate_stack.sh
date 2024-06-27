#!/bin/bash

if [ $2=="prod" ];
then
  aws cloudformation validate-template --template-url https://$1-artifacts.s3.$3.amazonaws.com/$5$1-$2-stack.yml --profile $4 --region $3
else
  aws cloudformation validate-template --template-url https://$1-artifacts-$2.s3.$3.amazonaws.com/$5$1-$2-stack.yml --profile $4 --region $3
fi