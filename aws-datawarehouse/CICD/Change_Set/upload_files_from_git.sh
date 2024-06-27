#!/bin/bash

# sync all to artifacts bucket
if [ $2=="prod" ];
then
  aws s3 cp ./ s3://$1-artifacts/ --recursive --exclude ".git*"  --profile $4 --region $3
else
  aws s3 cp ./ s3://$1-artifacts-$2/ --recursive --exclude ".git*"  --profile $4 --region $3
fi