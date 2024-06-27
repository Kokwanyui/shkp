#!/bin/bash

# Check if artifacts bucket already exists in s3
# create one if not exists
if [ $2=="prod" ];
then
  if aws s3api head-bucket --bucket "$1-artifacts" 2>&1;
  then
    echo 'artifacts bucket already exists';
  else
    aws s3 mb "s3://$1-artifacts" --profile $4 --region $3
    echo 'Created artifacts bucket';
  fi
else
  if aws s3api head-bucket --bucket "$1-artifacts" 2>&1;
  then
    echo 'artifacts bucket already exists';
  else
    aws s3 mb "s3://$1-artifacts-$2" --profile $4 --region $3
    echo 'Created artifacts bucket';
  fi
fi