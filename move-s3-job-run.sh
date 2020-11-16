job_run_id=$(echo $1"-driver")
buchet=$2
directory_source=$3
directory_target=$4
profile=$5

backwardToken=$(aws logs get-log-events --log-group-name /aws-glue/jobs/logs-v2 --log-stream-name $job_run_id --region us-east-1 --profile $profile | egrep "nextForwardToken" | egrep -o "f\/[^\"]+")

while true; do
  
  aws logs get-log-events --log-group-name /aws-glue/jobs/logs-v2 --log-stream-name $job_run_id --next-token $backwardToken --region us-east-1 --profile $profile | egrep "Copiando CSV" | egrep -o "seguros_db\/dbo\/[^\/]+\/[^\-]+\-[^\-]+\.csv\.gz" | while read file; do echo aws s3 mv s3://$buchet/$directory_source/$file s3://$buchet/$directory_target/$file --profile $profile; done

  nextBackwardToken=$(aws logs get-log-events --log-group-name /aws-glue/jobs/logs-v2 --log-stream-name $job_run_id --next-token $backwardToken --region us-east-1 --profile $profile | egrep "nextBackwardToken" | egrep -o "b\/[^\"]+")

  if [[ "$nextBackwardToken" == "$backwardToken" ]]; then
    break
  else
    backwardToken=$nextBackwardToken
  fi

done
