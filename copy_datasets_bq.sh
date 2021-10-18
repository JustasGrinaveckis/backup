#!/bin/bash

SLACK_WEBHOOK_URL=$(cat webhook.txt)
curr_date="`date +%Y_%m_%d`";
echo "Current date: $curr_date";
backup_ds=backup_$curr_date;

function error_handling () {
    if [[ $? -eq 1 ]]; then
        curl -X POST -H 'Content-type: application/json' --data '{"text":"Nulūžo scriptas"}' $SLACK_WEBHOOK_URL
        exit 1
    fi
}

#creating new backup dataset with current date
bq --location=US mk -d --description "Backup dataset for $curr_date" $backup_ds

#filter not needed datasets (no pattern, so hardcoding datasets)
datasets=$(bq ls -max_results=1000 | awk '{print $1}' | tail +3 | grep -iv "sys\|production\|fivetran\|looker_scratch\|kilolt\|temp\|unused_tables\|backup\|audits");

for dataset in $datasets;  do
#All this for firebase tables as some of them doesn't have same schema
if [[ "$dataset" == *"firebase_transfer"* ]]; then
    arr=(${dataset//_/ })
    table_name=${arr[0]}
    echo "select * from \`${dataset}.events_intraday_*\`" > events_intraday.txt
    echo "select * from \`${dataset}.events_*\`" > events.txt
    bq query --use_legacy_sql=false --destination_table=${backup_ds}.${table_name}_events_intraday_${curr_date} -n=0 < events_intraday.txt >> log.txt
    error_handling
    bq query --use_legacy_sql=false --destination_table=${backup_ds}.${table_name}_events_${curr_date} -n=0 < events.txt >> log.txt
    error_handling
else
    #for now - copying all tables in dataset
    tables=$(bq ls -max_results=5000 kilo-dw:$dataset | awk '{print $1}' | tail +3)
    for table in $tables; do
        bq cp -a ${dataset}.${table} ${backup_ds}.${dataset}_${table}_${curr_date} >> log.txt
        error_handling
    done
fi
done

#sort backup datasets in asc order and take first one
backup_tbl_cnt=$(bq ls | grep "backup_[0-9].*" | awk '{print $1}' | wc -l)
if [[ $backup_tbl_cnt -gt 1 ]]; then
    rm_ds=$(bq ls | grep "backup_[0-9].*" | sort | awk 'NR==1 {print $1;}')
    echo $rm_ds | tee deleted_backup_dataset_$curr_date.txt
    bq rm -d -f=true $rm_ds >> log.txt
    error_handling
fi


