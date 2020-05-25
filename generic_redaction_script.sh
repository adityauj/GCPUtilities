#!/bin/bash

TRUSTED_TABLE_NAME=$1
ETLBATCHID=$2
FILENAME=$3

column_names=`bq query --format=csv --use_legacy_sql=False "SELECT CONCAT('CONCAT(',SUBSTR(column_names, 0, LENGTH(column_names)-6),')') FROM ( SELECT STRING_AGG(FORMAT( IF (data_type <> \"STRING\", \"ifnull(cast(%t as string),''),'~|*'\", \"ifnull(%t, ''),'~|*'\"), column_name )) AS column_names FROM \\\`prj-gousenaib-dlak-res01\\\`.trusted_layer.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '$TRUSTED_TABLE_NAME' AND column_name NOT IN ('hash_key_value'))"  | tail -n 1 |tr -d "\""`

echo $column_names

if [ $? -ne 0 ]
then 
echo -ne "{\\\"error\\\" : \\\"Error occurred while fetching column_name\\\"}"|tr -d '\n'|tr "'" "*"
exit 1
fi

if [ -z "$column_names" ]
then 
echo "No Table found !!"
fi

table_data=`bq query --format=csv --use_legacy_sql=False "SELECT $column_names FROM \\\`prj-gousenaib-dlak-res01\\\`.trusted_layer.$TRUSTED_TABLE_NAME WHERE etlbatchid = '$ETLBATCHID' limit 10000"`

table_data=$(echo $table_data | cut -d' ' -f2-)

if [ "${table_data:0:1}" == "\"" ]
then
    table_data=${table_data#?}
	table_data=${table_data//"$ETLBATCHID\" "/"$ETLBATCHID\n"}
fi

table_data=${table_data//"$ETLBATCHID "/"$ETLBATCHID\n"}

echo -e $table_data | cat > $TRUSTED_TABLE_NAME.$ETLBATCHID.csv

gsutil -m cp $TRUSTED_TABLE_NAME.$ETLBATCHID.csv gs://redacted_files/$TRUSTED_TABLE_NAME.$ETLBATCHID.csv
