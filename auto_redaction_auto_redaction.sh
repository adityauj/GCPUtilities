#!/bin/bash
set -x 

if [ $# -ne 4 ]
then
	echo "Please provide all the requirement parameters"
	exit 1
fi

export file_path_gcs=$1
export required_record=$2
export config_key=$3
export destination_location=$4
export config_file_path=gs://bkt-gousenaib-dlak-dmrt-str01/auto_redaction/config_file
export project_id=pid-gousenaig-dlak-res01
export temp_directory=~/auto_redaction_temp
export log_date=`date "+%m%d%y%H%M%S"`
#export query_to_fetch_ordinal_position="select ordinal_position from trusted_layer.INFORMATION_SCHEMA.COLUMNS where table_name='@{table_name}' and column_name in ('@{sensitive_column}')"
export  file_basename=$(echo $file_path_gcs | awk -F "/" '{print $NF}') 
export config_entry=$(gsutil cat $config_file_path | grep -iw "^$config_key")
echo "Setting Environment To $project_id"
gcloud config set project $project_id
mkdir -p $temp_directory/

passed_parameter()
{
	echo "***************** Passed Parameters *****************"
	echo "file_path_gcs: $file_path_gcs"
	echo "required_record: $required_record"
	echo "config_key: $config_key"
	echo "destination_location: $destination_location"
	echo "***************** Passed Parameters *****************"
}

check_config()
{	
	username=$(whoami)
	echo "Script has been executed by: $username"
	
	passed_parameter
	
	if [ ${#config_entry} -eq 0 ]
	then
		echo "No config entry found for file name $file_basename"
		exit 2
	fi
	
	if [ $(echo $config_entry | cut -d'#' -f2 | tr -d ' ' | wc -L) -ne 0 ]
	then
		op_delimiter=$(echo $config_entry | cut -d'#' -f2)
		inp_delimiter=$(echo $op_delimiter | sed "s/./\\\&/g")
		
		IFS="," read -a sensitive_column_nbr_arr <<< $(echo $config_entry | cut -d'#' -f3)
		
		if [ $required_record -gt 20000 ]
		then
			execute_process_delimitted
		else
			execute_process_delimitted_without_temp_file
		fi
	else
		if [ $required_record -gt 20000 ]
		then
			execute_process_fixed_width
		else
			execute_process_fixed_width_without_temp_file
		fi
	fi
}

get_sensitive_field_number()
{
	echo "Traversing over sensitive_column_arr to get their ordinal position"
	table_name=$(echo $config_entry | cut -d'#' -f2 | cut -d'.' -f2)
	sensitive_column=$(echo $config_entry | cut -d'#' -f3 | sed "s/,/','/g")
	executeable_query=$(echo $query_to_fetch_ordinal_position | sed "s/@{table_name}/$table_name/g" | sed "s/@{sensitive_column}/$sensitive_column/g")

	bq_result_temp=$(bq query --use_legacy_sql=FALSE --format=csv "$executeable_query")
		
	sensitive_column_nbr_arr=($(echo $bq_result_temp | cut -d' ' -f2-))
}

process_line_for_fixed_width()
{
	data="${1}"
	IFS="," read -r -a sensitive_column_arr <<< $(echo $config_entry | cut -d'#' -f3)	
	for redaction_range in "${sensitive_column_arr[@]}"
	do
		start_index=$(echo $redaction_range | cut -d'-' -f1)
		end_index=$(echo $redaction_range | cut -d'-' -f2)
		
		str=$(echo "$data" | cut -c$redaction_range)
		repl=$(echo "$str"| sed "s/[a-zA-Z0-9]/X/g")
			
		data=$(echo "$data" | awk -v repl="$repl" -v start_index=$start_index -v end_index=$end_index '{print substr($0, 0, start_index) repl substr($0, end_index+1)}')
	done
	echo "${data}" >> $temp_directory/$file_basename"_temp"
}

execute_process_fixed_width()
{
	true > $temp_directory/$file_basename"_temp"
	gsutil cat $file_path_gcs | head -$required_record > $temp_directory/$file_basename
	
	while IFS= read -r LINE
	do
		process_line_for_fixed_width "${LINE}"
	done < "$temp_directory/$file_basename"
	
	mv $temp_directory/$file_basename"_temp" $temp_directory/$file_basename
}

execute_process_fixed_width_without_temp_file()
{
	true > $temp_directory/$file_basename"_temp"
	for ((i=1; i<=$required_record; i++));
	do
		record=$(gsutil cat $file_path_gcs | awk -v line_number=$i 'NR==line_number')
		
		if [ ${#record} -eq 0 ]
		then
			break
		fi
		
		process_line_for_fixed_width "${record}"
	done
	mv $temp_directory/$file_basename"_temp" $temp_directory/$file_basename
}

execute_process_delimitted()
{
	#get_sensitive_field_number
	gsutil cat $file_path_gcs | head -$required_record > $temp_directory/$file_basename
	
	for ordinal_position in "${sensitive_column_nbr_arr[@]}"
	do
		awk -F "$inp_delimiter" -v ordinal_position=$ordinal_position -v OFS="$op_delimiter" '{gsub(/[a-zA-Z0-9]/,"X",$ordinal_position)}1' $temp_directory/$file_basename > $temp_directory/$file_basename"_temp"
		mv $temp_directory/$file_basename"_temp" $temp_directory/$file_basename
	done
}

execute_process_delimitted_without_temp_file()
{
	#get_sensitive_field_number
	true > $temp_directory/$file_basename
	
	for ((i=1; i<=$required_record; i++));
	do
		record=$(gsutil cat $file_path_gcs | awk -v line_number=$i 'NR==line_number')
		
		if [ ${#record} -eq 0 ]
		then
			break
		fi
		
		for ordinal_position in "${sensitive_column_nbr_arr[@]}"
		do	
			record=$(echo "${record}" | awk -F "$inp_delimiter" -v ordinal_position=$ordinal_position -v OFS="$op_delimiter" '{gsub(/[a-zA-Z0-9]/,"X",$ordinal_position)}1') 
		done
		
		echo "${record}" >> $temp_directory/$file_basename	
	done
}

echo "***************** redaction process started for file $file_path_gcs *****************"

check_config >> $temp_directory/auto_redaction_${log_date}.log