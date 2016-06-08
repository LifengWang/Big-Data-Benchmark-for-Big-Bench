#!/usr/bin/env bash

#"INTEL CONFIDENTIAL"
#Copyright 2015  Intel Corporation All Rights Reserved. 
#
#The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
#
#No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

# define used temp tables
TEMP_TABLE1="${TEMP_TABLE}_DateRange"
TEMP_TABLE2="${TEMP_TABLE}_sales_review"
TEMP_TABLE3="${TEMP_TABLE}_webSales_date"

BINARY_PARAMS="$BINARY_PARAMS --hiveconf TEMP_TABLE1=$TEMP_TABLE1 --hiveconf TEMP_TABLE2=$TEMP_TABLE2 --hiveconf TEMP_TABLE3=$TEMP_TABLE3"

query_run_main_method () {
	QUERY_SCRIPT="$QUERY_SQL_DIR/$QUERY_NAME.sql"
	if [ ! -r "$QUERY_SCRIPT" ]
	then
		echo "SQL file $QUERY_SCRIPT can not be read."
		exit 1
	fi

        runCmdWithErrorCheck runEngineCmd -f "$QUERY_SCRIPT"
	return $?
}

query_run_clean_method () {
	runCmdWithErrorCheck runEngineCmd -q "DROP VIEW IF EXISTS $TEMP_TABLE1; DROP VIEW IF EXISTS $TEMP_TABLE2; DROP VIEW IF EXISTS $TEMP_TABLE3; DROP TABLE IF EXISTS $RESULT_TABLE;"
	return $?
}

query_run_validate_method () {
	VALIDATION_TEMP_DIR="`mktemp -d`"
	runCmdWithErrorCheck runEngineCmd -q "INSERT OVERWRITE LOCAL DIRECTORY '$VALIDATION_TEMP_DIR' SELECT * FROM $RESULT_TABLE LIMIT 10;"
	if [ `wc -l < "$VALIDATION_TEMP_DIR/000000_0"` -qq 1 ]
	then
		echo "Validation passed: Query returned one result"
	else
		echo "Validation failed: Query did not return results"
	fi
	rm -rf "$VALIDATION_TEMP_DIR"
}