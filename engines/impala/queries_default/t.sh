sed s/'$QUERY_SQL_DIR/$QUERY_NAME.sql'/'a'/ q01/run.sh 

var='$QUERY_SQL_DIR/$QUERY_NAME.sql'
sed -i s#"$var""$VALUE"/g `grep hiveconf -rl $QUERY_SQL_DIR/*.sql`
