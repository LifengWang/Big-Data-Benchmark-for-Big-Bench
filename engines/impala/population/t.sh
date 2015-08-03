  PARAMETER_NUMBER=`cat b |wc -l`
  if [[ $PARAMETER_NUMBER -gt 0 ]]
  then
    for m in `seq 1 $PARAMETER_NUMBER`
    do
      PARAMETER=`cat b |head -n $m|tail -n 1 |awk -F '=' '{print $1}'`
      VALUE=`cat b |head -n $m|tail -n 1|awk -F '=' '{print $2}'`
      var='${hiveconf:'$PARAMETER'}'
      #replace the query parameter with the real value in sql script
      sed -i s#"$var"#"$VALUE"#g t.sql
    done
  fi
