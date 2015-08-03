  PARAMETER_NUMBER=`cat a |awk -F 'set ' '{print $2}'|wc -l`
  if [[ $PARAMETER_NUMBER -gt 0 ]]
  then
    for m in `seq 1 $PARAMETER_NUMBER`
    do
      PARAMETER=`cat b |awk -F 'set ' '{print $2}'|awk -F ';' '{print $1}'|head -n $m|tail -n 1 |awk -F '=' '{print $1}'`
      VALUE=`cat b |awk -F 'set ' '{print $2}'|awk -F ';' '{print $1}'|head -n $m|tail -n 1|awk -F '=' '{print $2}'`
      var='${hiveconf:'$PARAMETER'}'
      #replace the query parameter with the real value in sql script
      sed -i  s/"$var"/"$VALUE"/g a
    done
  fi

