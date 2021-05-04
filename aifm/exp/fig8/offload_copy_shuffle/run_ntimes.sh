#log_dir=`pwd`

for (( i=1; i<=$1; i++ )); do
#    echo "$log_dir/$i"
#echo $i
    ./run.sh $i
done
