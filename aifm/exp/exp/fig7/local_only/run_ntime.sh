current_dir=`pwd`
num_list=(1 2 3 4 5)

if [ $# -lt 2 ]
then
   echo "Usage: ./run_ntime.sh [executable] [# to repeat]"
   exit
fi

#for num in ${num_list[@]}; do
#    cd $current_dir
#    ./run.sh $num
#echo $num
#echo $current_dir
#done

for (( i=1; i<=$2; i++ )); do
#echo $i
#echo $current_dir/$1
    ./$1 $i
done
