cache_sizes=(1 3 5 7 9 11 13 15 17 19 21 23 25 27 29 31)
working_dir=`pwd`

for cache_size in ${cache_sizes[@]}; do
#    echo $cache_size
#    echo $working_dir
    cd $working_dir
    ./run.sh $cache_size
done
