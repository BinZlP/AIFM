#local_rams=( 256 512 1024 2048 3072 4096 5120 6144 7168 8192 9216 10240 11264 12288 13312 14336 15360 16384 17408 18432 )
local_rams=( 1024 2048 3072 4096 5120 6144 7168 8192 9216 10240 11264 12288 13312 14336 15360 16384 17408 18432 )

for local_ram in ${local_rams[@]};do
  ./run_one.sh $local_ram
done
