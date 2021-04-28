cd $1

files=`ls`
for file in ${files[@]}; do
#echo $file
echo "[$file]"
tail -n 2 $file | grep Total
done

cd ../
