rm  1_fs/* 2_master/* 3_mapper/* 4_reducer/*
make clean
make
cp fs_server 1_fs
cp master_server 2_master
cp mapper_node 3_mapper
cp reducer_node 4_reducer
