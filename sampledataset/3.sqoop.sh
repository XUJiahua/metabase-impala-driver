# export mysql to hive
sqoop import-all-tables \
    -m 1 \
    --connect jdbc:mysql://host.docker.internal:3306/sampledataset \
    --username=root \
    --password=root \
    --compression-codec=snappy \
    --as-parquetfile \
    --warehouse-dir=/user/hive/warehouse \
    --hive-import
