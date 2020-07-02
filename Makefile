install-local-jar:
	lein localrepo install lib/ImpalaJDBC42.jar impala-jdbc42 2.6.17
build:
	 lein uberjar
link-plugins:
	ln -s `pwd`/target/uberjar/impala.metabase-driver.jar ../../../plugins/impala.metabase-driver.jar

# cdh 5.7.0 hive 1.1.0 impala 2.5.0
# https://docs.cloudera.com/documentation/enterprise/release-notes/topics/cdh_vd_cdh_package_tarball_57.html
start-impala-server:
	docker run --name cloudera_quickstart --hostname=quickstart.cloudera --privileged=true -t -i -d -p 8888:8888 -p 80:80 -p 10000:10000 -p 7180:7180 -p 21050:21050 -p 50070:50070 -p 50075:50075 -p 50010:50010 -p 50020:50020 -p 8020:8020 cloudera/quickstart /usr/bin/docker-quickstart
