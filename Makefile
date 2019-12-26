# Makefile for Hadoop MapReduce WordCount demo project.

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/usr/local/hadoop
jar.name=team-data-1.0.jar
jar.path=target/${jar.name}
job.name=com.project.preprocessor.Driver
job.knn=KNN.KNN
job.pr=PageRank.Driver
local.nodes=nodes
local.intermediate=outputs/intermediate
local.edges=edges
local.final.individual=outputs/Final-Individual-DataSet
local.output=output
#outputs/Final-DataSet outputs/knn inputs
local.knn.test.input=inputs
local.knn.output=outputs/knn
local.pr.input=outputs/Final-Individual-DataSet/paper
local.pr.output=outputs/pagerank
local.final.total=outputs/Final-DataSet
# Pseudo-Cluster Execution
hdfs.user.name=hduser
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.17.0
aws.region=us-east-1
aws.bucket.name=abhishek-mr
aws.subnet.id=subnet-5f87ff50
aws.dataset=/knn/input1
aws.testset=/knn/input2
aws.output=knn/output/10Nodes/Run1
aws.log.dir=knn/log/10Nodes/Run1
aws.cluster.name="knn 10 Nodes Run 1"
aws.num.nodes=10
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
#nodes outputs/intermediate edges outputs/Final-Individual-DataSet outputs/Final-DataSet
local: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${local.nodes} ${local.intermediate} ${local.edges} ${local.final.individual} ${local.final.total}

#nodes outputs/intermediate edges outputs/Final-Individual-DataSet outputs/Final-DataSet
pr: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.pr} ${local.pr.input} ${local.pr.output}

knn: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.knn} ${local.final.total} ${local.knn.output} ${local.knn.test.input}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
aws: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name ${aws.cluster.name} \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.dataset}","s3://${aws.bucket.name}/${aws.output}","s3://${aws.bucket.name}/${aws.testset}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate \
		--ec2-attributes SubnetId=${aws.subnet.id}

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f Hw1-Twitter-Followers-Hadoop.tar.gz
	rm -f Hw1-Twitter-Followers-Hadoop.zip
	rm -rf build
	mkdir -p build/deliv/Hw1-Twitter-Followers-Hadoop
	cp -r src build/deliv/Hw1-Twitter-Followers-Hadoop
	cp -r config build/deliv/Hw1-Twitter-Followers-Hadoop
	cp -r input build/deliv/Hw1-Twitter-Followers-Hadoop
	cp pom.xml build/deliv/Hw1-Twitter-Followers-Hadoop
	cp Makefile build/deliv/Hw1-Twitter-Followers-Hadoop
	cp README.txt build/deliv/Hw1-Twitter-Followers-Hadoop
	tar -czf Hw1-Twitter-Followers-Hadoop.tar.gz -C build/deliv Hw1-Twitter-Followers-Hadoop
	cd build/deliv && zip -rq ../../Hw1-Twitter-Followers-Hadoop.zip Hw1-Twitter-Followers-Hadoop
