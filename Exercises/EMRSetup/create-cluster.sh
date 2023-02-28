echo "${OPTARG}"
while getopts 'c:b:k:' arg; do
  case ${arg} in
    c)
	CLUSTER_NAME="${OPTARG}"
	;;
    b) 
	BOOTSTRAP_NAME="${OPTARG}"
	;;
    k) 
	KEY_NAME="${OPTARG}" 
	echo "----------";;
  esac
done

echo ${CLUSTER_NAME}
echo ${BOOTSTRAP_NAME}
echo ${KEY_NAME}
echo "Starting creating cluster..."

aws emr create-cluster \
--name ${CLUSTER_NAME} \
--use-default-roles \
--release-label emr-6.6.0 \
--applications Name=Spark \
--bootstrap-actions Path=${BOOTSTRAP_NAME} \
--ec2-attributes KeyName=${KEY_NAME} \
--instance-type 'm5.xlarge' \
--instance-count 3 \
--configurations 'file://emr_configurations.json' \
--region us-east-1
