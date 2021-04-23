while getopts c: flag
do
    case "${flag}" in
        c) ClUSTER_ID=${OPTARG};;
    esac
done

aws emr terminate-clusters --cluster-ids $ClUSTER_ID
