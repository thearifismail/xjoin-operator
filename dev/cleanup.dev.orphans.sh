#!/bin/bash
kubectl get xjoindatasourcesynchronizer -o custom-columns=name:metadata.name --no-headers | while read -r datasourcesynchronizer ; do
  kubectl patch xjoindatasourcesynchronizer "$datasourcesynchronizer" -p '{"metadata":{"finalizers":null}}' --type=merge
  kubectl delete xjoindatasourcesynchronizer "$datasourcesynchronizer"
done

kubectl get xjoindatasource -o custom-columns=name:metadata.name --no-headers | while read -r datasource ; do
  kubectl patch xjoindatasource "$datasource" -p '{"metadata":{"finalizers":null}}' --type=merge
  kubectl delete xjoindatasource "$datasource"
done

kubectl get xjoinindex -o custom-columns=name:metadata.name --no-headers | while read -r index ; do
  kubectl patch xjoinindex "$index" -p '{"metadata":{"finalizers":null}}' --type=merge
  kubectl delete xjoinindex "$index"
done

kubectl get xjoinindexsynchronizer -o custom-columns=name:metadata.name --no-headers | while read -r indexsynchronizer ; do
  kubectl patch xjoinindexsynchronizer "$indexsynchronizer" -p '{"metadata":{"finalizers":null}}' --type=merge
  kubectl delete xjoinindexsynchronizer "$indexsynchronizer"
done

kubectl get xjoinindexvalidator -o custom-columns=name:metadata.name --no-headers | while read -r indexvalidator ; do
  kubectl patch xjoinindexvalidator "$indexvalidator" -p '{"metadata":{"finalizers":null}}' --type=merge
  kubectl delete xjoinindexvalidator "$indexvalidator"
done

kubectl get deployments -n test -o custom-columns=name:metadata.name --no-headers | grep xjoin-core | while read -r xjoincore ; do
  kubectl delete deployment "$xjoincore"
done

echo "Deleting connectors.."
kubectl -n test get KafkaConnector -o custom-columns=name:metadata.name | grep indexsynchronizer | while read -r connector ; do
    kubectl delete KafkaConnector "$connector" -n test
done
kubectl -n test get KafkaConnector -o custom-columns=name:metadata.name | grep datasourcesynchronizer | while read -r connector ; do
    kubectl delete KafkaConnector "$connector" -n test
done

echo "Deleting topics.."
kubectl -n test get KafkaTopic -o custom-columns=name:metadata.name | grep indexsynchronizer | while read -r topic ; do
    kubectl delete KafkaTopic "$topic" -n test
done
kubectl -n test get KafkaTopic -o custom-columns=name:metadata.name | grep datasourcesynchronizer | while read -r topic ; do
    kubectl delete KafkaTopic "$topic" -n test
done

echo "Deleting avro subjects.."
artifacts=$(curl "http://localhost:1080/apis/registry/v2/search/artifacts?limit=100" | jq '.artifacts|map(.id)|@sh')
artifacts=($artifacts)
total=${#artifacts[@]}
for i in "${!artifacts[@]}"; do
    if [ "$total" -eq 1 ]; then
      artifact=${artifacts[$i]}
      artifact="${artifact:2}"
      artifact="${artifact::-2}"
    elif [ "$i" -eq 0 ]; then
      echo "At the start"
      artifact=${artifacts[$i]}
      artifact="${artifact:2}"
      artifact="${artifact::-1}"
    elif [ "$i" -eq "$total-1" ]; then
      echo "At the end"
      artifact=${artifacts[$i]}
      artifact="${artifact:1}"
      artifact="${artifact::-2}"
    else
      echo "In the middle"
      artifact=${artifacts[$i]}
      artifact="${artifact:1}"
      artifact="${artifact::-1}"
    fi
    echo "$artifact"
    curl -X DELETE http://localhost:1080/apis/registry/v1/artifacts/$artifact
done

echo "Deleting replication slots"
HBI_USER=$(kubectl get secret/host-inventory-db -o custom-columns=:data.username | base64 -d)
HBI_NAME=$(kubectl get secret/host-inventory-db -o custom-columns=:data.name | base64 -d)
psql -U "$HBI_USER" -h localhost -p 5432 -d "$HBI_NAME" -t -c "SELECT slot_name from pg_catalog.pg_replication_slots" | while read -r slot ; do
  psql -U "$HBI_USER" -h localhost -p 5432 -d "$HBI_NAME" -c "SELECT pg_drop_replication_slot('$slot');"
done

CATS_USER=$(kubectl get secret/cats-db -o custom-columns=:data.username | base64 -d)
CATS_NAME=$(kubectl get secret/cats-db -o custom-columns=:data.name | base64 -d)
psql -U "$CATS_USER" -h localhost -p 5433 -d "$CATS_NAME" -t -c "SELECT slot_name from pg_catalog.pg_replication_slots" | while read -r slot ; do
  psql -U "$CATS_USER" -h localhost -p 5433 -d "$CATS_NAME" -c "SELECT pg_drop_replication_slot('$slot');"
done

echo "Deleting ES indexes"
ES_PASSWORD=$(kubectl get secret/xjoin-elasticsearch-es-elastic-user -o custom-columns=:data.elastic | base64 -d)
curl -u "elastic:$ES_PASSWORD" http://localhost:9200/_cat/indices\?format\=json | jq '.[] | .index' | grep xjoinindexsynchronizer | while read -r index ; do
  index="${index:1}"
  index="${index::-1}"
  curl -u "elastic:$ES_PASSWORD" -X DELETE "http://localhost:9200/$index"
done

echo "Deleting subgraph pods"
kubectl delete deployments --selector='xjoin.index=xjoinindexsynchronizer-hosts'
kubectl delete deployments --selector='xjoin.index=xjoinindexsynchronizer-cats'
kubectl delete deployments --selector='xjoin.index=xjoinindexsynchronizer-cats'
kubectl delete deployments --selector='xjoin.index=xjoinindexsynchronizer-hosts-hbi-tags'

kubectl delete services --selector='xjoin.index=xjoinindexsynchronizer-hosts'
kubectl delete services --selector='xjoin.index=xjoinindexsynchronizer-cats'
kubectl delete services --selector='xjoin.index=xjoinindexsynchronizer-cats'
kubectl delete services --selector='xjoin.index=xjoinindexsynchronizer-hosts-hbi-tags'
