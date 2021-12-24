Copy jar to kafka cluster
```
scp target\AzureKafka-1.0-SNAPSHOT.jar sshuser@dmp-data-kafka-primary-cluster-ssh.azurehdinsight.net:AzureKafka.jar
```

Get brokers and zk hosts in ssh
```
sudo apt -y install jq
export clusterName='dmp-data-kafka-primary-cluster'
export password='<password>'
export KAFKAZKHOSTS=$(curl -sS -u admin:$password -G https://$clusterName.azurehdinsight.net/api/v1/clusters/$clusterName/services/ZOOKEEPER/components/ZOOKEEPER_SERVER | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")' | cut -d',' -f1,2);
export KAFKABROKERS=$(curl -sS -u admin:$password -G https://$clusterName.azurehdinsight.net/api/v1/clusters/$clusterName/services/KAFKA/components/KAFKA_BROKER | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")' | cut -d',' -f1,2);
```

Run jar for producer in ssh
```
java -jar AzureKafka-1.0-SNAPSHOT.jar producer $KAFKABROKERS <topicName>
```

Run jar for consumer in ssh
```
java -jar AzureKafka-1.0-SNAPSHOT.jar consumer $KAFKABROKERS <topicName>
```

Run jar for list topics in ssh
```
java -jar AzureKafka-1.0-SNAPSHOT.jar list $KAFKABROKERS
```