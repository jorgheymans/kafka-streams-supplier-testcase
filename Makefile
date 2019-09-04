kafka-topics:
	docker-compose exec kafka kafka-topics --zookeeper zookeeper:2181 --create --topic supplier-test-case-input --partitions 1 --replication-factor 1 --if-not-exists
	docker-compose exec kafka kafka-topics --zookeeper zookeeper:2181 --create --topic supplier-test-case-output --partitions 1 --replication-factor 1 --if-not-exists
