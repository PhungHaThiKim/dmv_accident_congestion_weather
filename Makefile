up-clickhouse:
	cd ./docker-setup/clickhouse && docker-compose -f docker-compose.yml up -d
down-clickhouse:
	cd ./docker-setup/clickhouse && docker-compose -f docker-compose.yml down
create-database:
	docker exec -it clickhouse clickhouse-client -q "CREATE DATABASE IF NOT EXISTS bronze;"
	docker exec -it clickhouse clickhouse-client -q "CREATE DATABASE IF NOT EXISTS silver;"
	docker exec -it clickhouse clickhouse-client -q "CREATE DATABASE IF NOT EXISTS gold;"
	
build-airflow:
	cd ./docker-setup/airflow && docker-compose -f docker-compose.yml build
up-airflow:
	cd ./docker-setup/airflow && docker-compose -f docker-compose.yml up -d
down-airflow:
	cd ./docker-setup/airflow && docker-compose -f docker-compose.yml down

up-superset:
	cd ./docker-setup/superset && docker-compose -f docker-compose-image-tag.yml up -d
down-superset:
	cd ./docker-setup/superset && docker-compose -f docker-compose-image-tag.yml up -d