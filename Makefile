PROJECT_NAME=bigdata-env

build:
	docker build -t $(PROJECT_NAME) .

up:
	docker-compose up

down:
	docker-compose down
	docker container rm -f $(PROJECT_NAME)

clean:
	docker-compose down --rmi all --volumes --remove-orphans
	docker system prune -af

shell:
	docker exec -it bigdata-container bash