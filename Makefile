# start containers and view their logs
up:
	docker-compose up --build -d && docker-compose logs -f

# stop containers
down:
	docker-compose down

# stop containers and then start
down_up:
	docker-compose down && docker-compose up --build -d && docker-compose logs -f

# run tests
test:  # running from python -m way ensures that our code folders are added in sys path 
	python -m pytest -vvvs .  

