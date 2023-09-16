build:
	docker build . -t server

run:
	docker run --env GOOGLE_API_KEY=${GOOGLE_API_KEY} -p 8000:8000 server

deploy:
	flyctl deploy --local-only