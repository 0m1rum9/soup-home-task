# Home-task for soup
:D



## Requirements:
- docker
- uv for chaos

## Installation
```bash
git clone https://github.com/0m1rum9/soup-home-task.git
cd soup-home-task
```

## Run
Initially, API and workers do not require .env, but chaos script does, but he is not launched in the docker-compose
```bash
docker-compose up
```
The API is available on http://localhost:8000

## Run chaos script
1. See chaos/README.md
