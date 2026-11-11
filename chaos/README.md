# Chaos harness

## Create .env from .env.example in the project dir
```sh
cp .env.example .env
  
```
## Setup an API, DB and workers
```sh
docker compose -f docker-compose.yml -f chaos/docker-compose.chaos.yml up -d --build --scale worker=8
```

## Running:
```sh
uv run --project chaos python chaos/harness.py --seed 12345
```


Results are in `runs/`: for each launch harness creates `raw.log` and `result.json`; `INDEX.tsv`.


