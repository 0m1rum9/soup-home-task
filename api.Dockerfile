FROM python:3.13-slim-bookworm
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/
WORKDIR /api
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --extra api --no-install-project
COPY . .
RUN uv sync --locked --no-dev --extra api
ENTRYPOINT ["uv", "run", "soup-internship"]

