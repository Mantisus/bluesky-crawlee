FROM apify/actor-python:3.13

ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-install-project --no-editable -q --no-dev

COPY . .

RUN uv sync --frozen --no-editable -q --no-dev

CMD ["actor-run"]