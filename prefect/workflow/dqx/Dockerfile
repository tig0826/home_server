# ベースイメージを指定
FROM python:3.11.6-slim

ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false \
    \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    PYSETUP_PATH="/opt/pysetup"

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN pip install poetry
# 必要なライブラリをインストール
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 必要なPythonパッケージをインストール
RUN mkdir -p /opt/prefect
RUN mkdir -p /opt/prefect/common
COPY pyproject.toml /opt/prefect/pyproject.toml
WORKDIR /opt/prefect
RUN poetry install --only main

# flowをコピー
COPY get_itemname/get_itemname.py /opt/prefect/get_itemname/get_itemname.py
COPY get_item_hash_hiroba/get_item_hash.py /opt/prefect/get_item_hash_hiroba/get_item_hash.py
COPY get_trade_history/get_trade_history.py /opt/prefect/get_trade_history/get_trade_history.py
COPY get_price/get_price.py /opt/prefect/get_price/get_price.py
COPY common/ /opt/prefect/common
