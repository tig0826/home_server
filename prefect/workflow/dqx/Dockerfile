# ベースイメージを指定
FROM python:3.11.6

ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false \
    \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    PYSETUP_PATH="/opt/pysetup"

# SeleniumのChromeドライバを設定
ENV CHROME_DRIVER_PATH="/opt/prefect/chromedriver-linux64/chromedriver"

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN pip install poetry
# 必要なライブラリをインストール
RUN apt-get update && apt-get install -y \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# 必要なPythonパッケージをインストール
RUN mkdir -p /opt/prefect
COPY pyproject.toml /opt/prefect/pyproject.toml
WORKDIR /opt/prefect
RUN poetry install --only main

RUN mkdir -p /opt/prefect/chromedriver_linux64
COPY chromedriver-linux64/chromedriver /opt/prefect/chromedriver-linux64/chromedriver

# flowをコピー
COPY get_trade_history/get_trade_history.py /opt/prefect/get_trade_history/get_trade_history.py
COPY get_price/get_price.py /opt/prefect/get_price/get_price.py