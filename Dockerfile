# 軽量ベース。Cloud Run Jobs 対応
FROM python:3.12-slim

# 環境
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Tokyo

# OS パッケージ（証明書/タイムゾーン + Cビルド依存）
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates tzdata \
        build-essential gcc \
        libxml2-dev libxslt1-dev \
        libjpeg62-turbo-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# 依存ライブラリ
WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# アプリ本体（ローカルの *.py をコピー）
# 例: function_api_payload_from_sprinklr.py / sprinklr_client.py も同じディレクトリにある前提
COPY . .

# デフォルト実行コマンド
# Cloud Run Jobs 側で --args を渡せば上書きされます
ENTRYPOINT ["python", "-u", "main.py"]