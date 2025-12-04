'''
gcloud auth application-default login

PROJECT_ID=dev-peak
REGION=asia-northeast1
IMAGE_URL=asia-northeast1-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/chronoro-sprinklr:latest
SA_EMAIL=chronoro-ui-sa@${PROJECT_ID}.iam.gserviceaccount.com

# ビルド
gcloud builds submit --tag "${IMAGE_URL}"

# ジョブ作成/更新
gcloud run jobs deploy urlcloner-sprinklr-job \
  --region="${REGION}" \
  --image="${IMAGE_URL}" \
  --service-account="${SA_EMAIL}" \
  --tasks=20 \
  --parallelism=8 \
  --task-timeout=3600s \
  --max-retries=1 \
  --set-env-vars='^|^GOOGLE_CLOUD_PROJECT='"${PROJECT_ID}"'|TASK_COUNT=20|WORKERS_CLASSIFY=8|WORKERS_TRANSLATE=12|PAYLOAD_PATH=gs://urlcloner/inputs/sprinklr_api_payload/payload_bouei_hakusho.json|MASTER_MEDIA_CSV=gs://urlcloner/data/master_media_data-utf8.csv|OUTPUT_BASE_PREFIX=gs://urlcloner/outputs/|OUTPUT_NAME_STEM=filtered_data_from_sprinklr_with_article|RUN_ID='"${RUN_ID}"'' \
  --set-secrets=SLACK_WEBHOOK_URL=slack_webhook:latest
'''

# ===== インポート =====
import argparse
import json
import os
import re
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse
import html as html_lib

import pandas as pd
from zoneinfo import ZoneInfo

import function_api_payload_from_sprinklr as api
from sprinklr_client import spr

# Gemini / Vertex AI
from google import genai
from google.genai import types as genai_types

# Additional imports for GCS, threading, etc.
import time
import requests
from pathlib import Path
from google.cloud import storage  # type: ignore
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from threading import Lock


# ===== 定数・設定 =====
# TODO:必要ない項目多いのでいずれ削除する
TZ = ZoneInfo(os.getenv("TZ"))
PROJECT_FALLBACK = os.getenv("PROJECT_FALLBACK")
REGION = os.getenv("REGION")  # prefer Tokyo
MODEL_NAME = os.getenv("MODEL_NAME")
PAYLOAD_PATH = os.getenv("PAYLOAD_PATH")
MASTER_MEDIA_CSV = os.getenv("MASTER_MEDIA_CSV")
OUTPUT_CSV = os.getenv("OUTPUT_CSV")


# Env overrides and output name stem
PAYLOAD_PATH = os.getenv("PAYLOAD_PATH", PAYLOAD_PATH)  # [ENV]
MASTER_MEDIA_CSV = os.getenv("MASTER_MEDIA_CSV", MASTER_MEDIA_CSV)  # [ENV]
OUTPUT_CSV = os.getenv("OUTPUT_CSV", OUTPUT_CSV)  # can be gs://... or local path  # [ENV]
OUTPUT_NAME_STEM = os.getenv("OUTPUT_NAME_STEM", "filtered_data_from_sprinklr_with_article")  # [ENV]


# Run/session id used in output paths (default: timestamp)
RUN_ID = os.getenv("RUN_ID") or time.strftime("%Y%m%d-%H%M%S")  # [ENV]
# Base GCS prefix for outputs (must end with '/' when using GCS directory mode)
OUTPUT_BASE_PREFIX = os.getenv("OUTPUT_BASE_PREFIX", OUTPUT_CSV if isinstance(OUTPUT_CSV, str) else "")  # [ENV]

# Shared folder id for this job execution so that all tasks write into the same directory
# Priority: explicit RUN_FOLDER > Cloud Run execution id > RUN_ID > timestamp
RUN_FOLDER = (
    os.getenv("RUN_FOLDER")  # [ENV]
    or os.getenv("CLOUD_RUN_EXECUTION")  # provided by Cloud Run Jobs; stable across tasks of one execution  # [ENV]
    or RUN_ID
)


# ===== GenAIレート制限・バックオフ =====
# Control overall QPS to avoid 429 RESOURCE_EXHAUSTED. Tunable via env.
GENAI_RATE_INTERVAL_SEC = float(os.getenv("GENAI_RATE_INTERVAL_SEC", "3.0"))  # e.g., 3.0–5.0  # [ENV]
_GENAI_LAST_TS = 0.0
_GENAI_LOCK = Lock()

def _throttle_genai():
    """Globally throttle GenAI calls to ~1/GENAI_RATE_INTERVAL_SEC."""
    global _GENAI_LAST_TS
    with _GENAI_LOCK:
        now = time.time()
        wait = (_GENAI_LAST_TS + GENAI_RATE_INTERVAL_SEC) - now
        if wait > 0:
            time.sleep(wait)
        _GENAI_LAST_TS = time.time()


def _genai_with_backoff_call(call):
    """Call a 0-arg function with throttle + exponential backoff for 429s."""
    delay = 1.0  # seconds
    for attempt in range(7):  # up to ~1 + 2 + 4 + ...
        _throttle_genai()
        try:
            return call()
        except Exception as e:
            s = str(e)
            # Retry only for clear capacity/limit signals
            if ("RESOURCE_EXHAUSTED" in s) or (" 429" in s) or ('"code": 429' in s) or ("quota" in s.lower()):
                jitter = random.uniform(0.0, 0.8)
                time.sleep(delay + jitter)
                delay = min(delay * 2, 20.0)
                continue
            raise
    raise RuntimeError("GenAI call failed after 7 retries due to capacity limits")


# ===== プロジェクト・環境ヘルパー =====
def _ensure_project_env() -> Optional[str]:
    pid = os.getenv("GOOGLE_CLOUD_PROJECT")  # [ENV]
    if pid:
        return pid
    try:
        import google.auth  # type: ignore
        _, proj = google.auth.default()
        if proj:
            os.environ["GOOGLE_CLOUD_PROJECT"] = proj  # [ENV set]
            return proj
    except Exception as e:
        print(f"[WARN] Could not resolve project id from ADC: {e}")
    return None


def _get_project_id() -> Optional[str]:
    return os.getenv("GOOGLE_CLOUD_PROJECT") or _ensure_project_env()  # [ENV]


# ===== GCSヘルパー =====
def _parse_gs(gs_uri: str) -> tuple[str, str]:
    assert gs_uri.startswith("gs://"), gs_uri
    bucket, key = gs_uri[5:].split("/", 1)
    return bucket, key

def _storage_client() -> storage.Client:
    return storage.Client()

def _adc_principal() -> str:
    # Best-effort principal identification for logs
    return os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL") or os.getenv("SA_EMAIL") or os.getenv("GOOGLE_AUTH_PRINCIPAL") or "unknown"  # [ENV]

def _test_gcs_perms(gs_uri: str) -> list[str]:
    try:
        bkt, _ = _parse_gs(gs_uri)
        client = _storage_client()
        perms = client.bucket(bkt).test_iam_permissions(
            [
                "storage.objects.create",
                "storage.objects.get",
                "storage.objects.list",
            ]
        )
        return perms or []
    except Exception:
        return []

def _download_gs_to_file(gs_uri: str, local_path: str) -> None:
    bkt, key = _parse_gs(gs_uri)
    client = _storage_client()
    blob = client.bucket(bkt).blob(key)
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(local_path)

def _upload_file_to_gs(local_path: str, gs_uri: str, content_type: str | None = None) -> str:
    principal = _adc_principal()
    print(f"[GCS][DEBUG] principal={principal} target={gs_uri}")
    allowed = _test_gcs_perms(gs_uri)
    print(f"[GCS][DEBUG] allowed_perms on bucket: {allowed}")
    bkt, key = _parse_gs(gs_uri)
    client = _storage_client()
    blob = client.bucket(bkt).blob(key)
    if content_type:
        blob.content_type = content_type
    try:
        # Create-only to avoid requiring delete permission
        blob.upload_from_filename(local_path, if_generation_match=0)
        return f"gs://{bkt}/{key}"
    except Exception as e:
        msg = str(e)
        if "Precondition" in msg or "412" in msg or "already exists" in msg:
            base, ext = os.path.splitext(key)
            ts = time.strftime("%Y%m%d-%H%M%S")
            new_key = f"{base}__dup_{ts}{ext or ''}"
            print(f"[GCS][WARN] {key} exists; uploading as {new_key} (no delete perms)")
            blob2 = client.bucket(bkt).blob(new_key)
            if content_type:
                blob2.content_type = content_type
            blob2.upload_from_filename(local_path, if_generation_match=0)
            return f"gs://{bkt}/{new_key}"
        print(f"[GCS][ERROR] upload failed for {gs_uri} by {principal}: {e}", file=sys.stderr)
        raise

def _maybe_gs_from_console_url(path: str) -> str:
    # Convert https://storage.cloud.google.com/bucket/key to gs://bucket/key
    if not isinstance(path, str):
        return path
    if path.startswith("https://storage.cloud.google.com/"):
        tail = path.replace("https://storage.cloud.google.com/", "", 1)
        return f"gs://{tail}"
    return path

def _resolve_input_to_local(path: str, *, kind: str) -> str:
    """
    Accepts local path, gs://, or https://storage.cloud.google.com/.
    Returns local filesystem path, downloading to /tmp if needed.
    """
    path = _maybe_gs_from_console_url(path)
    if isinstance(path, str) and path.startswith("gs://"):
        bkt, key = _parse_gs(path)
        local = f"/tmp/inputs/{kind}/{key.split('/')[-1]}"
        _download_gs_to_file(path, local)
        print(f"[GCS][OK] downloaded {path} -> {local}")
        return local
    # local file path
    return path

# ===== 実行単位GCS出力構造ヘルパー =====
def _resolve_output_paths(target: str) -> tuple[str, str]:
    """
    If target is gs://bucket/dir (ends with '/'), create timestamped filename.
    Returns (local_path, gs_uri_final).
    """
    target = _maybe_gs_from_console_url(target)
    ts = time.strftime("%Y%m%d-%H%M%S")
    name = f"{OUTPUT_NAME_STEM}-{ts}.csv"
    if target.startswith("gs://"):
        if target.endswith("/") or target.rstrip().endswith("/"):
            bkt, key_prefix = _parse_gs(target if target.endswith("/") else target + "/")
            gs_uri = f"gs://{bkt}/{key_prefix}{name}"
        else:
            gs_uri = target
        local = f"/tmp/outputs/{name}"
        Path(local).parent.mkdir(parents=True, exist_ok=True)
        return local, gs_uri
    # local output
    local = target if target.lower().endswith(".csv") else os.path.join("/tmp/outputs", name)
    Path(local).parent.mkdir(parents=True, exist_ok=True)
    return local, ""

def _build_output_paths(base_prefix: str, folder_id: str | None = None) -> dict:
    """
    Build GCS output paths under a shared folder so *all tasks of one execution* land in the same place.
    base_prefix: gs://bucket/dir  (with or without trailing '/')
    folder_id: subfolder name (e.g., CLOUD_RUN_EXECUTION). If falsy, use none (flat).
    Returns:
      - base_dir  : gs://.../{folder_id}/
      - parts_dir : gs://.../{folder_id}/parts/
      - final_csv : gs://.../{folder_id}/articles_sprinklr_{folder_id or 'final'}.csv
    """
    if not isinstance(base_prefix, str):
        return {}
    base_prefix = _maybe_gs_from_console_url(base_prefix)
    if not (isinstance(base_prefix, str) and base_prefix.startswith("gs://")):
        return {}
    root = base_prefix if base_prefix.endswith("/") else base_prefix + "/"
    sub = (folder_id or "").strip("/")
    base_dir = f"{root}{sub}/" if sub else root
    parts_dir = f"{base_dir}parts/"
    final_suffix = sub if sub else "final"
    final_csv = f"{base_dir}articles_sprinklr_{final_suffix}.csv"
    return {"base_dir": base_dir, "parts_dir": parts_dir, "final_csv": final_csv}

# ===== GCSヘルパー終わり =====

# ===== タスク・シャーディングヘルパー =====
def _get_task_index_count() -> tuple[int, int]:
    idx = int(os.getenv("TASK_INDEX", os.getenv("CLOUD_RUN_TASK_INDEX", "0")))  # [ENV]
    cnt = int(os.getenv("TASK_COUNT", os.getenv("CLOUD_RUN_TASK_COUNT", "1")))  # [ENV]
    return idx, max(1, cnt)

def _gcs_list(gs_prefix: str) -> list[str]:
    assert gs_prefix.startswith("gs://"), gs_prefix
    bkt, key_prefix = _parse_gs(gs_prefix)
    client = _storage_client()
    return [f"gs://{bkt}/{b.name}" for b in client.list_blobs(bkt, prefix=key_prefix)]

def _parts_prefix_for(gs_target: str, name_stem: str) -> str:
    # gs_target should be a full object path like gs://bucket/dir/file.csv
    bkt, key = _parse_gs(gs_target)
    dirkey = os.path.dirname(key)
    return f"gs://{bkt}/{dirkey}/parts/{name_stem}/"
# ===== タスク・シャーディングヘルパー終わり =====


# ===== Sprinklrヘルパー =====
def whoami() -> None:
    info = spr("GET", "/api/v2/me")
    who = str(info)[:200]
    print(f"[AUTH] /api/v2/me OK @ {who}")


# ===== 言語・翻訳ヘルパー =====
def detect_language_cloud(text: str, project_id: Optional[str] = None) -> Optional[str]:
    """Detect language using Cloud Translation v3 (locations/global).
    Returns BCP-47 code like 'en', 'ja', 'zh-CN', or None on failure.
    """
    if not text or not text.strip():
        return None
    try:
        from google.cloud import translate_v3 as translate  # type: ignore
    except Exception:
        try:
            from google.cloud import translate_v3 as translate  # type: ignore
        except Exception:
            print("[WARN] google-cloud-translate v3 not available.")
            return None

    project_id = project_id or _get_project_id()
    if not project_id:
        print("[WARN] GOOGLE_CLOUD_PROJECT is not set and ADC didn't return a project; skip detectLanguage.")
        return None
    client = translate.TranslationServiceClient()
    parent = f"projects/{project_id}/locations/global"
    try:
        resp = client.detect_language(
            request={
                "parent": parent,
                "content": text,
                "mime_type": "text/plain",
            }
        )
        if resp.languages:
            return resp.languages[0].language_code or None
    except Exception as e:
        print(f"[WARN] detect_language failed: {e}")
    return None


def translate_text_llm(text: str, *, target_lang: str = "ja", source_lang: Optional[str] = None) -> Optional[str]:
    """Simple translator using Cloud Translation v3 translateText (locations/global).
    Returns translated string, "" for empty input, or None on failure.
    """
    if not text or not text.strip():
        return ""
    proj = _get_project_id()
    if not proj:
        print("[WARN] No project id; skip translateText v3.")
        return None
    try:
        from google.cloud import translate_v3 as translate  # type: ignore
    except Exception as e:
        print(f"[WARN] google-cloud-translate v3 not available: {e}")
        return None
    client = translate.TranslationServiceClient()
    parent = f"projects/{proj}/locations/global"
    req: Dict[str, Any] = {
        "parent": parent,
        "contents": [text],
        "mime_type": "text/plain",
        "target_language_code": target_lang,
    }
    if source_lang:
        req["source_language_code"] = source_lang
    try:
        resp = client.translate_text(request=req)
        if resp and resp.translations:
            return resp.translations[0].translated_text
        print("[WARN] translateText v3 returned no translations.")
    except Exception as e:
        print(f"[WARN] translateText v3 failed: {e}")
    return None


# ===== Gemini分類器・翻訳器 =====
PROMPT_TEMPLATE = """あなたは **ニュース本文の行整形・行分類器（多言語対応）** です。  
対応言語は以下です:  
ja / en / zh / ru / ko / fr / vi / de / es / pt / it / nl / pl / ar / tr / he / fa  

以下を行い、JSONのみ返してください。生成・要約・言い換え・翻訳は禁止。入力原文を厳密に保持します。

1. 行分割（mode は paragraph または sentence）
2. 各行のラベル付け: body / caption / nav / title_dup / other

---

## 設定
mode: "paragraph"    # "sentence" にしても可（文末記号で分割）
title: "{title}"            # 記事タイトルが不明なら空文字で可
lang: "auto"         # 自動判定（複数混在可）

---

## 分割規則
- 共通:
  - 先頭/末尾の不要スペースをトリム
  - 空行は捨てる
  - 引用符はペアが閉じるまで同一行に含める
- paragraph: 連続空行で段落区切り
- sentence: 文末記号で分割
- 文末記号:
  - ja/zh/ko: 。！？ (全角/半角)
  - en/fr/de/es/pt/it/nl/pl: . ! ?
  - ru: . ! ? …
  - vi: . ! ?
  - ar: . ! ؟
  - tr: . ! ?
  - he: . ! ? ׃
  - fa: . ! ؟
- 引用符: 言語ごとに閉じ記号まで含める（例: 「」『』“” « » 『』 “” ‘’ “” ״ „ …）
- 箇条書き (1. / - / ・) は分割しない

---

## ラベル基準

### caption（写真キャプション/クレジット）
- 典型パターン: 地名 + 日付 + 撮影者/提供、括弧内クレジット
- 言語別キーワード例:
  - ja: （撮影）（提供）, 写真提供, 共同, 時事, ロイター, AP, 撮影＝, 提供＝
  - en: Photo:, Photograph:, Credit:, Courtesy of, via, Reuters, AP, Getty
  - zh: 图, 图自, 图片来源, 新华社, 路透, 美联社, 供图, 摄影
  - ru: Фото:, Снимок:, Фото предоставлено, РИА, ТАСС, Reuters, AP
  - ko: 사진:, 제공:, 연합뉴스, 로이터, AP, 게티
  - fr: Photo :, Crédit :, Avec l’aimable autorisation, via, AFP, Reuters, AP
  - vi: Ảnh:, Ảnh do, Nguồn:, Reuters, AP, Getty
  - de: Foto:, Bild:, Quelle:, Mit freundlicher Genehmigung, dpa, Reuters, AP
  - es: Foto:, Imagen:, Crédito:, Cortesía de, vía, EFE, Reuters, AP
  - pt: Foto:, Imagem:, Crédito:, Cortesia de, via, Lusa, Reuters, AP
  - it: Foto:, Immagine:, Credito:, Per gentile concessione, via, ANSA, Reuters, AP
  - nl: Foto:, Afbeelding:, Bron:, Met dank aan, via, ANP, Reuters, AP
  - pl: Zdjęcie:, Fot., Źródło:, Dzięki uprzejmości, PAP, Reuters, AP
  - ar: صورة:, تصوير:, المصدر:, وكالة, رويترز, أسوشيتد برس
  - tr: Fotoğraf:, Görsel:, Kaynak:, Anadolu Ajansı, Reuters, AP
  - he: צילום:, קרדיט:, באדיבות, רויטרס, AP
  - fa: عکس:, تصویر:, منبع:, خبرگزاری, رویترز, آسوشیتدپرس

---

### nav（UI誘導・ナビゲーション）
- 例: トップ, 会員, ログイン, 新着, ランキング, 関連記事, 広告, PR
- 言語別キーワード例:
  - en: Home, Subscribe, Sign in, Trending, Recommended, Related, Advertisement, Sponsored, Share, Comments, Next, Previous
  - ja: トップ, 国際, 会員, ログイン, 新着, ランキング, 関連記事, 広告, PR, 購読, タグ, シェア, コメント, 次へ, 前へ
  - zh: 首页, 登录, 热门, 推荐, 相关新闻, 广告, 赞助, 分享, 评论, 下一页, 上一页
  - ru: Главная, Войти, Подписка, Популярное, Рекомендуемое, Реклама, Поделиться, Комментарии, Далее, Назад
  - ko: 홈, 로그인, 구독, 인기, 추천, 관련 기사, 광고, 스폰서, 공유, 댓글, 다음, 이전
  - fr: Accueil, Connexion, S’abonner, Tendance, Recommandé, Articles liés, Publicité, Sponsorisé, Partager, Commentaires, Suivant, Précédent
  - vi: Trang chủ, Đăng nhập, Đăng ký, Xu hướng, Đề xuất, Bài liên quan, Quảng cáo, Tài trợ, Chia sẻ, Bình luận, Tiếp, Trước
  - de: Startseite, Anmelden, Abonnieren, Trends, Empfohlen, Verwandte Artikel, Werbung, Gesponsert, Teilen, Kommentare, Weiter, Zurück
  - es: Inicio, Suscribirse, Iniciar sesión, Tendencias, Recomendado, Relacionados, Publicidad, Patrocinado, Compartir, Comentarios, Siguiente, Anterior
  - pt: Início, Assinar, Entrar, Tendências, Recomendado, Relacionados, Publicidade, Patrocinado, Compartilhar, Comentários, Seguinte, Anterior
  - it: Home, Abbonati, Accedi, Tendenze, Consigliato, Articoli correlati, Pubblicità, Sponsorizzato, Condividi, Commenti, Successivo, Precedente
  - nl: Startpagina, Inloggen, Abonneren, Trends, Aanbevolen, Gerelateerd, Advertentie, Gesponsord, Delen, Reacties, Volgende, Vorige
  - pl: Strona główna, Zaloguj się, Subskrybuj, Trendy, Polecane, Powiązane artykuły, Reklama, Sponsorowane, Udostępnij, Komentarze, Następny, Poprzedni
  - ar: الصفحة الرئيسية, تسجيل الدخول, اشتراك, شائع, موصى به, مقالات ذات صلة, إعلان, برعاية, مشاركة, تعليقات, التالي, السابق
  - tr: Anasayfa, Giriş yap, Abone ol, Trend, Önerilen, İlgili makaleler, Reklam, Sponsorlu, Paylaş, Yorumlar, Sonraki, Önceki
  - he: דף הבית, התחברות, הירשם, מגמות, מומלץ, מאמרים קשורים, פרסומת, ממומן, שתף, תגובות, הבא, הקודם
  - fa: صفحه اصلی, ورود, اشتراک, داغ, پیشنهادی, مقالات مرتبط, تبلیغات, اسپانسری, اشتراک‌گذاری, نظرات, بعدی, قبلی

---

### title_dup
- 記事タイトルとほぼ同一
- 正規化して比較（大文字小文字無視、NFKC正規化、句読点・記号・余分空白除去）
- title が空なら通常該当なし

---

### body
- 上記以外の本文の平叙文
- 事実説明・引用・見解を含む文
- 段落見出しも本文扱い（navに該当しない限り）

---

### other
- 断片・残骸・URLのみ・タグや装飾記号列など意味が薄い行

---

## 出力形式
- 配列形式（推奨）: [ { "text": "...", "label": "body|caption|nav|title_dup|other", "confidence": 0-1, "index": 0 } , ... ]
- もしくはオブジェクト形式: { "lines": [ { "text": "...", "label": "...", "confidence": 0-1, "index": 0 }, ... ] }

## 厳守事項
- 入力の語句・順序・句読点を一切改変しない
- 翻訳・生成・補完をしない
- JSON以外のテキストは出力禁止
- confidenceはヒューリスティクスに応じて設定

text: |
{body}
"""




def _strip_code_fences(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return re.sub(r"^```(?:json)?\s*|\s*```$", "", s.strip(), flags=re.DOTALL)


def _parse_json_maybe(s: str):
    if not s:
        return None
    t = _strip_code_fences(s)
    try:
        return json.loads(t)
    except Exception:
        pass
    m_arr = re.search(r"\[.*\]", t, flags=re.DOTALL)
    if m_arr:
        try:
            return json.loads(m_arr.group(0))
        except Exception:
            pass
    m_obj = re.search(r"\{.*\}", t, flags=re.DOTALL)
    if m_obj:
        try:
            return json.loads(m_obj.group(0))
        except Exception:
            pass
    return None


def _norm(s: str) -> str:
    import unicodedata
    if not isinstance(s, str):
        return ""
    t = unicodedata.normalize("NFKC", s)
    t = re.sub(r"[\s\u3000]+", " ", t).strip()
    t = re.sub(r"[\W_]+", "", t, flags=re.UNICODE)
    return t.lower()


def _build_gemini_client(project_id: Optional[str]) -> genai.Client:
    proj = project_id or _get_project_id() or PROJECT_FALLBACK
    return genai.Client(vertexai=True, project=proj, location=REGION)


###########################
# --- Pre-segmentation helper for dense scraped text ---
_SENTINELS = [
    r"관련\s*기사", r"관련\s*키워드", r"関連記事", r"関連\s*キーワード",
    r"기자\b", r"記者\b", r"사진[:：]", r"제공\)?$",
    r"Photo\s*:\s*", r"Image\s*:\s*", r"Credit\s*:\s*",
]
_SENTINEL_RE = re.compile("|".join(_SENTINELS), re.IGNORECASE)

def _presegment_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    s = (text or "").strip()
    if not s:
        return ""
    # 1) Insert a newline before sentinel blocks so nav/caption become separate lines
    s = _SENTINEL_RE.sub(lambda m: "\n" + m.group(0), s)
    # 2) If the text is very dense (few newlines), add newlines after sentence enders
    if s.count("\n") < 2 and len(s) > 300:
        # CJK sentence enders
        s = re.sub(r"([。！？])", r"\1\n", s)
        # Latin punctuation enders
        s = re.sub(r"([.!?])\s+", r"\1\n", s)
        # Collapse excessive newlines
        s = re.sub(r"\n{3,}", "\n\n", s)
    return s

# --- Noise filters for residual caption/nav/credits lines ---
_NOISE_PATTERNS = [
    # Japanese
    r"^\s*関連記事\s*$", r"^\s*関連キーワード\s*$", r"提供\)?$", r"^\s*写真[:：]",
    r"記者\s*$",
    # Korean
    r"관련\s*기사", r"관련\s*키워드", r"제공\)?$", r"사진[:：]?",
    r"기자\s*$",
    # English
    r"^\s*Related\s+Articles?\s*$", r"^\s*Related\s*$", r"Photo\s*:\s*", r"Image\s*:\s*", r"Credit\s*:\s*",
    r"^\s*(Subscribe|Recommended|Trending|Advertisement|Sponsored)\b",
]
_NOISE_RE = re.compile("|".join(_NOISE_PATTERNS), flags=re.IGNORECASE)

def _is_noise_line(txt: str, title_text: str = "") -> bool:
    if not isinstance(txt, str):
        return True
    t = txt.strip()
    if not t:
        return True
    # very short fragments
    if len(t) <= 3:
        return True
    # direct title duplicate guard (again)
    if title_text and _norm(t) == _norm(title_text):
        return True
    # credit / caption / nav patterns
    if _NOISE_RE.search(t):
        return True
    return False
###########################

def classify_and_extract_article(message_text: str, title_text: str = "", client: Optional[genai.Client] = None) -> str:
    if not isinstance(message_text, str) or not message_text.strip():
        return ""
    client = client or _build_gemini_client(_get_project_id())
    msg = _presegment_text(message_text)
    mode = "sentence" if (msg.count("\n") < 2 and len(msg) > 300) else "paragraph"
    prompt = PROMPT_TEMPLATE.replace("{body}", msg.strip()).replace("{title}", title_text or "")
    prompt = prompt.replace('mode: "paragraph"', f'mode: "{mode}"')

    resp = _genai_with_backoff_call(lambda: client.models.generate_content(
        model=MODEL_NAME,
        contents=[prompt],
        config=genai_types.GenerateContentConfig(
            temperature=0.0,
            max_output_tokens=4096,
            response_mime_type="application/json",
        ),
    ))
    raw = resp.text or ""
    obj = _parse_json_maybe(raw)
    # print(obj)
    if not obj:
        return ""

    try:
        if isinstance(obj, list):
            lines = obj
        elif isinstance(obj, dict) and isinstance(obj.get("lines"), list):
            lines = obj["lines"]
        else:
            return ""

        def _idx(x):
            try:
                return int(x.get("index", 0))
            except Exception:
                return 0

        lines = sorted(lines, key=_idx)
        kept: List[str] = []
        title_norm = _norm(title_text)
        for i, ln in enumerate(lines):
            label = str(ln.get("label", "")).strip().lower()
            try:
                conf = float(ln.get("confidence", 0) or 0)
            except Exception:
                conf = 0.0
            txt = ln.get("text") if isinstance(ln.get("text"), str) else ""
            if not txt.strip():
                continue
            txt_norm = _norm(txt)
            if label == "title_dup":
                continue
            if title_norm and txt_norm == title_norm:
                continue
            if i == 0 and title_norm and txt_norm == title_norm:
                continue
            if label == "body" and conf >= 0.9:
                if not _is_noise_line(txt, title_text):
                    kept.append(txt)

        return "\n\n".join(kept)
    except Exception:
        return ""


def _translate_to_ja_with_gemini(text: str, project_id: Optional[str]) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    system_prompt = (
        "You are a professional translator. Translate the given text into Japanese with high fidelity. "
        "Do NOT add, omit, or summarize. Preserve structure (paragraph breaks, lists), numbers, units, URLs, and quoted text. "
        "Keep named entities accurate; prefer original proper nouns with appropriate Japanese katakana only when common. "
        "Return Japanese only."
    )
    proj = project_id or _get_project_id() or PROJECT_FALLBACK

    # Region/model candidates (asia-northeast1 sometimes lacks *-lite)
    if REGION == "asia-northeast1":
        candidates = [
            (REGION, "gemini-2.5-flash"),
            ("us-central1", "gemini-2.5-flash-lite"),
            ("us-central1", "gemini-2.5-flash"),
        ]
    else:
        candidates = [
            (REGION, "gemini-2.5-flash-lite"),
            (REGION, "gemini-2.5-flash"),
            ("us-central1", "gemini-2.5-flash-lite"),
            ("us-central1", "gemini-2.5-flash"),
        ]

    for loc, model in candidates:
        try:
            client = genai.Client(vertexai=True, project=proj, location=loc)
            resp = _genai_with_backoff_call(lambda: client.models.generate_content(
                model=model,
                contents=[text],
                config=genai_types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.0,
                    max_output_tokens=4096,
                    response_mime_type="text/plain",
                ),
            ))
            out = (resp.text or "").strip()
            if out:
                return out
        except Exception as e:
            msg = str(e)
            if "NOT_FOUND" in msg or "not found" in msg.lower():
                print(f"[INFO] Model {model} not found in {loc}; trying next...")
                continue
            print(f"[WARN] Gemini translate failed on {model}@{loc}: {e}")
            continue
    return ""


# ===== DataFrameユーティリティ =====
def _extract_title_from_html(html_text: str) -> str:
    if not isinstance(html_text, str):
        return ""
    m = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    t = m.group(1)
    # remove tags inside title and unescape
    t = re.sub(r"<[^>]+>", "", t)
    return html_lib.unescape(t).strip()

def extract_hostname(url: str) -> str:
    if not isinstance(url, str):
        return ""
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host

def _strip_html_to_text(html_text: str) -> str:
    if not isinstance(html_text, str):
        return ""
    s = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", " ", html_text, flags=re.IGNORECASE)
    s = re.sub(r"<style\b[^<]*(?:(?!</style>)<[^<]*)*</style>", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</(p|div|section|article|li|h[1-6]|blockquote|header|footer)>", "\n\n", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html_lib.unescape(s)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

# ===== 記事抽出ヘルパー =====
def extract_content_from_url(url: str) -> str:
    """
    Placeholder for extracting article content from URL.
    """
    return ""

def _try_import_scraper():
    print("[SCRAPER] probing for scraper module...")
    try:
        import scraper  # type: ignore
        print("[SCRAPER] scraper module imported OK.")
        return scraper
    except Exception:
        print("[SCRAPER] scraper module NOT found in image.")
        return None



def _scrape_article_via_scraper(url: str) -> str:
    """
    Use existing scraper.py without modifying its internals.
    Tries common public functions; returns plain text content if available.
    If the scraper returns HTML, convert to text and return.
    """
    s = _try_import_scraper()
    try:
        names = sorted([n for n in dir(s) if not n.startswith("__")])
        print(f"[SCRAPER] available names: {names[:60]}{'...' if len(names)>60 else ''}")
    except Exception:
        pass
    print(f"[SCRAPER] start url={url!r}")
    # Add guard for valid URL
    if not (isinstance(url, str) and url.strip()):
        print("[SCRAPER] unavailable or blank url; skip.")
        return ""
    if not (url.startswith("http://") or url.startswith("https://")):
        print(f"[SCRAPER] invalid url passed to scraper: {url!r}")
        return ""
    if not s:
        print("[SCRAPER] unavailable scraper module; skip.")
        return ""
    # candidate callables commonly exposed by scrapers
    candidate_names = [
        "get_content", "extract", "get_article", "scrape", "fetch",
        "get_text", "get_article_text", "parse", "content", "article", "crawl"
    ]
    # also support pattern: module.Scraper().get_content(url) etc.
    callables = []
    for name in candidate_names:
        fn = getattr(s, name, None)
        if callable(fn):
            callables.append(fn)
    # If class-based
    if not callables:
        ScraperCls = getattr(s, "Scraper", None)
        if ScraperCls:
            try:
                inst = ScraperCls()
                for name in candidate_names:
                    fn = getattr(inst, name, None)
                    if callable(fn):
                        callables.append(fn)
            except Exception:
                pass
    # Fallback: module-level callable named SCRAPER
    if not callables:
        c = getattr(s, "SCRAPER", None)
        if callable(c):
            callables.append(c)
    if not callables:
        print("[SCRAPER] no callable candidates found in scraper module.")

    def _normalize_scraper_output(out) -> str:
        # Accept str, dict, list/tuple of str/dict
        if isinstance(out, str):
            return out
        if isinstance(out, dict):
            for k in ("content", "text", "body", "article"):
                v = out.get(k)
                if isinstance(v, str) and v.strip():
                    return v
        if isinstance(out, (list, tuple)):
            for elem in out:
                s = _normalize_scraper_output(elem)
                if isinstance(s, str) and s.strip():
                    return s
        return ""

    for fn in callables:
        name = getattr(fn, "__name__", str(fn))
        print(f"[SCRAPER] try {name}()")
        out = None

        # 1) fn(url)
        try:
            out = fn(url)
        except Exception:
            out = None

        # 2) fn([url]) if empty/None
        if (out is None) or (isinstance(out, str) and not out.strip()):
            try:
                out = fn([url])
                print(f"[SCRAPER] {name} accepted [url] list input")
            except Exception:
                pass

        # 3) kw url=
        if (out is None) or (isinstance(out, str) and not out.strip()):
            try:
                out = fn(url=url)
                print(f"[SCRAPER] {name} accepted kw url=")
            except Exception:
                pass

        # 4) kw urls=
        if (out is None) or (isinstance(out, str) and not out.strip()):
            try:
                out = fn(urls=[url])
                print(f"[SCRAPER] {name} accepted kw urls=")
            except Exception:
                pass

        text = _normalize_scraper_output(out).strip()
        if text:
            if re.search(r"<[^>]+>", text):
                text = _strip_html_to_text(text).strip()
            if text:
                print(f"[SCRAPER] success via {name}, len={len(text)}")
                return text
    print("[SCRAPER] all candidates returned empty.")
    return ""

def _fetch_and_classify(url: str, *, title_hint: str = "") -> str:
    """
    Fallback when scraper provides nothing:
      - GET URL
      - HTML -> text
      - classify_and_extract_article to extract body (EN or source lang)
    Returns plain text article (no translation).
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 ChronoroFetcher/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8,ja;q=0.7",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        html_text = resp.text or ""
    except Exception:
        return ""
    title = title_hint or _extract_title_from_html(html_text)
    plain = _strip_html_to_text(html_text)
    if not plain:
        return ""
    client = _build_gemini_client(_get_project_id())
    body = classify_and_extract_article(plain, title_text=title, client=client)
    print(f"[RECOVER][FETCH] url len_in={len(plain)} -> body_len={len(body)} (title_hint={bool(title)})")
    return body or ""


def build_master_dicts(master_csv: str) -> Dict[str, Dict[str, str]]:
    mdf = pd.read_csv(master_csv)
    mdf["host"] = mdf["URL"].astype(str).map(extract_hostname)
    off = dict(zip(mdf["host"], mdf["オフィシャル度"]))
    country = dict(zip(mdf["host"], mdf.get("国", None)))
    source = dict(zip(mdf["host"], mdf.get("資料源", None)))
    return {"official": off, "country": country, "source": source}


def _lookup(d: Dict[str, Any], key: Any):
    if not isinstance(key, str):
        return None
    return d.get(key.lower(), None)


def extract_fields(item: dict) -> dict:
    es = item.get("ES_MESSAGE_ID_0", {}) or {}
    sender = es.get("senderProfile", {}) or {}

    media_list = es.get("mediaList", []) or []
    media_type = None
    if isinstance(media_list, list) and len(media_list) > 0 and isinstance(media_list[0], dict):
        media_type = media_list[0].get("type")

    workflow = es.get("workflowProperties", {}) or {}
    sentiment = workflow.get("sentiment")

    return {
        "snType": es.get("snType"),
        "snMsgId": es.get("snMsgId"),
        "messageType": es.get("messageType"),
        "universalMessageId": es.get("universalMessageId"),
        "permalink": es.get("permalink") or item.get("PERMALINK_4"),
        "WEB_TITLE_ES_MESSAGE_ID_3": item.get("WEB_TITLE_ES_MESSAGE_ID_3"),
        "message": es.get("message"),
        "screenName": sender.get("screenName"),
        "publisherName": sender.get("publisherName"),
        "COUNTRY_5": item.get("COUNTRY_5"),
        "snCreatedTime": es.get("snCreatedTime"),
        "snModifiedTime": es.get("snModifiedTime"),
        "type": media_type,
        "sentiment": sentiment,
    }


def jst_label_from_sn_created(series: pd.Series) -> pd.Series:
    _dt = pd.to_datetime(series, errors="coerce", utc=True)
    try:
        _dt = _dt.dt.tz_convert("Asia/Tokyo")
    except Exception:
        pass
    m = _dt.dt.month.astype("Int64")
    d = _dt.dt.day.astype("Int64")
    lbl = m.astype(str).str.replace("<NA>", "").str.cat(
        d.astype(str).str.replace("<NA>", ""), sep="月", na_rep=""
    ).replace(r"^$", pd.NA, regex=True).where(_dt.notna(), other=pd.NA)
    lbl.loc[lbl.notna()] = lbl.loc[lbl.notna()].astype(str) + "日"
    return lbl

# ---- GCS/Slack helpers ----
def _gcs_to_download_url(gs_uri: str) -> str:
    bkt, key = _parse_gs(gs_uri)
    return f"https://storage.cloud.google.com/{bkt}/{key}"


def _post_slack_blocks(webhook_url: str, blocks: list[dict]) -> None:
    headers = {"Content-Type": "application/json; charset=utf-8"}
    payload = {"blocks": blocks}
    resp = requests.post(webhook_url, data=json.dumps(payload), headers=headers, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(f"Slack webhook failed: {resp.status_code} {resp.text}")


# Simple Slack text posting helper
def _post_slack(webhook_url: str, text: str) -> None:
    headers = {"Content-Type": "application/json; charset=utf-8"}
    payload = {"text": text}
    resp = requests.post(webhook_url, data=json.dumps(payload), headers=headers, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(f"Slack webhook failed: {resp.status_code} {resp.text}")


# ===== メインパイプライン =====
def run(start: Optional[str], end: Optional[str]) -> None:
    whoami()

    # ---- Resolve inputs (GCS/local/https) ----
    payload_path_effective = _resolve_input_to_local(PAYLOAD_PATH, kind="payload")
    master_csv_effective = _resolve_input_to_local(MASTER_MEDIA_CSV, kind="master")

    # ---- Fetch from Sprinklr ----
    data_from_sprinklr = api.fetch_rows_simple(
        payload_path=payload_path_effective,
    )

    # --- Diagnostics: Sprinklr fetch totals ---
    try:
        total_fetched = len(data_from_sprinklr) if isinstance(data_from_sprinklr, (list, tuple)) else -1
    except Exception:
        total_fetched = -1
    print(f"[DIAG] Sprinklr fetched records: {total_fetched}")
    if total_fetched == -1:
        print(f"[DIAG] data_from_sprinklr type={type(data_from_sprinklr)} (expected list-like)")

    # Peek first record keys to verify expected shape (non-fatal)
    try:
        if isinstance(data_from_sprinklr, (list, tuple)) and data_from_sprinklr:
            sample_keys = sorted(list((data_from_sprinklr[0] or {}).keys()))[:20]
            print(f"[DIAG] Sample top-level keys in first record: {sample_keys}")
    except Exception as e:
        print(f"[DIAG] Could not inspect first record keys: {e}")

    # ---- Records -> DataFrame ----
    records = [extract_fields(x) for x in data_from_sprinklr]
    df = pd.DataFrame(records)

    print(f"[DIAG] DataFrame constructed: shape={df.shape} (rows={len(df)})")

    # Pre-filter diagnostics: count nulls and unique publishers
    try:
        null_pub = int(df["publisherName"].isna().sum()) if "publisherName" in df.columns else -1
        uniq_pub = int(df["publisherName"].nunique(dropna=True)) if "publisherName" in df.columns else -1
        print(f"[DIAG] publisherName: unique={uniq_pub}, nulls={null_pub}")
    except Exception as e:
        print(f"[DIAG] publisherName diagnostics failed: {e}")

    # Save raw (pre-filter) diagnostics CSV locally (one per task shard later)
    try:
        Path("/tmp/outputs/diag").mkdir(parents=True, exist_ok=True)
        _task_index, _task_count = _get_task_index_count()
        prefilter_diag_local = f"/tmp/outputs/diag/raw_prefilter_task-{_task_index}.csv"
        diag_cols = [
            "snType","snMsgId","messageType","universalMessageId",
            "permalink","WEB_TITLE_ES_MESSAGE_ID_3","publisherName","COUNTRY_5",
            "snCreatedTime","snModifiedTime","type","sentiment"
        ]
        df_diag = df.reindex(columns=[c for c in diag_cols if c in df.columns])
        df_diag.to_csv(prefilter_diag_local, index=False, encoding="utf-8-sig")
        print(f"[DIAG] Saved pre-filter diagnostic CSV: {prefilter_diag_local} (rows={len(df_diag)})")
    except Exception as e:
        print(f"[DIAG] Failed to write pre-filter diagnostic CSV: {e}")

    # ---- Master dictionaries ----
    master_dicts = build_master_dicts(master_csv_effective)
    df["オフィシャル度"] = df["publisherName"].map(lambda x: _lookup(master_dicts["official"], x))
    df["国"]       = df["publisherName"].map(lambda x: _lookup(master_dicts["country"], x))
    df["資料源"]   = df["publisherName"].map(lambda x: _lookup(master_dicts["source"],  x))

    # ---- Filter out rows without official degree (with diagnostics) ----
    before_filter = len(df)
    missing_mask = df["オフィシャル度"].isna()
    missing_count = int(missing_mask.sum())
    print(f"[DIAG] Rows before filter: {before_filter}, missing official degree: {missing_count}")

    # Show top 20 publishers that failed to match master (help find mapping issues)
    try:
        if missing_count > 0 and "publisherName" in df.columns:
            top_missing = (
                df.loc[missing_mask, "publisherName"]
                  .astype(str)
                  .str.lower()
                  .value_counts()
                  .head(20)
            )
            print("[DIAG] Top missing publisherName (up to 20):")
            for name, cnt in top_missing.items():
                print(f"  - {name}: {cnt}")
    except Exception as e:
        print(f"[DIAG] Failed to compute top missing publishers: {e}")

    df = df.loc[~missing_mask].reset_index(drop=True)
    after_filter = len(df)
    print(f"[DIAG] Rows after official-degree filter: {after_filter} (dropped {before_filter - after_filter})")
    # Precompute output paths for this run so coordinator can merge even with 0 rows
    run_paths = _build_output_paths(OUTPUT_BASE_PREFIX, RUN_FOLDER)
    if run_paths:
        print(f"[OUTPUT] base_dir={run_paths['base_dir']} parts_dir={run_paths['parts_dir']} final={run_paths['final_csv']}")

        # Upload pre-filter diagnostic (if present) to the run folder in GCS
        try:
            _task_index, _ = _get_task_index_count()
            diag_local = f"/tmp/outputs/diag/raw_prefilter_task-{_task_index}.csv"
            if os.path.exists(diag_local):
                diag_gcs = f"{run_paths['base_dir']}diag/raw_prefilter_task-{_task_index}.csv"
                _upload_file_to_gs(diag_local, diag_gcs, content_type="text/csv; charset=utf-8")
                print(f"[DIAG] Uploaded pre-filter diagnostic CSV: {diag_gcs}")
        except Exception as e:
            print(f"[DIAG] Failed to upload diagnostic CSV: {e}")

    # ---- Shard rows across Cloud Run tasks (after filtering) ----
    task_index, task_count = _get_task_index_count()
    total = len(df)
    start_i = (total * task_index) // task_count
    end_i = (total * (task_index + 1)) // task_count
    df = df.iloc[start_i:end_i].reset_index(drop=True)
    print(f"[SHARD] task {task_index}/{task_count} -> rows {start_i}:{end_i} (local {len(df)})")
    # Coordinator logs the global expectation for merge visibility
    if task_index == 0:
        print(f"[DIAG] Coordinator expects up to {task_count} shard files; zero-row tasks will not upload shards.")
    has_rows = len(df) > 0
    if not has_rows:
        if task_index != 0:
            print("[SHARD] No rows for this non-coordinator task; exiting early.")
            return
        else:
            print("[SHARD] Coordinator has 0 rows; will still proceed to merge after other tasks finish.")

    if has_rows:
        # ---- Phase 1: Gemini classify (body extraction) in parallel ----
        client = _build_gemini_client(_get_project_id())
        workers_classify = int(os.getenv("WORKERS_CLASSIFY", "16"))  # [ENV]
        def _do_classify(row):
            return classify_and_extract_article(
                row.get("message", ""),
                title_text=row.get("WEB_TITLE_ES_MESSAGE_ID_3", ""),
                client=client,
            )

        # Track where each article content came from: "sprinklr" or "scraper"
        article_sources: list[str] = [""] * len(df)
        results_article: list[str] = [""] * len(df)
        with ThreadPoolExecutor(max_workers=workers_classify) as ex:
            futures = {ex.submit(_do_classify, r._asdict()): i for i, r in enumerate(df.itertuples(index=False))}
            for fut in as_completed(futures):
                i = futures[fut]
                try:
                    results_article[i] = fut.result() or ""
                except Exception as e:
                    print(f"[WARN] classify failed on row {i}: {e}")
                    results_article[i] = ""

        # Mark sources for rows that already have content from Sprinklr classification
        for i, a in enumerate(results_article):
            if isinstance(a, str) and a.strip():
                article_sources[i] = "sprinklr"

        # ---- Phase 1.5: Recovery for empty articles using scraper.py, then API classify fallback ----
        empty_idxs = [i for i, a in enumerate(results_article) if not (isinstance(a, str) and a.strip())]
        if empty_idxs:
            print(f"[RECOVER] Trying scraper for {len(empty_idxs)} empty articles...")
            scraper_mod = _try_import_scraper()
            print(f"[RECOVER] scraper available? {bool(scraper_mod)}")
            for i in empty_idxs:
                url = df.at[i, "permalink"] if "permalink" in df.columns else ""
                title_hint = df.at[i, "WEB_TITLE_ES_MESSAGE_ID_3"] if "WEB_TITLE_ES_MESSAGE_ID_3" in df.columns else ""
                # 1) prefer existing scraper.py
                txt = _scrape_article_via_scraper(url)
                if isinstance(txt, str):
                    txt = txt.strip()
                if txt:
                    # Pre-segment then classify scraped content; if classification is empty, leave empty (no fallback to raw)
                    try:
                        pre = _presegment_text(txt)
                        cls_txt = classify_and_extract_article(pre, title_text=title_hint, client=client)
                    except Exception as e:
                        print(f"[WARN] classify(scraper) failed on row {i}: {e}")
                        cls_txt = ""
                    cls_txt = (cls_txt or "").strip()
                    if cls_txt:
                        results_article[i] = cls_txt
                        article_sources[i] = "scraper"
                        print(f"[RECOVER] row={i} source=scraper lines_in={txt.count('\n')} lines_pre={pre.count('\n')} classified_len={len(cls_txt)} url={url}")
                    else:
                        results_article[i] = ""
                        print(f"[RECOVER] row={i} source=scraper classification_empty -> leaving article empty url={url}")
                    continue
                # 2) fallback to lightweight fetch+classify (treated as sprinklr-origin for labeling)
                txt = _fetch_and_classify(url, title_hint=title_hint)
                if isinstance(txt, str):
                    txt = txt.strip()
                if txt:
                    results_article[i] = txt
                    if not article_sources[i]:
                        article_sources[i] = "sprinklr"
                    print(f"[RECOVER] row={i} source=fetch+classify len={len(txt)} url={url}")
                else:
                    results_article[i] = ""
                    print(f"[RECOVER] row={i} source=none (empty) url={url}")

        df["article"] = results_article
        df["article_source"] = article_sources

        # ---- Phase 2: Japanese translation (article & title) in parallel ----
        proj = _get_project_id() or PROJECT_FALLBACK
        workers_translate = int(os.getenv("WORKERS_TRANSLATE", "24"))  # [ENV]

        def _trans_article(t: str) -> str:
            return _translate_to_ja_with_gemini(t or "", proj)

        def _trans_title(t: str) -> str:
            return _translate_to_ja_with_gemini(t or "", proj)

        translated_article: list[str] = [""] * len(df)
        translated_title: list[str] = [""] * len(df)

        with ThreadPoolExecutor(max_workers=workers_translate) as ex:
            futs_art = {ex.submit(_trans_article, t): i for i, t in enumerate(df["article"].tolist())}
            futs_ttl = {ex.submit(_trans_title, t): i for i, t in enumerate(df["WEB_TITLE_ES_MESSAGE_ID_3"].tolist())}
            for fut in as_completed({**futs_art, **futs_ttl}):
                # Determine which map it belongs to
                if fut in futs_art:
                    i = futs_art[fut]
                    try:
                        translated_article[i] = fut.result() or ""
                    except Exception as e:
                        print(f"[WARN] translate(article) failed on row {i}: {e}")
                        translated_article[i] = ""
                else:
                    i = futs_ttl[fut]
                    try:
                        translated_title[i] = fut.result() or ""
                    except Exception as e:
                        print(f"[WARN] translate(title) failed on row {i}: {e}")
                        translated_title[i] = ""

        # Fix column name (remove stray bracket)
        df["article_jp"] = translated_article

        df["タイトル（日本語）"] = translated_title

        # ---- Write shard output (phase12 result) ----
        # run_paths was computed earlier
        Path("/tmp/outputs/parts").mkdir(parents=True, exist_ok=True)
        shard_name = f"articles_sprinklr.part-{task_index}.csv"
        local_shard = f"/tmp/outputs/parts/{shard_name}"
        df.to_csv(local_shard, index=False, encoding="utf-8-sig")
        print(f"[OK] Phase12 shard saved locally: {local_shard} ({len(df)} rows)")
        shard_gs = ""
        parts_prefix = ""
        if run_paths:
            parts_prefix = run_paths["parts_dir"]
            shard_gs = f"{parts_prefix}{shard_name}"
            _upload_file_to_gs(local_shard, shard_gs, content_type="text/csv; charset=utf-8")
            print(f"[OK] Phase12 shard uploaded: {shard_gs}")

    # === Coordinator (task 0) merges shards and finalizes ===
    if task_index != 0:
        # Non-coordinator tasks finish after uploading their phase12 shard.
        return

    if not run_paths:
        # Local-only mode: merge from local parts directory
        part_paths = sorted([str(p) for p in Path("/tmp/outputs/parts").glob("articles_sprinklr.part-*.csv")])
    else:
        # GCS mode: poll until all shards are present, then download
        expected = task_count
        prefix = run_paths["parts_dir"]
        print(f"[MERGE] Waiting for {expected} shards under {prefix}")
        deadline = time.time() + int(os.getenv("MERGE_TIMEOUT_SECS", "600"))
        while time.time() < deadline:
            found = [p for p in _gcs_list(prefix) if p.endswith(".csv")]
            if len(found) >= expected:
                break
            time.sleep(5)
        found = [p for p in _gcs_list(prefix) if p.endswith(".csv")]
        if len(found) < expected:
            print(f"[WARN] Only {len(found)}/{expected} shards found; proceeding with available shards.")
        # download all found
        part_paths = []
        for uri in sorted(found):
            local_p = f"/tmp/outputs/parts/{os.path.basename(uri)}"
            _download_gs_to_file(uri, local_p)
            part_paths.append(local_p)

    if not part_paths:
        print("[ERROR] No shards to merge; aborting finalization.")
        return

    # Concatenate shards
    dfs = []
    for pth in part_paths:
      try:
        dfs.append(pd.read_csv(pth))
      except Exception as e:
        print(f"[WARN] Failed to read shard {pth}: {e}")
    df_full = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    print(f"[MERGE] Concatenated rows: {len(df_full)}")
    if df_full.empty:
        print("[ERROR] Merged dataframe is empty; aborting.")
        return

    # ---- Date label in JST ----
    df_full["日付"] = jst_label_from_sn_created(df_full["snCreatedTime"])  # e.g., "9月10日"

    # ---- Notes column ----
    note_cols = ["publisherName", "COUNTRY_5", "type", "snMsgId"]
    base_notes = df_full[note_cols].astype(str).apply(lambda r: ",".join([x for x in r if x and x != "nan"]), axis=1)
    prefix = df_full["article_source"].map(lambda s: "[scraper]" if str(s).strip().lower() == "scraper" else "[sprinklr]")
    df_full["備考欄"] = prefix + base_notes.map(lambda s: ("," + s) if s else "")

    # ---- Ordering / Renaming ----
    df_full = df_full.reset_index(drop=True)
    df_full["NO."] = df_full.index + 1

    if "キーワード" not in df_full.columns:
        df_full["キーワード"] = ""

    ordered_cols = [
        "NO.",
        "国",              # 国（地域）
        "資料源",          # 反応主体
        "オフィシャル度",
        "日付",
        "WEB_TITLE_ES_MESSAGE_ID_3",  # タイトル（原語）
        "タイトル（日本語）",
        "permalink",      # URL
        "article",        # 本文
        "article_jp",     # 和訳
        "キーワード",
        "備考欄",
    ]

    rename_map = {
        "国": "国（地域）",
        "資料源": "反応主体",
        "WEB_TITLE_ES_MESSAGE_ID_3": "タイトル（原語）",
        "permalink": "URL",
        "article": "本文",
        "article_jp": "和訳",
    }

    for c in ordered_cols:
        if c not in df_full.columns:
            df_full[c] = ""

    df_full = df_full[ordered_cols].rename(columns=rename_map)

    # ---- Save & Upload final ----
    final_local = f"/tmp/outputs/articles_sprinklr_{RUN_ID}.csv"
    df_full.to_csv(final_local, index=False, encoding="utf-8-sig")
    print(f"[OK] CSV saved locally: {final_local} ({len(df_full)} rows)")

    final_gs = ""
    if run_paths:
        final_gs = _upload_file_to_gs(final_local, run_paths["final_csv"], content_type="text/csv; charset=utf-8")
        print(f"[OK] CSV uploaded: {final_gs}")

    # ---- Slack notify (only once from coordinator) ----
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        link = _gcs_to_download_url(final_gs) if final_gs else final_local
        run_id = RUN_ID
        total = len(df_full)
        download_url = link
        lines = [
            ":white_check_mark: sprinklr chronoro generator 完了",
            f"Run ID: `{run_id}`",
            f"URL件数: {total}",
        ]
        lines.append(f"CSV: <{download_url}|ダウンロード>")
        _post_slack(webhook, "\n".join(lines))
        print("[OK] Slack notified.")


# ===== CLIエントリポイント =====
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=False, default=None)
    parser.add_argument("--end", required=False, default=None)
    args = parser.parse_args()

    # Convert only when provided (kept for future use)
    dt_start = datetime.fromisoformat(args.start).replace(tzinfo=TZ) if args.start else None
    dt_end = datetime.fromisoformat(args.end).replace(tzinfo=TZ) if args.end else None

    # Note: If running Cloud Run Jobs with multiple tasks, consider sharding by payload here
    # using TASK_INDEX/TASK_COUNT env vars to split work across tasks when multiple payloads exist.
    run(args.start, args.end)