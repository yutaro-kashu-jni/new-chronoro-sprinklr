import re, sys, json, time, requests, bs4, tldextract
from typing import Callable, Optional, Tuple, List
 # 必要な型やライブラリのインポート
import datetime
from dataclasses import dataclass
from dateparser import parse as dparse
from dateutil import parser as dtparser
from dateparser.search import search_dates
 # 日付処理やHTML解析用の外部ライブラリをインポート
import trafilatura
from readability import Document as RDoc
from newspaper import Article
from urllib.parse import urlparse, urljoin
from pathlib import Path
 # HTMLパースやURL操作のためのライブラリをインポート
import html
import atexit

"""
Refactored scraper with a plug‑in style extractor registry.

 # このスクリプトの目的や拡張方法についての説明（英語）

Goals:
  • Keep existing extractor logic intact, but make it easy to add/override per‑domain behavior.
  • Provide a single registration point via `register_extractor(...)` (or `@domain_extractor(...)`).
  • Choose the most specific extractor automatically by (priority, path_prefix length).
  • CLI improvement: allow either a URL file OR direct URLs as arguments.

How to add a new domain extractor in the future:
  1) Implement a function `def extract_example(html: str) -> tuple[str|None, datetime|None]: ...`.
  2) Register it below with either:
        register_extractor(name="example", domain="example.com", func=extract_example)
     or for a path‑specific handler:
        register_extractor(name="example:video", domain="example.com", path_prefix="/video/", func=extract_example, priority=80)
  3) No other code changes needed.
"""

# ── Load .env for local development (optional) ───────────────────────────────
 # .envファイルを読み込んで環境変数を設定（ローカル開発用）
import os
from pathlib import Path

try:
    from dotenv import load_dotenv  # pip install python-dotenv
    # 既存の環境変数を優先しつつ .env / .env.local を順に読み込む
    for _p in (Path(".env"), Path(".env.local")):
        if _p.exists():
            load_dotenv(dotenv_path=_p, override=False)
except Exception:
    # フォールバック: KEY=VALUE または KEY: VALUE に対応（クォート除去）
    for _p in (Path(".env"), Path(".env.local")):
        try:
            if _p.exists():
                for _line in _p.read_text(encoding="utf-8").splitlines():
                    _s = _line.strip()
                    if not _s or _s.startswith("#"):
                        continue
                    sep = "=" if "=" in _s else (":" if ":" in _s else None)
                    if not sep:
                        continue
                    _k, _v = _s.split(sep, 1)
                    os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))
        except Exception:
            pass

# ================================
# Browser/HTTP and global settings
# ================================
 # HTTPリクエストやグローバル設定の定義
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr,fr-FR;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
TIMEOUT_SEC = 15

MIN_LENGTH  = 100


# ---- RSS disabled: stub for compatibility ----
 # RSS機能は無効化されており、常にNoneを返すダミー関数
def _try_rss_for_url(url: str) -> dict | None:
    # RSS取得処理（現在は常にNoneを返すダミー関数）
    """RSS step removed; always return None."""
    return None


# ---- Step logging + run metadata -------------------------------------------
 # スクレイピング処理の各ステップを記録するためのロギング機能

RUN_ID = os.environ.get("RUN_ID") or time.strftime("%Y%m%d-%H%M%S")  # [ENV]
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")  # [ENV]
STEP_LOG: list[dict] = []
_LAST_FETCH_WAS_BROWSER = False  # Playwright経由かどうかの判定に利用

# ログや実行情報の初期化・管理用変数

def _log_step(url: str, step: str, status: str, reason: str | None = None,
              content_len: int | None = None, published: str | None = None,
              extra: dict | None = None) -> None:
    # スクレイピング処理の各ステップを記録する関数
    try:
        rec = {
            "run_id": RUN_ID,
            "url": url,
            "step": step,
            "status": status,
            "reason": reason or "",
            "content_len": int(content_len) if content_len is not None else None,
            "published": published or None,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        if extra:
            rec.update({k: v for k, v in extra.items() if k not in rec})
        STEP_LOG.append(rec)
    except Exception as _e:
        print("[STEPLOG] append failed:", _e)

def _ensure_bq_table(client, dataset: str, table: str) -> None:
    # BigQueryのテーブルが存在しなければ作成する補助関数
 # BigQueryのテーブルが存在しなければ作成する補助関数
    try:
        from google.cloud import bigquery  # type: ignore
    except Exception:
        return
    try:
        dataset_ref = bigquery.Dataset(f"{client.project}.{dataset}")
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            client.create_dataset(dataset_ref, exists_ok=True)
        table_id = f"{client.project}.{dataset}.{table}"
        schema = [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("step", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("reason", "STRING"),
            bigquery.SchemaField("content_len", "INTEGER"),
            bigquery.SchemaField("published", "TIMESTAMP"),
            bigquery.SchemaField("ts", "TIMESTAMP"),
            bigquery.SchemaField("http_status", "INTEGER"),
            bigquery.SchemaField("bytes_in", "INTEGER"),
            bigquery.SchemaField("parser_version", "STRING"),
            bigquery.SchemaField("duplicate_of", "STRING"),
            bigquery.SchemaField("via", "STRING"),
        ]
        table_obj = bigquery.Table(table_id, schema=schema)
        client.create_table(table_obj, exists_ok=True)
    except Exception as e:
        print("[BQ] ensure table skipped:", e)


# ---- Playwright browser pool (optional) ------------------------------------
 # Playwrightによるブラウザプールの管理（オプション機能）
_BROWSER = None
_PLAYWRIGHT = None
_BROWSER_USES = 0
BROWSER_POOL = os.environ.get("BROWSER_POOL", "1") == "1"  # [ENV]
BROWSER_ROTATE_EVERY = int(os.environ.get("BROWSER_ROTATE_EVERY", "80"))  # [ENV]
SAVE_RENDERED_HTML_ALL = os.environ.get("SAVE_RENDERED_HTML_ALL", "0") == "1"  # [ENV]

# Playwrightブラウザプールの設定値

def _ensure_browser():
    # Playwrightブラウザの起動・再利用を管理する関数
 # Playwrightブラウザの起動・再利用を管理する関数
    global _PLAYWRIGHT, _BROWSER, _BROWSER_USES
    if not BROWSER_POOL:
        return None
    if _PLAYWRIGHT is None:
        try:
            from playwright.sync_api import sync_playwright
            _PLAYWRIGHT = sync_playwright().start()
        except Exception as e:
            print("[BROWSER-POOL] init failed:", e)
            _PLAYWRIGHT = None
            return None
    if _BROWSER is None and _PLAYWRIGHT:
        try:
            _BROWSER = _PLAYWRIGHT.chromium.launch(headless=True)
            _BROWSER_USES = 0
        except Exception as e:
            print("[BROWSER-POOL] launch failed:", e)
            _BROWSER = None
    return _BROWSER

def _browser_used():
    # ブラウザの利用回数をカウントし、一定回数ごとに再起動する関数
 # ブラウザの利用回数をカウントし、一定回数ごとに再起動する関数
    global _BROWSER_USES, _BROWSER
    if not BROWSER_POOL or _BROWSER is None:
        return
    _BROWSER_USES += 1
    try:
        if _BROWSER_USES >= BROWSER_ROTATE_EVERY:
            print("[BROWSER-POOL] rotate browser")
            _BROWSER.close()
            _BROWSER = None
            _BROWSER_USES = 0
    except Exception:
        _BROWSER = None
        _BROWSER_USES = 0

def _shutdown_browser():
    # プロセス終了時にブラウザとPlaywrightを安全にシャットダウンする関数
 # プロセス終了時にブラウザとPlaywrightを安全にシャットダウンする関数
    global _BROWSER, _PLAYWRIGHT
    try:
        if _BROWSER:
            _BROWSER.close()
    except Exception:
        pass
    try:
        if _PLAYWRIGHT:
            _PLAYWRIGHT.stop()
    except Exception:
        pass
    _BROWSER = None
    _PLAYWRIGHT = None

atexit.register(_shutdown_browser)

# プロセス終了時にブラウザを自動でシャットダウンする登録



def _maybe_bq_insert(rows: list[dict]) -> None:
    # BigQueryにステップログを挿入する関数
 # BigQueryにステップログを挿入する関数
    dataset = os.environ.get("BQ_DATASET")
    table = os.environ.get("BQ_TABLE")
    if not (dataset and table and rows):
        return
    try:
        from google.cloud import bigquery  # type: ignore
        client = bigquery.Client()
        _ensure_bq_table(client, dataset, table)
        table_ref = f"{client.project}.{dataset}.{table}"
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            print("[BQ] insert errors:", errors)
        else:
            print(f"[BQ] inserted {len(rows)} rows to {table_ref}")
    except Exception as e:
        print("[BQ] skipped (no client or error):", e)

def _flush_step_logs():
    # ステップログをファイルに書き出し、BigQueryにも送信する関数
 # ステップログをファイルに書き出し、BigQueryにも送信する関数
    try:
        from pathlib import Path as _P
        if STEP_LOG:
            _P(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
            import csv
            out_path = _P(OUTPUT_DIR) / f"steps_{RUN_ID}.csv"
            fieldnames = sorted({k for rec in STEP_LOG for k in rec.keys()})
            with out_path.open("w", encoding="utf-8", newline="") as fp:
                w = csv.DictWriter(fp, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(STEP_LOG)
            print(f"[STEPLOG] wrote {len(STEP_LOG)} rows → {out_path}")
            _maybe_bq_insert(STEP_LOG)
    except Exception as e:
        print("[STEPLOG] flush failed:", e)

#atexit.register(_flush_step_logs)

# =====================
# Extractor registration
# =====================
 # 各ドメインごとの抽出関数を登録・管理する仕組み
@dataclass
class Extractor:
    # 抽出器の情報を保持するデータクラス
    name: str
    domain: str                 # e.g. "dvidshub.net" (no www)
    func: Callable[[str], Tuple[Optional[str], Optional[datetime.datetime]]]
    path_prefix: Optional[str] = None   # e.g. "/image/"
    path_regex: Optional[re.Pattern] = None
    no_fallback: bool = False           # if True, skip generic fallback extractor
    priority: int = 50                  # higher first (use 80+ for path‑specific)

REGISTRY: List[Extractor] = []

# 抽出器の登録リスト

def register_extractor(*, name: str, domain: str, func: Callable[[str], Tuple[Optional[str], Optional[datetime.datetime]]],
                       path_prefix: Optional[str] = None, path_regex: Optional[re.Pattern] = None,
                       no_fallback: bool = False, priority: int = 50) -> None:
    # 抽出器をレジストリに登録する関数
 # 抽出器をレジストリに登録する関数
    REGISTRY.append(Extractor(name=name, domain=domain, func=func,
                              path_prefix=path_prefix, path_regex=path_regex,
                              no_fallback=no_fallback, priority=priority))

def find_extractor(domain: str, path: str) -> Optional[Extractor]:
    # ドメインとパスに基づいて最適な抽出器を選択する関数
 # ドメインとパスに基づいて最適な抽出器を選択する関数
    candidates: List[Extractor] = []
    for ext in REGISTRY:
        if ext.domain != domain:
            continue
        ok = True
        if ext.path_prefix and not path.startswith(ext.path_prefix):
            ok = False
        if ok and ext.path_regex and not ext.path_regex.search(path):
            ok = False
        if ok:
            candidates.append(ext)
    # Sort by priority desc, then by length of path_prefix desc (more specific first)
    candidates.sort(key=lambda e: (e.priority, len(e.path_prefix or "")), reverse=True)
    return candidates[0] if candidates else None


# DVIDS image extractor
 # DVIDSの画像ページからキャプションと日付を抽出する関数
def extract_dvids_image(html_str: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # DVIDSの画像ページからキャプションと日付を抽出する関数
    """Return the photo caption and its date for DVIDS /image/ pages."""
    soup = bs4.BeautifulSoup(html_str, "lxml")
    og = soup.find("meta", property="og:description")
    caption = og["content"].strip() if og and og.get("content") else None
    if not caption:
        cap_tag = soup.select_one("div.caption, div#caption, div.image-details")
        caption = cap_tag.get_text(" ", strip=True) if cap_tag else None
    raw = None
    for txt in soup.stripped_strings:
        if txt.startswith(("Date Posted:", "Date Taken:")):
            m = re.search(r"\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2})?", txt)
            if m:
                raw = m.group().replace(".", "-")
                break
    pub_dt = _parse_date(raw) or _find_pub_date(html_str, soup)
    return caption, pub_dt

# DVIDS video extractor
 # DVIDSの動画ページからキャプションと日付を抽出する関数
def extract_dvids_video(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # DVIDSの動画ページからキャプションと日付を抽出する関数
    soup = bs4.BeautifulSoup(html, "lxml")

    # 1. 全体テキストを取得
    full_text = soup.get_text(" ", strip=True)

    # 2. キャプションっぽい部分を抽出
    match = re.search(
        r"(U\.S\..+?)(?:\s*\(U\.S\. .*?video by .+?\)|U\.S\. .*?video by .+?\))",
        full_text
    )
    desc = match.group(0) if match else None

    # 3. 日付取得（従来通り）
    raw = None
    for txt in soup.stripped_strings:
        if txt.startswith("Date Posted:"):
            if m := re.search(r"\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2})?", txt):
                raw = m.group().replace(".", "-")
                break

    pub_dt = _parse_date(raw) or _find_pub_date(html, soup)
    return desc, pub_dt

 # defense.gov Multimedia Photos (/Multimedia/Photos/igphoto/...) extractor
 # defense.govの写真ページ専用の抽出関数
 # Site-specific and side-effect free (only used for the igphoto path)
def extract_defense_igphoto(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # defense.govの写真ページ専用の抽出関数
    soup = bs4.BeautifulSoup(html, "lxml")

    # Prefer meta descriptions first (usually the clean caption)
    text = None
    og = soup.find("meta", attrs={"property": "og:description"})
    if og and og.get("content"):
        text = og["content"].strip()
    if not text:
        tw = soup.find("meta", attrs={"name": "twitter:description"})
        if tw and tw.get("content"):
            text = tw["content"].strip()

    # DOM caption fallbacks (prefer <p> inside figcaption/caption blocks)
    if not text:
        # 1) direct <p> inside caption containers
        psel = soup.select(
            "figure figcaption p, .image__caption p, .image-caption p, .photo__caption p, .photo-caption p, .figure__caption p, .caption p"
        )
        for p in psel:
            s = p.get_text(" ", strip=True)
            if s and len(s) > 20:
                text = s
                break

    if not text:
        # 2) caption containers themselves (then try to pick first <p>)
        cap = (
            soup.select_one("figure figcaption")
            or soup.select_one(".image__caption, .image-caption, .photo__caption, .photo-caption, .figure__caption, .caption")
        )
        if cap:
            p = cap.find("p")
            text = p.get_text(" ", strip=True) if p else cap.get_text(" ", strip=True)

    if not text:
        # 3) fall back to first substantial <p> within article/main
        for sel in ["article p", "main p"]:
            for p in soup.select(sel):
                s = p.get_text(" ", strip=True)
                if s and len(s) > 40:
                    text = s
                    break
            if text:
                break

    # Cleanup specific to defense.gov photo pages
    def _cleanup(s: str) -> str:
        s = s.replace("\u00a0", " ")
        # Cut everything after SHARE: (case/spacing insensitive)
        s = re.split(r"share\s*:\s*", s, maxsplit=1, flags=re.I)[0].strip()
        # Remove trailing download/footer tokens if they slipped in
        s = re.sub(r"\b(?:Download|Downloads?)\s*:\s*.*$", "", s, flags=re.I)
        s = re.sub(r"\bFull\s*Size\b\s*\([^)]*\)", "", s, flags=re.I)
        s = re.sub(r"\s*\|\s*$", "", s)  # trailing pipes
        return s.strip()

    if text:
        text = _cleanup(text)

    # Still nothing? Fall back to the visible text then clean
    if not text:
        body_txt = soup.get_text("\n", strip=True)
        if body_txt:
            text = _cleanup(body_txt)

    pub_dt = _find_pub_date(html, soup)
    return (text or None), pub_dt

 # DVIDS news extractor
 # DVIDSのニュースページから本文と日付を抽出する関数
def extract_dvids_news(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # DVIDSのニュースページから本文と日付を抽出する関数
    """Return the story body (before 'NEWS INFO') and date for DVIDS /news/ pages."""
    soup = bs4.BeautifulSoup(html, "lxml")

    # Remove chrome so we don't pick up REGISTER/LOGINやグローバルナビ
    _clean(soup, [
        "header", "nav", "footer", "aside", "form", "script", "style",
        ".site-header", ".site-footer", ".navbar", ".nav", ".menu",
        ".global-header", ".global-footer", ".breadcrumbs",
        "#header", "#footer", "#nav", ".topbar", ".top-nav", ".toolbar", "ul.nav"
    ])

    def _clean_to_body(raw_text: str, title_text: str) -> Optional[str]:
        # 1) NEWS INFO より後ろは切り捨て
        t = re.split(r"\bNEWS\s*INFO\b", raw_text, maxsplit=1, flags=re.I)[0].strip()
        # 2) タイトルが先頭に残っていれば除去
        if title_text and t.startswith(title_text):
            t = t[len(title_text):].lstrip(" :\n")
        # 3) よく出るノイズ
        t = t.replace("DVIDS Hub works best with JavaScript enabled", "")
        t = t.replace("see less|", " ")  # トグルUI表記を除去

        # Courtesy Photo ブロックを本文前から除去（記事本文の前置きにだけ作用）
        dash_pos = t.find(" — ")
        preface = t[:dash_pos] if dash_pos != -1 else t
        rest    = t[dash_pos:] if dash_pos != -1 else ""
        preface = re.sub(r"Courtesy Photo\b.*?(?:see less\||read more|View Image Page|$)", "", preface, flags=re.S|re.I)
        t = preface + rest

        # 可能ならデイトライン（例: "FORT BONIFACIO, Manila, Philippines — ")から開始
        m_dateline = re.search(r"[A-Z][A-Za-z .'()/-]+,\s*[A-Za-z .'()/-]+(?:,\s*[A-Za-z .'()/-]+)?\s—\s", t)
        if m_dateline:
            t = t[m_dateline.start():]

        # 以降は行単位でノイズ除去
        lines: list[str] = []
        skip_next = 0
        for ln in t.splitlines():
            s = ln.strip()
            if not s:
                continue
            if skip_next > 0:
                skip_next -= 1
                continue
            # byline ブロック（次行・次々行の氏名・所属まで落とす）
            if re.match(r"^(Story by|By)\b", s, re.I):
                skip_next = 2
                continue
            # 画面上のラベル類
            if s == title_text:
                continue
            if re.search(r"\bCourtesy Photo\b", s, re.I):
                continue
            if re.match(r"^(MANILA|[A-Z][A-Z ,\-]+)$", s):
                continue  # 全大文字のロケーション見出しなど
            if re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", s):
                continue  # 05.26.2025 のような日付行
            if re.match(r"^(Subscribe\b|✔|✗|Web Views:|Downloads:|PUBLIC DOMAIN)", s, re.I):
                continue
            if s in {"read more", "View Image Page", "50"}:
                continue
            if "works best with JavaScript enabled" in s:
                continue
            # ありがちな長大キャプション（末尾が")"で終わる）を排除。ただしデイトラインは残す
            if len(s) > 200 and "(" in s and s.endswith(")") and " — " not in s:
                continue
            lines.append(s)

        # デイトライン優先で本文開始位置を決定
        start_idx = None
        for i, s in enumerate(lines):
            if " — " in s and "," in s.split(" — ")[0]:
                start_idx = i
                break
        if start_idx is None:
            for i, s in enumerate(lines):
                if re.search(r"[.!?]$", s) or ("—" in s and len(s) > 40) or len(s) > 140:
                    start_idx = i
                    break
        if start_idx is None:
            start_idx = 0

        cleaned = "\n".join(lines[start_idx:]).strip()
        return cleaned if len(cleaned) > 120 else None

    text: Optional[str] = None

    # Strategy A: タイトル見出し(h1)を起点に抽出
    h = soup.find(["h1", "h2"], string=True)
    if h:
        container = h
        for _ in range(4):
            if container.parent and container.parent.name in ("div", "section", "article"):
                container = container.parent
            else:
                break
        raw = container.get_text("\n", strip=True)
        title = (h.get_text(" ", strip=True) or "").strip()
        text = _clean_to_body(raw, title)

    # Strategy B: よくある本文ブロックを直接参照（同じクレンジング適用）
    if not text:
        body = (
            soup.select_one("div.field-name-body")
            or soup.select_one("article .field--name-body")
            or soup.select_one("div[itemprop='articleBody']")
            or soup.select_one("article")
        )
        if body:
            raw = body.get_text("\n", strip=True)
            title2 = (h.get_text(" ", strip=True).strip() if h else "")
            text = _clean_to_body(raw, title2)

    pub_dt = _find_pub_date(html, soup)
    return (text or None), pub_dt

# DVIDS audio extractor
 # DVIDSの音声ページからトランスクリプトと公開日を抽出する関数
def extract_dvids_audio(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # DVIDSの音声ページからトランスクリプトと公開日を抽出する関数
    """
    Extract transcript/description and published date from DVIDS /audio/ pages.
    Prefer the transcript that appears directly after the /audio/embed/ iframe.
    """
    soup = bs4.BeautifulSoup(html, "lxml")

    # Remove obvious chrome to avoid pulling REGISTER/LOGIN and menus
    _clean(soup, [
        "header", "nav", "footer", "aside", "form", "script", "style",
        ".site-header", ".site-footer", ".navbar", ".nav", ".menu",
        ".global-header", ".global-footer", ".breadcrumbs",
        "#header", "#footer", "#nav", ".topbar", ".top-nav", ".toolbar", "ul.nav"
    ])

    text = None

    # Strategy A: Gather text AFTER the /audio/embed/ iframe (including NavigableString nodes)
    iframe = soup.find("iframe", src=re.compile(r"/audio/embed/"))
    if iframe:
        parts: list[str] = []

        def _gather(nodes) -> bool:
            for node in nodes:
                if isinstance(node, bs4.NavigableString):
                    chunk = str(node).strip()
                elif getattr(node, "get_text", None):
                    chunk = node.get_text("\n", strip=True)
                else:
                    continue
                if not chunk:
                    continue
                # Stop once we hit the AUDIO INFO section
                if re.search(r"\bAUDIO\s*INFO\b", chunk, re.I):
                    return True
                # Skip any residual embed help text
                if "Advanced Embed Example" in chunk or "video size:" in chunk:
                    continue
                parts.append(chunk)
            return False

        # 1) Collect siblings within the same parent after the iframe
        stop = _gather(iframe.next_siblings)
        # 2) If needed, continue with siblings of the iframe's parent (content often lives there)
        if not stop and iframe.parent:
            _gather(iframe.parent.next_siblings)

        joined = "\n".join(parts).strip()
        if joined:
            # Drop common chrome that appears right after the embed
            lines = []
            for ln in joined.splitlines():
                s = ln.strip()
                if not s:
                    continue
                if re.match(r"^(show more|UNITED STATES|Audio by|Subscribe\b|\d{2}\.\d{2}\.\d{4}|Defense Media Activity)", s, re.I):
                    continue
                lines.append(s)
            cleaned = "\n".join(lines).strip()
            # If the narrative opener exists (e.g., WELCOME ...), start from there
            m = re.search(r"\bWELCOME\b", cleaned, re.I)
            candidate = cleaned[m.start():].strip() if m else cleaned
            if len(candidate) > 80:
                text = candidate

    # Strategy B: a container headed by the page <h1>, up to "AUDIO INFO"
    if not text:
        h = soup.find(["h1", "h2"], string=True)
        if h:
            container = h
            for _ in range(3):
                if container.parent and container.parent.name in ("div", "section", "article"):
                    container = container.parent
                else:
                    break
            before_audio = re.split(r"\bAUDIO\s*INFO\b", container.get_text("\n", strip=True), maxsplit=1, flags=re.I)[0].strip()
            if before_audio and len(before_audio) > 80:
                # Drop the heading itself if it's at the very beginning
                title = (h.get_text(" ", strip=True) or "").strip()
                if before_audio.startswith(title):
                    before_audio = before_audio[len(title):].lstrip(" :\n")
                # Also drop any embed boilerplate
                before_audio = re.sub(r"^.*?Advanced Embed Example.*?(?=$)", "", before_audio, flags=re.S)
                if len(before_audio) > 80:
                    text = before_audio

    # Strategy C: explicit transcript containers
    if not text:
        candidates = soup.select(
            "div.transcript, section.transcript, div#transcript, article.transcript, "
            "[class*='transcript'], [id*='transcript']"
        )
        for cand in candidates:
            t = cand.get_text("\n", strip=True)
            if t and len(t) > 80:
                text = t
                break

    # Strategy D: typical description/body blocks on DVIDS
    if not text:
        body = (
            soup.select_one("div.field-name-body")
            or soup.select_one("div[itemprop='description']")
            or soup.select_one("div.description")
            or soup.select_one("article .description")
            or soup.select_one("article")
        )
        if body:
            t = body.get_text("\n", strip=True)
            # Keep only before "AUDIO INFO"
            t = re.split(r"\bAUDIO\s*INFO\b", t, maxsplit=1, flags=re.I)[0].strip()
            if t:
                text = t

    # Strategy E: meta descriptions
    if not text:
        og = soup.find("meta", attrs={"property": "og:description"})
        if og and og.get("content"):
            text = og["content"].strip()
    if not text:
        md = soup.find("meta", attrs={"name": "description"})
        if md and md.get("content"):
            text = md["content"].strip()
    if not text:
        # Last resort: full page text up to "AUDIO INFO"
        text = re.split(r"\bAUDIO\s*INFO\b", soup.get_text("\n", strip=True), maxsplit=1, flags=re.I)[0].strip()

    pub_dt = _find_pub_date(html, soup)
    return (text or None), pub_dt


# CRNTT (crntt.com / bj.crntt.com) extractor
 # CRNTT系サイトのタイトル抽出補助関数


def _crntt_find_title(soup: bs4.BeautifulSoup) -> Optional[str]:
    # CRNTT系サイトのタイトル抽出補助関数
    # Prefer meta title if present
    og = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "title"})
    if og and og.get("content"):
        return og["content"].strip()
    h = soup.find(["h1", "h2"])  # sometimes there is a heading
    if h:
        t = h.get_text(" ", strip=True)
        if t:
            return t
    # Fallback: first meaningful visible line before the date row
    lines = [ln.strip() for ln in soup.get_text("\n", strip=True).splitlines() if ln.strip()]
    # Drop UI tokens
    while lines and (re.search(r"〖[^〗]+〗", lines[0]) or "打印" in lines[0] or re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", lines[0]) or "CRNTT" in lines[0].upper()):
        lines.pop(0)
    return lines[0] if lines else None



# Helper for vietnam.vn title extraction
 # vietnam.vn用のタイトル抽出補助関数
def _vietnam_vn_find_title(soup: bs4.BeautifulSoup) -> Optional[str]:
    # vietnam.vn用のタイトル抽出補助関数
    """Prefer og:title, then visible <h1>, then <title>."""
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()
    h1 = soup.find("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t:
            return t
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None

# Helper for rfi.fr title extraction (language-agnostic)
 # rfi.fr用のタイトル抽出補助関数
def _rfi_find_title(soup: bs4.BeautifulSoup) -> Optional[str]:
    # rfi.fr用のタイトル抽出補助関数
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()
    h1 = soup.find("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t:
            return t
    tw = soup.find("meta", attrs={"name": "twitter:title"})
    if tw and tw.get("content"):
        return tw["content"].strip()
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None

# Helper for ct.moreover.com title extraction (structure-based, language-agnostic)
 # ct.moreover.com用のタイトル抽出補助関数
def _moreover_find_title(soup: bs4.BeautifulSoup) -> Optional[str]:
    # ct.moreover.com用のタイトル抽出補助関数
    """Prefer OpenGraph/Twitter/meta title, then visible <h1>, then <title>.
    This is DOM-structure based and language-agnostic.
    """
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()
    tw = soup.find("meta", attrs={"name": "twitter:title"})
    if tw and tw.get("content"):
        return tw["content"].strip()
    mt = soup.find("meta", attrs={"name": "title"})
    if mt and mt.get("content"):
        return mt["content"].strip()
    h1 = soup.find("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t:
            return t
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None

# Helper for idahostatesman.com title extraction (structure-based, language-agnostic)
 # idahostatesman.com用のタイトル抽出補助関数
def _idahostatesman_find_title(soup: bs4.BeautifulSoup) -> Optional[str]:
    # idahostatesman.com用のタイトル抽出補助関数
    """Prefer OpenGraph/Twitter/meta title, then visible <h1>, then <title>."""
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()
    tw = soup.find("meta", attrs={"name": "twitter:title"})
    if tw and tw.get("content"):
        return tw["content"].strip()
    mt = soup.find("meta", attrs={"name": "title"})
    if mt and mt.get("content"):
        return mt["content"].strip()
    h1 = soup.find("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t:
            return t
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None

# ===== Generic brand‑stripping & structure‑first title selection (domain‑agnostic) =====
 # 汎用的なブランド名除去と構造優先のタイトル抽出ロジック
_SEP_RE = re.compile(r"\s*(?:\||–|—|•|·|-)\s*")

def _site_name_tokens(domain: str, soup: bs4.BeautifulSoup) -> set[str]:
    # サイト名やブランド名のバリエーションをセットとして生成
 # サイト名やブランド名のバリエーションをセットとして生成
    """
    Build a set of strings that represent the site's brand to help strip it
    from <title> suffixes like `" - SiteName | Example.com"`.
    Includes variations of domain, base label, and og:site_name.
    """
    tokens: set[str] = set()
    host = domain.replace("www.", "")
    tokens.update({host, host.split(":")[0]})
    if "." in host:
        base = host.split(".")[0]
        tokens.add(base)
    # meta site name(s)
    for css in ("meta[property='og:site_name']", "meta[name='application-name']"):
        tag = soup.select_one(css)
        if tag and tag.get("content"):
            tokens.add(tag["content"].strip())

    # normalize variations (case/spacing/punctuation)
    out: set[str] = set()
    for t in tokens:
        if not t:
            continue
        for v in {
            t,
            t.lower(),
            t.title(),
            t.upper(),
            t.replace(".", " "),
            t.replace("-", " "),
            t.replace("_", " "),
        }:
            v = v.strip()
            if v:
                out.add(v)
        # add ".com" variation for short labels (common in titles)
        if "." not in t:
            out.add(f"{t}.com")
            out.add(f"{t}.net")
            out.add(f"{t}.org")
    return out

def _strip_site_brand_from_title(title: str, site_tokens: set[str]) -> str:
    # タイトル末尾からブランド名やセクション名を除去する関数
 # タイトル末尾からブランド名やセクション名を除去する関数
    """
    Remove trailing brand/section chunks from a title using common separators.
    Example:
      "Headline goes here - memesita.com - Memesita" -> "Headline goes here"
    """
    if not title:
        return title
    parts = _SEP_RE.split(title.strip())

    lowered = {t.lower() for t in site_tokens if t}
    def looks_like_brand(chunk: str) -> bool:
        tl = chunk.strip().lower()
        if not tl:
            return False
        if tl in lowered:
            return True
        if any(tl in st or st in tl for st in lowered):
            return True
        # generic section words commonly appended after separators
        if re.fullmatch(r"(news|world|opinion|politics|military(?:\s*news)?|ニュース|軍事ニュース|ゲルマンニュース)", tl):
            return True
        return False

    # Pop brand‑ish tokens from the right
    while len(parts) > 1 and looks_like_brand(parts[-1]):
        parts.pop()

    cleaned = " - ".join(parts).strip()
    cleaned = re.sub(r"\s*(?:\||–|—|•|·|-)\s*$", "", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned

def _find_title_generic(soup: bs4.BeautifulSoup, domain: str) -> Optional[str]:
    # 汎用的な構造優先・言語非依存のタイトル抽出関数
 # 汎用的な構造優先・言語非依存のタイトル抽出関数
    """
    Structure‑first, language‑agnostic title selector:
      1) visible <h1> (prefer inside article/main)
      2) og:title / twitter:title / meta[name=title]
      3) <title> (with brand suffix stripped)
    """
    def _text(el):
        return el.get_text(" ", strip=True) if el else None

    # 1) Prefer a visible heading (avoid bare site names)
    for sel in ("article h1", "main article h1", "main h1", "h1", "article h2", "h2"):
        el = soup.select_one(sel)
        t = _text(el)
        if t and len(t) >= 6:
            tokens = _site_name_tokens(domain, soup)
            if not any(t.strip().lower() == s.lower() for s in tokens):
                return t

    # 2) OG/Twitter/meta title (often clean)
    for css in ("meta[property='og:title']", "meta[name='twitter:title']", "meta[name='title']"):
        tag = soup.select_one(css)
        if tag and tag.get("content"):
            t = tag["content"].strip()
            if t:
                return _strip_site_brand_from_title(t, _site_name_tokens(domain, soup))

    # 3) <title> with brand strip
    if soup.title and soup.title.string:
        return _strip_site_brand_from_title(soup.title.string.strip(), _site_name_tokens(domain, soup))

    return None

# Helper for shobserver.com title extraction (structure-based, language-agnostic)
 # shobserver.com用のタイトル抽出補助関数
def _shobserver_find_title(soup: bs4.BeautifulSoup) -> Optional[str]:
    # shobserver.com用のタイトル抽出補助関数
    """Prefer OG/Twitter/meta title, then visible <h1>/headline blocks inside the article container.
    Avoid relying on any language tokens; use only DOM structure.
    """
    for css, attr in (
        ("meta[property='og:title']", "content"),
        ("meta[name='twitter:title']", "content"),
        ("meta[name='title']", "content"),
    ):
        tag = soup.select_one(css)
        if tag and tag.get(attr):
            t = tag.get(attr).strip()
            if t:
                return t

    # Try headline elements inside likely containers first
    containers = [
        soup.select_one("#newsDetail"),
        soup.find("article"),
        soup.select_one("main article"),
        soup.select_one("main"),
    ]
    for cont in [c for c in containers if c]:
        h1 = cont.find("h1")
        if h1:
            t = h1.get_text(" ", strip=True)
            if t:
                return t

    for el in soup.select(".title, .news-title, .article-title, [class*='headline']"):
        t = el.get_text(" ", strip=True)
        if t:
            return t

    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None

# rfi.fr article extractor (HTML-structure based, language-agnostic)
 # rfi.frの記事ページから本文と公開日を抽出する関数
def extract_rfi(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # rfi.frの記事ページから本文と公開日を抽出する関数
    """Extractor for rfi.fr articles.
    - Finds <article> (or main role container) and strips share/tags/related/comments blocks via CSS selectors
    - Gets body from [itemprop='articleBody'] or common article body classes
    - Published date comes from meta/JSON-LD/<time datetime> via existing helpers
    """
    soup = bs4.BeautifulSoup(html, "lxml")

    # Locate main article container without relying on language tokens
    article = soup.find("article")
    if not article:
        article = (
            soup.select_one("[itemtype*='Article']")
            or soup.select_one("main article")
            or soup.select_one("main")
            or soup.select_one("[role='main']")
        )
    container = article or soup

    # Remove non-article sections via CSS (language-agnostic)
    _clean(container, [
        "header", "nav", "footer", "aside", "form", "script", "style",
        ".share", ".shares", ".sharing", ".share-buttons", ".share-tools",
        "[class*='share']", ".social", ".social-share", "[class*='social']",
        ".tags", ".post-tags", ".entry-tags", "[class*='tag-']", ".meta", ".post-meta", ".entry-meta", ".byline",
        ".related", ".related-posts", ".post-related", ".article-related", ".read-more", ".nav-links", ".pagination",
        ".comments", "#comments", ".comment", ".comment-list", ".comment-form",
        ".newsletter", ".subscribe", ".subscription", ".player", ".audio-player", ".video-player",
    ])

    # Typical RFI body containers
    body = (
        container.select_one("[itemprop='articleBody']")
        or container.select_one("div[itemprop='articleBody']")
        or container.select_one("section[itemprop='articleBody']")
        or container.select_one(".article__content, .article-content, .content__body, .article-body, .t-content__body, [class*='articleBody'], [class*='article-body']")
    )

    text: Optional[str] = None
    if body:
        text = body.get_text("\n", strip=True)
    else:
        # Fallback: collect paragraphs within the container
        parts: list[str] = []
        for el in container.find_all(["p", "h2", "h3"], recursive=True):
            s = el.get_text(" ", strip=True)
            if not s:
                continue
            # Prefer paragraphs; include substantial subheads
            if el.name == "p" or len(s) > 60:
                parts.append(s)
        text = "\n".join(parts).strip() if parts else container.get_text("\n", strip=True)

    # Publication date via robust helpers, then local <time datetime>
    pub_dt = _find_pub_date(html, soup)
    if not pub_dt and container:
        t = container.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            pub_dt = _parse_date(t["datetime"])  # type: ignore[arg-type]

    return (text or None), pub_dt


 # France24 extractor (video/article pages, language-agnostic)
 # france24.comの動画・記事ページから本文と公開日を抽出する関数
def extract_france24(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # france24.comの動画・記事ページから本文と公開日を抽出する関数
    """
    Extractor for france24.com (video/article pages, language-agnostic).
    - Title: og:title → JSON-LD headline/name → <h1> → <title>
    - Content: og:description → meta[name=description] → JSON-LD description → visible description block
    - Published: <time datetime> → JSON-LD uploadDate/datePublished/dateCreated/dateModified → generic helpers
    Returns (content, pub_dt).
    """
    import html as _html  # avoid shadowing the html string param above
    soup = bs4.BeautifulSoup(html, "lxml")

    # ---- Title --------------------------------------------------------------
    title = None
    ogt = soup.find("meta", attrs={"property": "og:title"})
    if ogt and ogt.get("content"):
        title = ogt["content"].strip()

    # JSON-LD collector (local; domain-specific use only)
    def _jsonld_items_local(s: bs4.BeautifulSoup) -> list[dict]:
        items: list[dict] = []
        for tag in s.select("script[type='application/ld+json']"):
            try:
                data = json.loads(_html.unescape(tag.string or ""))
            except Exception:
                continue
            # Flatten @graph
            if isinstance(data, dict) and "@graph" in data:
                data = data["@graph"]
            if isinstance(data, dict):
                if isinstance(data.get("@type"), (list, str)):
                    items.append(data)
            elif isinstance(data, list):
                for it in data:
                    if isinstance(it, dict):
                        items.append(it)
        return items

    items = _jsonld_items_local(soup)

    if not title:
        for it in items:
            t = it.get("headline") or it.get("name")
            if t:
                title = str(t).strip()
                break

    if not title:
        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(" ", strip=True)
            if t:
                title = t
    if not title and soup.title and soup.title.string:
        title = soup.title.string.strip()

    # ---- Content (prefer full article body; fall back to description) -------
    content = None
    # A) Prefer structured article body containers (language-agnostic)
    #    France24 commonly uses an <article> with .t-content__body or similar containers.
    art = soup.find("article") or soup.select_one("main article, .t-content, [role='article']")
    body_el = None
    if art:
        body_el = (
            art.select_one(".t-content__body")
            or art.select_one(".article__content")
            or art.select_one("[class*='content__body']")
            or art.select_one("[itemprop='articleBody']")
            or art.select_one(".o-article__content")
            or art.select_one(".c-article__content")
        )
    else:
        # Fallback: search globally for typical body containers
        body_el = (
            soup.select_one("article .t-content__body")
            or soup.select_one("article [itemprop='articleBody']")
            or soup.select_one("article .article__content")
            or soup.select_one("[class*='content__body']")
        )
    if body_el:
        parts: list[str] = []
        for p in body_el.find_all("p"):
            s = p.get_text(" ", strip=True)
            if not s:
                continue
            # Skip short UI crumbs or caption-ish one-liners
            if len(s) < 25:
                continue
            parts.append(s)
        body_txt = "\n".join(parts).strip()
        if len(body_txt) > 160:  # ensure we captured a meaningful body
            content = body_txt
    # B) If no body captured, use OG/Twitter/meta descriptions
    if not content:
        ogd = soup.find("meta", attrs={"property": "og:description"})
        if ogd and ogd.get("content"):
            content = ogd["content"].strip()
    if not content:
        md = soup.find("meta", attrs={"name": "description"})
        if md and md.get("content"):
            content = md["content"].strip()
    if not content:
        for it in items:
            dsc = it.get("description")
            if dsc:
                content = str(dsc).strip()
                break
    if not content:
        # Visible description blocks that are stable across locales/templates
        body = (
            soup.select_one("[itemprop='description']")
            or soup.select_one("article .article__lead, article .o-article__lead")
            or soup.select_one(".article__summary, .t-content__body")
            or soup.select_one("article")
        )
        if body:
            # Keep concise summary-like text, not the entire transcript
            txt = body.get_text("\n", strip=True)
            # Trim obvious chrome
            txt = re.sub(r"\b(Tags?|Related|Share)\b.*$", "", txt, flags=re.I|re.S).strip()
            if txt:
                content = txt

    # ---- Published date -----------------------------------------------------
    pub_dt: Optional[datetime.datetime] = None

    # 0) data-iso-date attribute (France24 often renders this on the date element)
    iso_node = soup.select_one("[data-iso-date]")
    if iso_node and iso_node.get("data-iso-date"):
        pub_dt = _parse_date(iso_node["data-iso-date"])

    # 1) <time datetime="...">
    if not pub_dt:
        t = soup.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            pub_dt = _parse_date(t["datetime"])

    # 2) JSON-LD: prefer VideoObject.uploadDate, then datePublished/dateCreated/dateModified
    if not pub_dt:
        for it in items:
            typ = it.get("@type")
            typ_l = [typ] if isinstance(typ, str) else (typ or [])
            if any(x in ("VideoObject", "NewsArticle", "Article", "ReportageNewsArticle") for x in typ_l):
                for key in ("uploadDate", "datePublished", "dateCreated", "dateModified"):
                    raw = it.get(key)
                    if raw and (dt := _parse_date(str(raw))):
                        pub_dt = dt
                        break
            if pub_dt:
                break

    # 3) Fallback to generic helpers (meta / next-data / jsonld / text scan)
    if not pub_dt:
        pub_dt = _find_pub_date(html, soup)

    # Ensure we always return a string for content (even if short)
    return (content or None), pub_dt

# Moreover ct.moreover.com extractor (language-agnostic)
 # ct.moreover.comの集約ページから本文と公開日を抽出する関数
def extract_moreover_ct(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # ct.moreover.comの集約ページから本文と公開日を抽出する関数
    """
    Extractor for ct.moreover.com aggregator pages (language-agnostic).
    Strategy:
      - Strip obvious non-article chrome via CSS selectors (no keyword reliance).
      - Find a stable article/main/body container by DOM roles/classes.
      - Concatenate meaningful paragraphs.
      - Published datetime via generic helpers; fallback to visible <time datetime> or common ISO-ish/date patterns.
    Returns (content, pub_dt).
    """
    soup = bs4.BeautifulSoup(html, "lxml")

    # Remove global chrome (structure-based)
    _clean(soup, [
        "header", "nav", "footer", "aside", "form", "script", "style",
        "[role='banner']", "[role='navigation']", "[role='complementary']",
        ".breadcrumbs", ".breadcrumb", ".toolbar",
    ])

    # Pick a likely article container by structure (no language tokens)
    container = (
        soup.select_one("article")
        or soup.select_one("[itemprop='articleBody']")
        or soup.select_one("[role='main'] article")
        or soup.select_one("main article")
        or soup.select_one("main")
        or soup.select_one("#main, #content, .content, .article, .entry-content, [class*='article__content'], [class*='content__body']")
        or soup
    )

    # Inside container, drop side blocks commonly appended after the body (structure-based)
    _clean(container, [
        ".related, .related-posts, .post-related, .article-related",
        ".comments, #comments, .comment, .comment-list, .comment-form",
        ".tags, .post-tags, .entry-tags, .byline, .meta, .post-meta, .entry-meta",
        ".share, .share-tools, .share-buttons, [class*='share']",
        "figure, figcaption, .gallery, .lightbox, .slideshow",
        "iframe, noscript",
    ])

    # Collect meaningful paragraphs
    parts: list[str] = []
    for el in container.find_all(["p", "div"], recursive=True):
        # prefer real paragraphs or divs that look like paragraphs (no nested blocks)
        if el.name == "div" and el.find(["p", "div", "figure", "section", "article"]):
            continue
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue
        # Skip ultra short UI crumbs by length (language-agnostic)
        if len(txt) < 25:
            continue
        parts.append(txt)

    content = "\n".join(parts).strip() if parts else None
    if content and len(content) < 120:
        content = None  # force fallback if we only caught crumbs

    # Determine published datetime
    pub_dt: Optional[datetime.datetime] = _find_pub_date(html, soup)
    if not pub_dt:
        # local fallback: <time datetime> within container
        t = container.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            pub_dt = _parse_date(t["datetime"])  # type: ignore[arg-type]
    if not pub_dt:
        # final text scan for dd Mon yyyy [hh:mm] style which is common on aggregators
        m = re.search(r"\b\d{1,2}\s+[A-Z][a-z]{2}\s+\d{4}(?:\s+\d{1,2}:\d{2})?\b", soup.get_text(" ", strip=True))
        if m:
            pub_dt = _parse_date(m.group(0))

    return (content or None), pub_dt

# idahostatesman.com extractor (HTML-structure based, language-agnostic)
 # idahostatesman.comの記事ページから本文と公開日を抽出する関数
def extract_idahostatesman(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # idahostatesman.comの記事ページから本文と公開日を抽出する関数
    """
    Extractor for idahostatesman.com (McClatchy). HTML-structure based & language-agnostic.
    Strategy:
      - Trim chrome via CSS selectors only (no keyword heuristics)
      - Find <article> / main container and typical article body blocks used by McClatchy
      - Concatenate meaningful paragraphs
      - Published datetime via generic helpers; fallback to <time datetime>
    Returns (content, pub_dt).
    """
    soup = bs4.BeautifulSoup(html, "lxml")

    # Remove non-article chrome
    _clean(soup, [
        "header", "nav", "footer", "aside", "form", "script", "style",
        ".breadcrumbs", ".breadcrumb", ".toolbar",
        ".share", ".share-tools", ".share-buttons", "[class*='share']",
        ".tags", ".tag-list", ".post-tags", ".entry-tags", ".byline", ".author", ".meta", ".post-meta", ".entry-meta",
        ".related", ".related-stories", ".more-stories", ".article-related", ".recommendations",
        ".comments", "#comments", ".comment", ".comment-list", ".comment-form",
        ".newsletter", ".subscribe", ".subscription", ".paywall",
        ".inline-video", ".video-player", ".audio-player", ".gallery", ".slideshow",
        "figure", "figcaption", "iframe", "noscript",
        ".ad", ".advertisement", "[id*='ad-']", "[class*='ad-']",
    ])

    # Locate main article container (structure-only)
    article = (
        soup.find("article")
        or soup.select_one("main article")
        or soup.select_one("[role='main'] article")
        or soup.select_one("[itemtype*='Article']")
        or soup
    )

    # Typical McClatchy body containers
    body = (
        article.select_one("div.article-body")
        or article.select_one("[class*='article__body']")
        or article.select_one("[class*='story-body']")
        or article.select_one("[data-qa='article-body'], [data-qa='story-body']")
        or article.select_one("[itemprop='articleBody']")
        or article
    )

    # Collect substantial paragraphs only
    parts: list[str] = []
    for el in body.find_all(["p", "li"], recursive=True):
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue
        if len(txt) < 25:  # skip crumbs/captions
            continue
        parts.append(txt)

    content = "\n".join(parts).strip() if parts else None
    if content and len(content) < 160:
        content = None  # force fallback if body is too short

    # Published datetime
    pub_dt: Optional[datetime.datetime] = _find_pub_date(html, soup)
    if not pub_dt:
        t = article.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            pub_dt = _parse_date(t.get("datetime"))

    return (content or None), pub_dt

# shobserver.com extractor (HTML-structure based, language-agnostic)
 # shobserver.comの記事詳細ページから本文と公開日を抽出する関数
def extract_shobserver(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # shobserver.comの記事詳細ページから本文と公開日を抽出する関数
    """
    Extractor for shobserver.com article detail pages (e.g., /staticsg/res/html/web/newsDetail.html).
    Strategy:
      - Locate article container (#newsDetail, <article>, or main) using only DOM structure.
      - Remove share/tags/related/comments/ads using CSS selectors (no language keywords).
      - Collect substantial paragraphs from typical body containers (#newsContent/.news-content/etc.).
      - Published datetime via generic helpers; then <time datetime>; then time/date class text scan.
    Returns (content, pub_dt).
    """
    soup = bs4.BeautifulSoup(html, "lxml")

    # 1) Locate a likely article container
    container = (
        soup.select_one("#newsDetail")
        or soup.find("article")
        or soup.select_one("main article")
        or soup.select_one("[role='main'] article")
        or soup.select_one("main")
        or soup
    )

    # 2) Strip obvious non-article chrome within that container
    _clean(container, [
        "header", "nav", "footer", "aside", "form", "script", "style",
        ".breadcrumbs", ".breadcrumb", ".toolbar",
        ".share", ".share-tools", ".share-buttons", "[class*='share']",
        ".tags", ".tag-list", ".post-tags", ".entry-tags", ".byline", ".author", ".meta", ".post-meta", ".entry-meta",
        ".related", ".related-stories", ".more-stories", ".article-related", ".recommendations",
        ".comments", "#comments", ".comment", ".comment-list", ".comment-form",
        ".newsletter", ".subscribe", ".subscription",
        ".inline-video", ".video-player", ".audio-player", ".gallery", ".slideshow",
        "figure", "figcaption", "iframe", "noscript",
        ".ad", ".advertisement", "[id*='ad-']", "[class*='ad-']",
    ])

    # 3) Typical body containers used for articles
    body = (
        container.select_one("#newsContent, #newscontent, [id*='news-content']")
        or container.select_one(".newsContent, .news-content, .news_content")
        or container.select_one("[itemprop='articleBody']")
        or container.select_one(".article-content, .article__content, .content__article, .content__body, .detail-content, .detail__content, .content")
        or container
    )

    # 4) Collect substantial paragraphs only (language-agnostic)
    parts: list[str] = []
    for el in body.find_all(["p", "div", "li"], recursive=True):
        # Treat <div> as paragraph-like only if it isn't an obvious wrapper
        if el.name == "div" and el.find(["div", "section", "article", "figure", "ul", "ol"]):
            continue
        txt = el.get_text(" ", strip=True)
        if not txt or len(txt) < 25:
            continue
        parts.append(txt)

    content = "\n".join(parts).strip() if parts else None
    if content and len(content) < 160:
        content = None  # force fallback if we only caught crumbs

    # 5) Publication datetime
    pub_dt: Optional[datetime.datetime] = _find_pub_date(html, soup)

    if not pub_dt:
        # Prefer an explicit <time datetime>
        t = container.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            pub_dt = _parse_date(t.get("datetime"))

    if not pub_dt:
        # Look for visible time/date blocks near the header
        for sel in ("[class*='time']", "[class*='date']", "[id*='time']", "[id*='date']"):
            node = container.select_one(sel)
            if not node:
                continue
            dt_txt = node.get_text(" ", strip=True)
            if dt_txt:
                dt = _parse_date(dt_txt)
                if dt:
                    pub_dt = dt
                    break

    if not pub_dt:
        # Final fallback: scan container text for date patterns
        scope_txt = container.get_text(" ", strip=True)
        pub_dt = _find_date_from_text(scope_txt) or _search_date_in_text(scope_txt)

    return (content or None), pub_dt

# Register shobserver.com extractor
 # shobserver.com用の抽出器をレジストリに登録
register_extractor(name="shobserver", domain="shobserver.com", func=extract_shobserver)

# ipdefenseforum.com extractor (HTML-structure based, language-agnostic)
 # ipdefenseforum.comの記事ページから本文と公開日を抽出する関数
def extract_ipdefenseforum(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # ipdefenseforum.comの記事ページから本文と公開日を抽出する関数
    """
    Extractor for ipdefenseforum.com article pages (JavaScript-heavy site).
    Strategy:
      - Locate article container (article, .entry-content, .post-content) using DOM structure.
      - Remove share/tags/related/comments/ads using CSS selectors (language-agnostic).
      - Published date via generic helpers; fallback to <time datetime> or meta.
    """
    soup = bs4.BeautifulSoup(html, "lxml")
    
    # Remove chrome/navigation/sidebar elements
    _clean(soup, [
        "nav", "header", "footer", ".navigation", ".nav", ".menu", ".sidebar", ".aside",
        ".share", ".share-tools", ".share-buttons", "[class*='share']",
        ".social", ".social-share", "[class*='social']", ".follow", "[class*='follow']",
        ".tags", ".post-tags", ".entry-tags", "[class*='tag-']", ".meta", ".post-meta", ".entry-meta", ".byline",
        ".related", ".related-posts", "[class*='related']", ".more-stories", ".recommended",
        ".comments", ".comment-form", "[class*='comment']", ".reply",
        ".ad", ".advertisement", "[id*='ad-']", "[class*='ad-']",
        "script", "style", "noscript", ".hidden"
    ])
    
    # Find main content container
    article = (
        soup.find("article")
        or soup.select_one(".entry-content")
        or soup.select_one(".post-content") 
        or soup.select_one("[class*='content']")
        or soup.select_one(".story-body")
        or soup.select_one("main")
        or soup.select_one("#main")
        or soup.select_one("#content")
    )
    
    if not article:
        # Fallback: use body and try to find the largest text block
        article = soup.body or soup
    
    # Extract text content
    content = None
    if article:
        # Get all paragraphs and text blocks
        paragraphs = article.find_all(["p", "div"], string=True)
        if paragraphs:
            # Join paragraph texts
            texts = []
            for p in paragraphs:
                text = p.get_text(" ", strip=True)
                if len(text) > 50:  # Only include substantial paragraphs
                    texts.append(text)
            content = "\n\n".join(texts)
        else:
            # Fallback: get all text
            content = article.get_text("\n", strip=True)
    
    # Clean up content
    if content:
        # Remove common noise patterns
        content = re.sub(r"\n{3,}", "\n\n", content)  # Reduce excessive newlines
        content = content.strip()
        
        # Remove if too short
        if len(content) < MIN_LENGTH:
            content = None
    
    # Find published date
    pub_dt = _find_pub_date(html, soup)
    
    return (content or None), pub_dt


def extract_crntt(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # crntt.com系ページから本文と公開日を抽出する関数
    """Extract body and date from CRNTT article pages; drop UI/related sections."""
    soup = bs4.BeautifulSoup(html, "lxml")

    # Full visible text
    raw = soup.get_text("\n", strip=True)
    # Remove UI widgets like 〖大 中 小〗, 打印, QR prompts
    raw = re.sub(r"〖[^〗]*〗", "", raw)
    raw = re.sub(r"扫描二维码.*?(?=相关新闻|$)", "", raw, flags=re.S)
    # Keep only before the Related News block
    raw = re.split(r"相关新闻\s*[:：]", raw, maxsplit=1)[0].strip()

    # Lines for header/body split
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]

    # Find publication time from the header row near the top
    pub_dt: Optional[datetime.datetime] = None
    for s in lines[:6]:
        m = re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", s)
        if m:
            pub_dt = _parse_date(m.group(0))
            break

    # Drop the title line + the date/site line if present
    body_start = 0
    # If the second line looks like the CRNTT site+datetime row, skip first two lines
    if len(lines) >= 2 and ("CRNTT" in lines[1].upper() or re.search(r"\d{4}-\d{2}-\d{2}", lines[1])):
        body_start = 2
    elif len(lines) >= 1 and (re.search(r"\d{4}-\d{2}-\d{2}", lines[0]) or "CRNTT" in lines[0].upper()):
        body_start = 1

    body_text = "\n".join(lines[body_start:]).strip()

    # Fallback date discovery if needed
    pub_dt = pub_dt or _find_pub_date(html, soup)

    return (body_text or None), pub_dt


# Extractor for vietnam.vn aggregator pages (all locales, incl. /ko/)
# Extractor for vietnam.vn aggregator pages (all locales, incl. /ko/)
def extract_vietnam_vn(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # vietnam.vnの集約ページから本文と公開日を抽出する関数
    """Extractor for vietnam.vn (language-agnostic, HTML-structure based).
    - Uses structural containers (article/main/entry-content) rather than language tokens
    - Removes share/tags/comments/related/meta sections via CSS selectors
    - Date comes from <time datetime> / meta / JSON-LD; finally scans for dd/mm/yyyy near the header
    """
    soup = bs4.BeautifulSoup(html, "lxml")

    # Global chrome removal (language-independent)
        # 言語非依存でグローバルな装飾要素を除去
    _clean(soup, [
        "header", "nav", "footer", "aside", "form", "script", "style",
        ".site-header", ".site-footer", ".navbar", ".breadcrumbs", ".breadcrumb",
        ".topbar", ".top-nav", ".toolbar",
    ])

    # --- Locate the main article container in a language-agnostic way ---
        # 言語非依存で主要な記事コンテナを特定
    h = soup.find("h1")
    container = None
    if h:
        container = h
        for _ in range(6):  # climb to a semantic container
            if container.parent and container.parent.name in ("article", "section", "div", "main"):
                container = container.parent
            else:
                break
    # Fallback typical containers
        # 典型的な記事コンテナが見つからない場合のフォールバック
    container = (
        container or soup.select_one("article") or soup.select_one("main")
        or soup.select_one("div.entry-content") or soup.select_one("div.post-content")
        or soup.select_one("div.single-content") or soup.select_one("div.content")
    )

    # If still nothing, use the whole document as a last resort
        # それでも見つからなければドキュメント全体を使用
    if not container:
        container = soup

    # --- Remove non-article sections by CSS (NOT by words) ---
        # CSSで記事以外のセクションを除去（キーワードではなく構造で判定）
    _clean(container, [
        # social/share/follow blocks
        ".share", ".shares", ".sharing", ".share-buttons", ".share-tools",
        "[class*='share']", ".social", ".social-share", "[class*='social']", ".follow", "[class*='follow']",
        # tags/metadata blocks
        ".tags", ".post-tags", ".entry-tags", "[class*='tag-']", ".meta", ".post-meta", ".entry-meta", ".byline",
        # related/navigation/comments
        ".related", ".related-posts", ".post-related", ".post-navigation", ".nav-links", ".pagination",
        ".comments", "#comments", ".comment", ".comment-list", ".comment-form",
        # footers/credits/sources blocks commonly placed after content
        ".single-post-footer", ".post-footer", "footer", ".credit", ".credits", ".attribution", ".source", ".sources",
    ])

    # Now extract visible text from the cleaned container
        # クリーンアップ後のコンテナから可視テキストを抽出
    title_text = h.get_text(" ", strip=True) if h else ""

    def _to_text(el: bs4.Tag) -> Optional[str]:
        # 要素からテキストを抽出し、タイトル重複や空白を正規化
        raw = el.get_text("\n", strip=True)
        # If title is duplicated at the top, drop it
        if title_text and raw.startswith(title_text):
            raw = raw[len(title_text):].lstrip(" :\n")
        # normalize whitespace (no language tokens here)
        raw = re.sub(r"\s{2,}", " ", raw)
        return raw if len(raw) > 80 else None

    text = _to_text(container)

    # --- Publication date ---
        # 公開日付の抽出処理
    # Prefer robust helpers first (meta/ld+json/next-data etc.)
    pub_dt = _find_pub_date(html, soup)
    if not pub_dt:
        # Look around the header block only; pick dd/mm/yyyy which is site style
        header_scope = h.parent if h and getattr(h, "parent", None) else container
        header_text = header_scope.get_text(" ", strip=True) if header_scope else soup.get_text(" ", strip=True)
        m = re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", header_text)
        if m:
            pub_dt = _parse_date(m.group(0))

    return (text or None), pub_dt

# ここから三段カスケード記事抽出スクリプトの説明・設定


"""
三段カスケード記事抽出スクリプト
  ① ドメイン専用 extractor
  ② Trafilatura
  ③ readability‑lxml
  ④ Newspaper3k
  + 公開日付を抽出して JSON に "published" フィールドで出力
"""



##############################################################################
# 主要な抽出戦略やグローバル設定の定義
# ① DoD／CloudFront 対策 – 完全なブラウザ UA に差し替え                    ★
#    （403 回避。無用なリジェクトを減らす。）
# ---------------------------------------------------------------------------
HEADERS = {
    # HTTPリクエスト用のヘッダー設定
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    # Add realistic headers to avoid WAF 403s on some media sites
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr,fr-FR;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
TIMEOUT_SEC = 15
MIN_LENGTH  = 200

BROWSER_FALLBACK_DOMAINS = {
    # ブラウザフォールバック対象ドメインのセット
    "defense.gov", "defence.gov.au", "minister.defence.gov.au",
    "europeafrica.army.mil", "rfi.fr", "france24.com",
    "cankaoxiaoxi.com", "shobserver.com", "nytimes.com",
    "news.az", "vnexpress.net", "baotintuc.vn", "vov.vn", "wionews.com"
}

# ---- Auto-fallback & cascade guards (env-tunable) -------------------------
 # 自動フォールバックやカスケード制御のための環境変数設定
AUTO_BROWSER_FALLBACK = os.environ.get("AUTO_BROWSER_FALLBACK", "1") == "1"  # [ENV]
HTML_MIN_BYTES_FOR_SUCCESS = int(os.environ.get("HTML_MIN_BYTES_FOR_SUCCESS", "2000"))  # [ENV]
PLAYWRIGHT_MAX_TRIES = int(os.environ.get("PLAYWRIGHT_MAX_TRIES", "1"))  # [ENV]
CASCADE_BUDGET_SEC = int(os.environ.get("CASCADE_BUDGET_SEC", "35"))  # [ENV]
ACCEPT_OG_FOR_VIDEO = os.environ.get("ACCEPT_OG_FOR_VIDEO", "0") == "1"  # [ENV]
# ---- AMP fallback (env-tunable) ------------------------------------------
 # AMPページへのフォールバック設定
AMP_FALLBACK_ENABLED = os.environ.get("AMP_FALLBACK_ENABLED", "1") == "1"  # [ENV]
# Common WAF/JS-challenge/blocked page phrases (language-agnostic)
 # WAFやJSチャレンジ、ブロックページの共通パターン
_BLOCK_PAT = re.compile(
    r"(blocked|access denied|just a moment|cf-chl|cloudflare|perimeterx|you are being redirected)",
    re.I
)
# ---- LLM fallback gates (env-tunable) ------------------------------------
 # LLM（大規模言語モデル）によるフォールバック制御
ENABLE_LLM_FALLBACK = os.environ.get("ENABLE_LLM_FALLBACK", "1") == "1"  # [ENV]
LLM_REQUIRE_BROWSER_RENDER = os.environ.get("LLM_REQUIRE_BROWSER_RENDER", "0") == "1"  # [ENV]
LLM_ALLOW_ON_HTTP_ERRORS = os.environ.get("LLM_ALLOW_ON_HTTP_ERRORS", "1") == "1"  # [ENV]
LLM_HTML_MAX_CHARS = int(os.environ.get("LLM_HTML_MAX_CHARS", "200000"))  # [ENV]
LLM_MAX_RATIO = float(os.environ.get("LLM_MAX_RATIO", "0.6"))  # [ENV]

# Internal flags toggled during decode/fetch
 # デコードやフェッチ時に切り替わる内部フラグ
_REDIRECT_SPLASH_NEXT_URL: str | None = None
_LAST_FINAL_URL: str | None = None  # リダイレクト/AMP後の最終URLを保持


# This flag is set by _decode_response() so the caller can decide to escalate

def _detect_redirect_splash_next(html: str, base_url: str) -> str | None:
    # リダイレクトやスプラッシュページを検出し、次のURLを返す関数
    """
    Detect redirect splash patterns and return the *absolute* next URL if found.
    Patterns:
      - <meta http-equiv="refresh" content="0;url=...">
      - JS: location.href / location.assign = "..."
      - Title/body contains 'You are being redirected' (best-effort)
    """
    try:
        soup = bs4.BeautifulSoup(html, "lxml")
    except Exception:
        soup = None

    # 1) Meta refresh
    if soup:
        meta = soup.find("meta", attrs={"http-equiv": lambda v: v and v.lower() == "refresh"})
        if meta and meta.get("content"):
            m = re.search(r"url\s*=\s*([^;]+)", meta["content"], flags=re.I)
            if m:
                return urljoin(base_url, m.group(1).strip(" '\""))

    # 2) JS location redirect
    m = re.search(r"location(?:\.href|\.assign)\s*=\s*['\"]([^'\"]+)['\"]", html, flags=re.I)
    if m:
        return urljoin(base_url, m.group(1))

    # 3) Splash-ish copy
    if soup:
        title_txt = soup.title.get_text(strip=True) if soup.title else ""
        body_head = soup.get_text(" ", strip=True)[:400]
        if re.search(r"\bredirect(?:ed)?\b", f"{title_txt} {body_head}", flags=re.I):
            a = soup.find("a", href=True)
            if a and a["href"]:
                return urljoin(base_url, a["href"])
    return None

def _amp_candidates(u: str) -> list[str]:
    # 指定URLからAMP候補URLリストを生成する関数
    from urllib.parse import urlsplit, urlunsplit
    p = urlsplit(u)
    base = urlunsplit((p.scheme, p.netloc, p.path.rstrip("/"), "", ""))
    cands: list[str] = []
    # NYTimesは .amp.html か ?outputType=amp が効きやすい
    if p.netloc.endswith("nytimes.com") and p.path.endswith(".html"):
        cands.append(urlunsplit((p.scheme, p.netloc, p.path.replace(".html", ".amp.html"), "", "")))
        cands.append(base + "?outputType=amp")
    # WordPress/汎用CMSパターン
    cands += [
        base + "/amp", base + "/amp/",
        base + "?amp", base + "?amp=1",
        base + ".amp", base + ".amp.html",
        base + "?mode=amp",
    ]
    # de-dup
    out, seen = [], set()
    for cu in cands:
        if cu in seen:
            continue
        seen.add(cu)
        out.append(cu)
    return out

def _try_amp_for_url(url: str) -> tuple[str, str] | None:
    # AMP変種の取得を試みる関数。成功時は(amp_url, amp_html)を返す
    """
    AMP変種の取得を試みる。成功したら (amp_url, amp_html) を返す。
    """
    if not AMP_FALLBACK_ENABLED:
        return None
    for au in _amp_candidates(url):
        try:
            r = requests.get(au, headers=HEADERS, timeout=(10, 15), allow_redirects=True)
            if r.status_code >= 400 or not r.content:
                continue
            try:
                enc = r.encoding or r.apparent_encoding or "utf-8"
            except Exception:
                enc = "utf-8"
            html_txt = r.content.decode(enc, errors="replace") if r.content else (r.text or "")
            # 明らかなブロック/薄すぎるページは除外
            if _BLOCK_PAT.search(html_txt) or len(html_txt) < max(1200, HTML_MIN_BYTES_FOR_SUCCESS // 2):
                continue
            print(f"[AMP] using {au} (len={len(html_txt)})")
            _log_step(url, "AMP", "ok", reason=au, content_len=len(html_txt))
            return au, html_txt
        except Exception:
            continue
    _log_step(url, "AMP", "fail", "no-usable-amp")
    return None


def _decode_response(r: requests.Response, domain: str) -> str:
    # HTTPレスポンスをテキストにデコードし、リダイレクトやAMPの自動処理も行う関数
    """
    Decode HTTP response bytes to text robustly and set global flags:
      - _LAST_HTML_WAS_TINY -> html bytes below HTML_MIN_BYTES_FOR_SUCCESS
      - _REDIRECT_SPLASH_NEXT_URL -> absolute URL if a redirect splash is detected
    """
    global _LAST_HTML_WAS_TINY, _REDIRECT_SPLASH_NEXT_URL, _LAST_FINAL_URL
    try:
        enc = r.encoding or r.apparent_encoding or "utf-8"
    except Exception:
        enc = "utf-8"
    try:
        html_txt = r.content.decode(enc, errors="replace")
    except Exception:
        html_txt = r.text

    _LAST_HTML_WAS_TINY = len(html_txt) < HTML_MIN_BYTES_FOR_SUCCESS
    _LAST_FINAL_URL = getattr(r, "url", None)

    # Detect redirect splash and follow once
    try:
        nxt = _detect_redirect_splash_next(html_txt, r.url)
        _REDIRECT_SPLASH_NEXT_URL = nxt

        if nxt:
            try:
                rr = requests.get(nxt, headers=HEADERS, timeout=(TIMEOUT_SEC, TIMEOUT_SEC), allow_redirects=True)
                rr.raise_for_status()
                enc2 = rr.encoding or rr.apparent_encoding or "utf-8"
                html_txt2 = rr.content.decode(enc2, errors="replace") if rr.content else (rr.text or "")

                # リダイレクト後のHTMLが薄い/ブロックっぽい場合はAMPを即試す
                _LAST_HTML_WAS_TINY = len(html_txt2) < HTML_MIN_BYTES_FOR_SUCCESS
                amp_try = None
                if AMP_FALLBACK_ENABLED and (_LAST_HTML_WAS_TINY or _BLOCK_PAT.search(html_txt2)):
                    amp_try = _try_amp_for_url(rr.url)
                    if amp_try:
                        _, amp_html = amp_try
                        html_txt2 = amp_html
                        _LAST_HTML_WAS_TINY = len(html_txt2) < HTML_MIN_BYTES_FOR_SUCCESS

                # 最終URLを確定
                if amp_try:
                    _LAST_FINAL_URL = amp_try[0]
                else:
                    _LAST_FINAL_URL = rr.url

                print(f"[REDIRECT] followed redirect-splash → {rr.url}")
                _REDIRECT_SPLASH_NEXT_URL = None
                return html_txt2
            except Exception:
                _REDIRECT_SPLASH_NEXT_URL = None
    except Exception:
        _REDIRECT_SPLASH_NEXT_URL = None

    # AMP auto-fallback for thin/blocked HTML
    if AMP_FALLBACK_ENABLED and (_LAST_HTML_WAS_TINY or _BLOCK_PAT.search(html_txt)):
        try:
            amp_try = _try_amp_for_url(r.url)
            if amp_try:
                amp_url, amp_html = amp_try
                html_txt = amp_html
                _LAST_HTML_WAS_TINY = len(html_txt) < HTML_MIN_BYTES_FOR_SUCCESS
                _LAST_FINAL_URL = amp_url
        except Exception:
            pass

    return html_txt


def _maybe_follow_redirect_splash(current_url: str) -> requests.Response | None:
    # リダイレクトスプラッシュが検出された場合に一度だけ追従する関数
    """
    If a redirect-splash next URL was detected during decode, follow it once and return the response.
    Otherwise return None. Resets the global redirect hint after use.
    """
    global _REDIRECT_SPLASH_NEXT_URL
    if not _REDIRECT_SPLASH_NEXT_URL:
        return None
    try:
        nxt = urljoin(current_url, _REDIRECT_SPLASH_NEXT_URL)
        print(f"[REDIRECT] followed redirect-splash → {nxt}")
        rr = requests.get(nxt, headers=HEADERS, timeout=(TIMEOUT_SEC, TIMEOUT_SEC), allow_redirects=True)
        rr.raise_for_status()
        _REDIRECT_SPLASH_NEXT_URL = None
        return rr
    except Exception:
        _REDIRECT_SPLASH_NEXT_URL = None
        return None

_LAST_HTML_WAS_TINY = False

# 直前のHTMLが極端に短かったかどうかのフラグ

# LLM readiness helper and one-time debug ping
def _llm_is_ready() -> bool:
    # LLM（大規模言語モデル）利用のための環境変数チェック関数
    ok = bool(os.environ.get("GOOGLE_CLOUD_PROJECT")) and bool(os.environ.get("VERTEX_LOCATION"))  # [ENV]
    if not ok:
        print("[LLM] Vertex config missing (GOOGLE_CLOUD_PROJECT / VERTEX_LOCATION)")
    return ok

# Emit readiness once at startup if LLM fallback is requested
if os.environ.get("ENABLE_LLM_FALLBACK") in ("1", "true", "True"):
    # LLMフォールバックが有効な場合、起動時に一度だけ準備状況を出力
    _ = _llm_is_ready()


# Helper for cankaoxiaoxi.com title extraction (structure-based, language-agnostic)
def _cankaoxiaoxi_find_title(soup: bs4.BeautifulSoup) -> Optional[str]:
    # cankaoxiaoxi.com用のタイトル抽出補助関数
    """Prefer meta title, then structural headings inside the detail/article container.
    Avoid returning the generic shell <title> ("参考消息") if any specific DOM title exists.
    """
    # 1) Meta first
    for css, attr in (("meta[property='og:title']", "content"), ("meta[name='twitter:title']", "content"), ("meta[name='title']", "content")):
        tag = soup.select_one(css)
        if tag and tag.get(attr):
            t = tag.get(attr).strip()
            if t:
                return t

    # 2) Search inside likely article containers
    containers = [
        soup.select_one("article"),
        soup.select_one("#app main"),
        soup.select_one("[role='main']"),
        soup.select_one("[id*='detail']"),
        soup.select_one("[class*='detail']"),
        soup
    ]
    shell = soup.title.string.strip() if soup.title and soup.title.string else None
    candidates: list[str] = []
    sels = [
        "h1", "h2", "h3",
        "[class*='title']", "[class*='headline']", "[class*='subject']"
    ]
    for cont in containers:
        if not cont:
            continue
        for sel in sels:
            for el in cont.select(sel):
                txt = el.get_text(" ", strip=True)
                if not txt:
                    continue
                if shell and txt == shell:
                    continue
                if len(txt) < 6:
                    continue
                candidates.append(txt)
    if candidates:
        # Prefer the longest candidate (tends to be the article title) 
        candidates.sort(key=len, reverse=True)
        return candidates[0].strip()

    # 3) Last resort: return <title>
    if shell:
        return shell
    return None

# cankaoxiaoxi.com extractor (HTML-structure based, language-agnostic)
def extract_cankaoxiaoxi(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # cankaoxiaoxi.comの記事詳細ページから本文と公開日を抽出する関数
    """
    Extractor for cankaoxiaoxi.com article detail pages (SPA-friendly).
    Strategy:
      - Prefer rendered DOM paragraphs by structural selectors (language-agnostic).
      - If empty: pick the largest text block anywhere in the document (global).
      - If still empty: parse <script> JSON payloads for keys like content/contentHtml/body and HTML-decode.
      - Published datetime via generic helpers; fallback to <time datetime> or JSON in <script>.
    Returns (content, pub_dt).
    """
    soup = bs4.BeautifulSoup(html, "lxml")

    # 1) Locate a likely article container (structure-only, no language tokens)
    # 1) 記事本体のコンテナを構造のみで特定（言語非依存）
    article = (
        soup.find("article")
        or soup.select_one("main article")
        or soup.select_one("[role='main'] article")
        or soup.select_one("#app main")
        or soup.select_one("main")
        or soup.select_one("[id*='detail'], [class*='detail']")
        or soup.select_one("[id*='content'], [class*='content']")
        or soup
    )

    # 2) Remove obvious chrome/UI blocks inside that container
    # 2) 記事コンテナ内の明らかなUI・装飾要素を除去
    _clean(article, [
        "header", "nav", "footer", "aside", "form", "script", "style",
        ".breadcrumbs", ".breadcrumb", ".toolbar",
        ".share", ".share-tools", ".share-buttons", "[class*='share']", ".social", ".social-share",
        ".tags", ".tag-list", ".post-tags", ".entry-tags", ".byline", ".author", ".meta", ".post-meta", ".entry-meta",
        ".related", ".related-stories", ".more-stories", ".article-related", ".recommendations", ".read-more",
        ".comments", "#comments", ".comment", ".comment-list", ".comment-form",
        ".newsletter", ".subscribe", ".subscription", ".paywall",
        ".inline-video", ".video-player", ".audio-player", ".gallery", ".slideshow",
        "figure", "figcaption", "iframe", "noscript",
        ".ad", ".advertisement", "[id*='ad-']", "[class*='ad-']",
    ])

    # 3) Body containers commonly seen on news templates
    # 3) ニュース系テンプレートでよく使われる本文コンテナを探索
    body = (
        article.select_one("[itemprop='articleBody']")
        or article.select_one("[class*='article__body']")
        or article.select_one("[class*='story-body']")
        or article.select_one("[class*='detail-content'], [class*='detail__content'], [class*='detailBody']")
        or article.select_one("[class*='article-content'], [class*='article__content'], [class*='content__article']")
        or article.select_one("[class*='rich'], [class*='text'], [class*='txt'], [class*='content']")
        or article
    )

    # 4) Collect substantial paragraphs (p/li + paragraph-like div/span). Language-agnostic.
    # 4) 実質的な段落要素（p/li/div/span）を収集（言語非依存）
    parts: list[str] = []

    def _looks_like_ui(el: bs4.Tag) -> bool:
        # UIや装飾要素かどうかを判定する補助関数
        cls = " ".join(el.get("class", [])).lower()
        idv = (el.get("id") or "").lower()
        hay = " ".join([cls, idv])
        ui_tokens = ("share", "tag", "breadcrumb", "nav", "comment", "meta", "byline", "author", "toolbar", "footer", "banner")
        return any(tok in hay for tok in ui_tokens)

    for el in body.find_all(["p", "li", "div", "span"], recursive=True):
        if _looks_like_ui(el):
            continue
        # treat <div>/<span> as paragraph-like only if they are not obvious wrappers of other blocks
        if el.name in {"div", "span"} and el.find(["section", "article", "figure", "ul", "ol"]):
            continue
        txt = el.get_text(" ", strip=True)
        if not txt or len(txt) < 25:
            continue
        parts.append(txt)

    content = "\n".join(parts).strip() if parts else None
    if content and len(content) < 160:
        content = None  # force fallback if too short

    # 5) Global largest-block fallback (search whole document) if still empty
    # 5) まだ空ならドキュメント全体から最大テキストブロックを抽出
    if not content:
        candidates = []
        for el in soup.find_all(["div", "section", "article", "main"], recursive=True):
            if not isinstance(el, bs4.Tag):
                continue
            if _looks_like_ui(el):
                continue
            t = el.get_text(" ", strip=True)
            if t and len(t) > 120:  # article-sized threshold
                candidates.append((len(t), t))
        if candidates:
            candidates.sort(reverse=True)
            top3 = ", ".join(str(c[0]) for c in candidates[:3])
            print(f"[CXN] top block sizes (global): {top3}")
            longest = candidates[0][1]
            # Split on typical paragraph separators
            paras = [p.strip() for p in re.split(r"\n+|\s{2,}", longest) if len(p.strip()) >= 25]
            if len(paras) >= 2:
                content = "\n".join(paras)

    # 6) Script-JSON fallback (look for content/contentHtml/body fields with HTML)
    # 6) <script>内のJSONから本文HTMLを抽出するフォールバック
    if not content:
        for tag in soup.find_all("script"):
            raw = tag.string or "".join(tag.strings) or ""
            if not raw or len(raw) < 100:
                continue
            # Look for long HTML-bearing fields inside JSON-ish blobs
            for m in re.finditer(r'"(?:content|contentHtml|articleContent|body|newsContent|detail(?:Text|Html)?)"\s*:\s*"(.*?)"', raw, flags=re.S):
                val = m.group(1)
                if ("<p" not in val) and ("&lt;p" not in val) and ("\\u003Cp" not in val):
                    continue
                try:
                    # Unescape typical JSON escapes
                    val_unesc = val.encode("utf-8").decode("unicode_escape")
                except Exception:
                    val_unesc = val
                val_unesc = val_unesc.replace("\\/", "/").replace('\\"', '"')
                # HTML-decode via BS4
                frag = bs4.BeautifulSoup(val_unesc, "lxml")
                txt = frag.get_text("\n", strip=True)
                if txt and len(txt) >= 160:
                    content = txt
                    print("[CXN] recovered content via script-JSON fallback")
                    break
            if content:
                break

    if content:
        print(f"[CXN] captured body length: {len(content)}")

    # 7) Published datetime
    # 7) 公開日付の抽出処理
    pub_dt: Optional[datetime.datetime] = _find_pub_date(html, soup)
    if not pub_dt:
        t = article.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            pub_dt = _parse_date(t.get("datetime"))

    if not pub_dt:
        # JSON inside <script> tags (language-agnostic keys)
        for tag in soup.find_all("script"):
            raw = (tag.string or "") + "".join(tag.stripped_strings or [])
            if not raw:
                continue
            m = re.search(r'"(?:datePublished|publishTime|releaseTime|time)"\s*:\s*"([^"]{10,})"', raw)
            if m:
                if dt := _parse_date(m.group(1)):
                    pub_dt = dt
                    break

    return (content or None), pub_dt
BROWSER_NAV_TIMEOUT_MS   = 30000
 # Playwrightブラウザのナビゲーションタイムアウト設定（ミリ秒）
BROWSER_IDLE_WAIT_MS     = 1500
 # Playwrightブラウザのアイドル待機時間（ミリ秒）

def _decode_response(r: requests.Response, domain: str) -> str:
    # HTTPレスポンスをHTMLテキストにデコードし、リダイレクトや短いHTMLの判定も行う関数
    """
    Decode HTTP response bytes to HTML text.
    Extras:
      - Follow one-level redirect splash (meta refresh or JS location.*) when AUTO_BROWSER_FALLBACK=1.
      - Mark extremely short HTML via _LAST_HTML_WAS_TINY so caller can escalate to Playwright if desired.
    """
    import re as _re
    from urllib.parse import urljoin as _urljoin

    global _LAST_HTML_WAS_TINY

    raw = r.content or b""

    # 1) Choose encoding: header -> meta -> requests' guess -> utf-8
    enc = None
    ctype = (r.headers.get("Content-Type") or "")
    m = _re.search(r"charset\s*=\s*([A-Za-z0-9_\-]+)", ctype, flags=_re.I)
    if m:
        enc = m.group(1).strip().lower()
    if not enc:
        m2 = _re.search(br"charset=['\"]?\s*([A-Za-z0-9_\-]+)", raw[:2048], flags=_re.I)
        if m2:
            try:
                enc = m2.group(1).decode("ascii", "ignore").lower()
            except Exception:
                enc = None
    if not enc:
        enc = (getattr(r, "encoding", None) or "utf-8")

    try:
        html = raw.decode(enc, errors="replace")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    # 2) Mark tiny HTML (helps trigger auto-fallback at call site)
    try:
        _LAST_HTML_WAS_TINY = (len(html) < HTML_MIN_BYTES_FOR_SUCCESS)
    except Exception:
        _LAST_HTML_WAS_TINY = False

    # 3) Redirect-splash detection & single follow (meta refresh / JS location.*)
    if AUTO_BROWSER_FALLBACK:
        # meta http-equiv="refresh" （エスケープと素の両方）
        mmeta = _re.search(
            r'&lt;meta[^&gt;]+http-equiv=["\']?refresh["\']?[^&gt;]+content=["\']?[^"\'>]*url=([^"\';&gt;]+)',
            html, flags=_re.I
        )
        if not mmeta:
            mmeta = _re.search(
                r'<meta[^>]+http-equiv=["\']?refresh["\']?[^>]+content=["\']?[^"\'>]*url=([^"\';>]+)',
                html, flags=_re.I
            )
        # JS redirect
        mjs = None
        if not mmeta:
            mjs = _re.search(
                r'location(?:\.href|\.replace)\s*=\s*["\']([^"\']+)["\']',
                html, flags=_re.I
            )

        target = (mmeta.group(1) if mmeta else (mjs.group(1) if mjs else None))
        looks_like_redirect_text = bool(_re.search(r"\bYou are being redirected\b|redirecting\s*\.\.\.", html, _re.I))

        # フラグ or 文言があれば、ワンステップだけ追従
        if target or looks_like_redirect_text:
            try:
                nxt = _urljoin(r.url, target) if target else r.url
                r2 = requests.get(nxt, headers=HEADERS, timeout=(10, TIMEOUT_SEC), allow_redirects=True)
                r2.raise_for_status()
                raw2 = r2.content or b""
                enc2 = (r2.encoding or "utf-8")
                try:
                    html2 = raw2.decode(enc2, errors="replace")
                except Exception:
                    html2 = raw2.decode("utf-8", errors="replace")

                if len(html2) > len(html):
                    print("[REDIRECT] followed redirect-splash →", nxt)
                    html = html2
                    _LAST_HTML_WAS_TINY = (len(html) < HTML_MIN_BYTES_FOR_SUCCESS)
            except Exception:
                pass

    return html

# ---- Date recovery (meta/JSON-LD/NEXT_DATA/text scan) ----------------------
def _recover_published_date(html: str, soup: bs4.BeautifulSoup) -> Optional[datetime.datetime]:
    # HTMLやsoupから公開日付を多段階で抽出する関数
    dt = _find_pub_date(html, soup)
    if dt:
        return dt
    # __NEXT_DATA__ JSON
    try:
        nd = soup.find("script", id="__NEXT_DATA__")
        if nd and nd.string:
            data = json.loads(nd.string)
            def _walk(o):
                if isinstance(o, dict):
                    for k, v in o.items():
                        if isinstance(v, (dict, list)):
                            r = _walk(v)
                            if r:
                                return r
                        elif isinstance(v, str) and k.lower() in {"publication_date","date","published","datepublished"}:
                            d = _parse_date(v)
                            if d:
                                return d
                elif isinstance(o, list):
                    for it in o:
                        r = _walk(it)
                        if r:
                            return r
                return None
            dt = _walk(data)
            if dt:
                return dt
    except Exception:
        pass
    # Visible <time datetime>
    t = soup.find("time", attrs={"datetime": True})
    if t and t.get("datetime"):
        dt = _parse_date(t.get("datetime"))
        if dt:
            return dt
    # Text scan
    txt = soup.get_text(" ", strip=True)
    for rx in [
        r"\b\d{4}-\d{1,2}-\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b",
        r"\d{4}年\d{1,2}月\d{1,2}日",
        r"\b\d{1,2}\s+[A-Z][a-z]{2}\s+\d{4}(?:\s+\d{1,2}:\d{2})?\b",
    ]:
        m = re.search(rx, txt)
        if m:
            d = _parse_date(m.group(0))
            if d:
                return d
    return None



# ---- LLM extraction (Vertex AI Gemini; Flash → Pro) with 50% gate ----------
 # LLM（Vertex AI Gemini）による記事抽出と利用制御
LLM_ENABLE = os.environ.get("LLM_ENABLE", "1") == "1"
LLM_MAX_RATIO = float(os.environ.get("LLM_MAX_RATIO", "0.5"))
LLM_USED_COUNT = 0
LLM_PROCESSED_COUNT = 0

# Vertex AI config (API keyは不要。Cloud RunのサービスアカウントでADC/IAM認可)
GCP_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT_ID")
VERTEX_LOCATION = os.environ.get("VERTEX_LOCATION") or os.environ.get("GOOGLE_CLOUD_LOCATION") or "us-central1"
GEMINI_FLASH_MODEL = os.environ.get("GEMINI_FLASH_MODEL", "gemini-1.5-flash")
GEMINI_PRO_MODEL   = os.environ.get("GEMINI_PRO_MODEL", "gemini-1.5-pro")

def _llm_gate_allows() -> bool:
    # LLM利用回数の割合に基づき利用可否を判定する関数
    if not LLM_ENABLE:
        return False
    try:
        ratio = (LLM_USED_COUNT / max(1, LLM_PROCESSED_COUNT))
        return ratio < LLM_MAX_RATIO
    except Exception:
        return False

def _llm_prompt_for_html(html: str) -> dict:
    # LLM用のプロンプトを生成する関数
    sys_prompt = (
        "You are a strict extractor. From the given HTML, extract TITLE, BODY, and DATE. "
        "Return ONLY valid JSON with keys: title, body, date (ISO8601). "
        "Rules: ignore navigation/ads/related/comments; keep article paragraphs; "
        "if multiple dates, choose the published date; if uncertain, leave empty string."
    )
    user_prompt = (
        "Extract from this HTML. Output JSON only. Keys: title (string), body (string), date (ISO8601 string).\n\n"
        "<HTML>\n" + html[:300000] + "\n</HTML>"
    )
    return {"contents": [{"role": "user", "parts": [{"text": sys_prompt + "\n\n" + user_prompt}]}]}

def _llm_call_vertex(model: str, html: str) -> Optional[dict]:
    # Vertex AI Gemini APIを呼び出して記事情報を抽出する関数
    """Call Vertex AI Generative (Gemini) via REST using ADC/IAM (no API key)."""
    if not (GCP_PROJECT_ID and VERTEX_LOCATION):
        print("[LLM] Vertex config missing (GOOGLE_CLOUD_PROJECT / VERTEX_LOCATION)")
        return None
    try:
        from google.auth import default as gauth_default  # type: ignore
        from google.auth.transport.requests import Request as GARequest  # type: ignore
    except Exception as e:
        print("[LLM] google-auth not available:", e)
        return None

    try:
        creds, _ = gauth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        if not getattr(creds, "valid", False):
            creds.refresh(GARequest())
        token = creds.token
    except Exception as e:
        print("[LLM] ADC token error:", e)
        return None

    payload = _llm_prompt_for_html(html)
    endpoint = (
        f"https://{VERTEX_LOCATION}-aiplatform.googleapis.com/v1/projects/"
        f"{GCP_PROJECT_ID}/locations/{VERTEX_LOCATION}/publishers/google/models/{model}:generateContent"
    )
    try:
        resp = requests.post(
            endpoint,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=60,
        )
    except Exception as e:
        print("[LLM] HTTP error:", e)
        return None

    if resp.status_code >= 400:
        print("[LLM] HTTP", resp.status_code, resp.text[:200])
        return None

    try:
        data = resp.json()
        # pull text from candidates[0].content.parts[].text
        txt = None
        parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
        for p in parts:
            if isinstance(p, dict) and "text" in p:
                txt = p["text"]
                break
        if not txt:
            return None
        # strict JSON extraction
        m = re.search(r"\{[\s\S]*\}", txt)
        if not m:
            return None
        js = json.loads(m.group(0))
        return js if isinstance(js, dict) else None
    except Exception as e:
        print("[LLM] parse error:", e)
        return None

def _try_llm_extract(url: str, html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # LLM（Gemini）による最終手段の本文・日付抽出関数
    """Final resort: Vertex AI Gemini (Flash → Pro), enabled only when gate allows."""
    global LLM_USED_COUNT, LLM_PROCESSED_COUNT
    LLM_PROCESSED_COUNT += 1
    if not _llm_gate_allows():
        _log_step(url, "llm", "skipped", reason="gate")
        return None, None

    js = _llm_call_vertex(GEMINI_FLASH_MODEL, html)
    model_used = "llm:flash-vertex"
    if not js:
        js = _llm_call_vertex(GEMINI_PRO_MODEL, html)
        model_used = "llm:pro-vertex" if js else model_used

    if not js:
        _log_step(url, "llm", "failed", reason="no json")
        return None, None

    body = (js.get("body") or "").strip()
    date_s = (js.get("date") or "").strip()
    if not body or len(body) < MIN_LENGTH:
        _log_step(url, model_used, "failed", reason="short body", content_len=len(body or ""))
        return None, None

    pub_dt = _parse_date(date_s) if date_s else None
    _log_step(url, model_used, "success", content_len=len(body), published=(pub_dt.isoformat() if pub_dt else None))
    LLM_USED_COUNT += 1
    return body, pub_dt


def _browser_fetch(url: str) -> Optional[str]:
    # PlaywrightブラウザでページをレンダリングしてHTMLを取得する関数
    global _LAST_FETCH_WAS_BROWSER
    try:
        use_pool = BROWSER_POOL
        browser = None
        if use_pool:
            browser = _ensure_browser()
            if browser is None:
                use_pool = False
        if not use_pool:
            try:
                from playwright.sync_api import sync_playwright
            except Exception as e:
                print("[BROWSER] Playwright not available:", e)
                _log_step(url, "playwright", "failed", reason=f"Playwright import: {e}")
                return None
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                return _browser_fetch_with_browser(url, browser)
        else:
            html = _browser_fetch_with_browser(url, browser)
            _browser_used()
            return html
    except Exception as e:
        print("[BROWSER] fetch failed:", e)
        _log_step(url, "playwright", "failed", reason=str(e))
        return None

def _browser_fetch_with_browser(url: str, browser) -> Optional[str]:
    # 既存のPlaywrightブラウザインスタンスでページをレンダリングしHTMLを取得する関数
    global _LAST_FETCH_WAS_BROWSER
    _LAST_FETCH_WAS_BROWSER = True
    _ctx_kwargs = {}
    _locale = "fr-FR"
    _accept_lang = HEADERS.get("Accept-Language", "fr,fr-FR;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6")
    _timezone = None
    if "france24.com" in url:
        _locale = "en-US"
        _accept_lang = "en-US,en;q=0.9,fr;q=0.7,ja;q=0.6"
    elif "shobserver.com" in url:
        _locale = "zh-CN"
        _accept_lang = "zh-CN,zh;q=0.9,en;q=0.6,ja;q=0.5"
    elif "defence.gov.au" in url or "minister.defence.gov.au" in url:
        _locale = "en-AU"
        _accept_lang = "en-AU,en;q=0.9"
        _timezone = "Australia/Sydney"
    _ctx_kwargs.update({
        "user_agent": HEADERS.get("User-Agent"),
        "locale": _locale,
        "extra_http_headers": {"Accept-Language": _accept_lang},
    })
    if _timezone:
        _ctx_kwargs["timezone_id"] = _timezone

    context = browser.new_context(**_ctx_kwargs)
    page = context.new_page()
    print("[BROWSER] headless render start")
    try:
        from urllib.parse import urlparse as _urlparse_for_browser_dbg
        _domain_for_browser_dbg = _urlparse_for_browser_dbg(url).netloc
    except Exception:
        _domain_for_browser_dbg = ""
    page.set_default_timeout(BROWSER_NAV_TIMEOUT_MS)
    try:
        page.goto(url, wait_until="networkidle")
    except Exception as e:
        print("[BROWSER][WARN] networkidle goto failed:", e)
        try:
            page.goto(url, wait_until="domcontentloaded")
        except Exception as e2:
            print("[BROWSER][ERROR] domcontentloaded goto failed:", e2)
            context.close()
            _log_step(url, "playwright", "failed", reason=str(e2))
            return None

    # domain waits
    if _domain_for_browser_dbg.endswith("shobserver.com"):
        try:
            page.wait_for_selector("#newsDetail, #newsContent, article", timeout=15000)
        except Exception:
            pass
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(600)
        except Exception:
            pass
        try:
            prev = -1
            for _ in range(14):
                cur = page.evaluate("document.body.innerText.length")
                if cur and cur > 1200 and cur == prev:
                    break
                prev = cur
                page.wait_for_timeout(650)
        except Exception:
            pass

    if _domain_for_browser_dbg.endswith("minister.defence.gov.au") or _domain_for_browser_dbg.endswith("defence.gov.au"):
        try:
            page.wait_for_selector("main article, main, article, #content", timeout=15000)
        except Exception:
            pass
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(600)
        except Exception:
            pass
        try:
            prev = -1
            for _ in range(14):
                cur = page.evaluate("document.body.innerText.length")
                if cur and cur > 1000 and cur == prev:
                    break
                prev = cur
                page.wait_for_timeout(650)
        except Exception:
            pass

    if _domain_for_browser_dbg.endswith("cankaoxiaoxi.com"):
        try:
            page.wait_for_selector("article, #app main, [id*='detail'], [class*='detail'], [itemprop='articleBody']", timeout=15000)
        except Exception:
            pass
    
    if _domain_for_browser_dbg.endswith("ipdefenseforum.com"):
        try:
            page.wait_for_selector("article, .entry-content, .post-content, [class*='content'], .story-body", timeout=20000)
        except Exception:
            pass
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(600)
        except Exception:
            pass
        try:
            prev = -1
            for _ in range(14):
                cur = page.evaluate("document.body.innerText.length")
                if cur and cur > 1400 and cur == prev:
                    break
                prev = cur
                page.wait_for_timeout(650)
        except Exception:
            pass

    # consent buttons
    for sel in [
        "button:has-text(\"J’accepte\")",
        "button:has-text(\"J'accepte\")",
        "button:has-text(\"Accepter\")",
        "button:has-text(\"Tout accepter\")",
        "button:has-text(\"Accept\")",
        "button:has-text(\"Agree\")",
        "button:has-text(\"Agree & close\")",
        "button:has-text(\"Accept and close\")",
        "button:has-text(\"I agree\")",
        "button[aria-label*='Accept' i]",
        "button[aria-label*='agree' i]",
        "[data-accept]",
        "[data-testid*='accept']",
        "button#didomi-notice-agree-button",
        "#didomi-notice .didomi-approve-button",
        "button#onetrust-accept-btn-handler",
        ".ot-sdk-container #onetrust-accept-btn-handler",
        ".qc-cmp2-summary-buttons .qc-cmp2-summary-accept-all",
        ".qc-cmp2-footer .qc-cmp2-accept-all",
    ]:
        try:
            loc = page.locator(sel)
            if loc.first.is_visible():
                loc.first.click()
                break
        except Exception:
            pass

    page.wait_for_timeout(BROWSER_IDLE_WAIT_MS)
    html_str = page.content()
    print(f"[BROWSER] rendered HTML length: {len(html_str)}")

    # Headlessレンダ後でも薄い/ブロックっぽい場合はAMPをスワップ
    if AMP_FALLBACK_ENABLED and (len(html) < HTML_MIN_BYTES_FOR_SUCCESS or _BLOCK_PAT.search(html)):
        amp_try = _try_amp_for_url(url)  # このスコープでの対象URL変数名が 'url' でない場合は合わせてください
        if amp_try:
            amp_url, amp_html = amp_try
            html = amp_html
            print(f"[AMP] swapped in AMP HTML (len={len(html)}) from {amp_url}")
    
    # If browser-rendered HTML is tiny or looks like a blocker page, try AMP
    if AMP_FALLBACK_ENABLED and (len(html) < HTML_MIN_BYTES_FOR_SUCCESS or _BLOCK_PAT.search(html)):
        amp_try = _try_amp_for_url(url)
        if amp_try:
            new_url, amp_html = amp_try
            html = amp_html
            print(f"[AMP] swapped in AMP HTML (len={len(html)}) from {new_url}")

    try:
        if SAVE_RENDERED_HTML_ALL or _domain_for_browser_dbg.endswith("cankaoxiaoxi.com"):
            from pathlib import Path as _P
            _P(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
            ts = time.strftime("%Y%m%d_%H%M%S")
            dbg_path = _P(OUTPUT_DIR)/f"_debug_{_domain_for_browser_dbg or 'page'}_{ts}.html"
            with dbg_path.open("w", encoding="utf-8") as fp:
                fp.write(html_str)
            print(f"[BROWSER] saved debug HTML → {dbg_path}")
    except Exception:
        pass

    context.close()
    if html_str:
        _log_step(url, "playwright", "success", content_len=len(html_str))
    else:
        _log_step(url, "playwright", "failed", reason="no html")
    return html_str

##############################################################################
# ④ 多言語・多形式 – 正規表現を全面拡充                                 ★
_TEXT_DATE_RE = re.compile(
 # 多言語・多形式の日付抽出用正規表現
    r"""\b(
        \d{4}[./-]\d{1,2}[./-]\d{1,2}
        (?:[T\s]\d{2}:\d{2}(?::\d{2})?)?
      | [A-Z][a-z]+ \s+\d{1,2},\s+\d{4}
      | \d{4}年\d{1,2}月\d{1,2}日(?:\d{1,2}:\d{2})?
    )\b""", re.X
)

def _parse_date(raw: Optional[str]) -> Optional[datetime.datetime]:
    # 文字列から日付をパースする関数
    if not raw:
        return None
    print("[PARSE] Attempting:", raw)
    try:
        dt = dtparser.parse(raw, fuzzy=True)
        print("[PARSE] dtparser success:", dt)
        return dt
    except Exception as e:
        print("[PARSE] dtparser failed:", e)
    try:
        dt = dparse(raw, settings={"RETURN_AS_TIMEZONE_AWARE": False})
        print("[PARSE] dateparser success:", dt)
        return dt
    except Exception as e:
        print("[PARSE] dateparser failed:", e)
        return None

# ④‑2 正規表現にヒットしない場合も全体をスキャンして候補を拾う
def _search_date_in_text(text: str) -> Optional[datetime.datetime]:
    # テキスト全体から日付を検索しパースする関数
    try:
        found = search_dates(
            text,
            languages=["en", "ja", "zh", "ko"],
            settings={"RETURN_AS_TIMEZONE_AWARE": False},
        )
        if not found:
            return None
        candidates = [
            _parse_date(dt.isoformat() if isinstance(dt, datetime.datetime) else dt)
            for _txt, dt in found
        ]
        candidates = [c for c in candidates if c and c <= datetime.datetime.now()]
        return max(candidates) if candidates else None
    except Exception:
        return None

def _find_date_from_text(text: str) -> Optional[datetime.datetime]:
    # 正規表現で日付を抽出しパースする関数
    if m := _TEXT_DATE_RE.search(text):
        return _parse_date(m.group(1))
    return None

def _find_date_from_jsonld(soup: bs4.BeautifulSoup) -> Optional[datetime.datetime]:
    # JSON-LDスクリプトから日付を抽出する関数
    for tag in soup.select("script[type='application/ld+json']"):
        try:
            data = json.loads(html.unescape(tag.string or ""))
            print("[JSONLD] keys:", list(data.keys()) if isinstance(data, dict) else type(data))
            if isinstance(data, dict) and "@graph" in data:
                data = data["@graph"]
            if isinstance(data, dict):
                raw = data.get("datePublished") or data.get("dateCreated")
                if dt := _parse_date(raw):
                    return dt
            elif isinstance(data, list):
                for item in data:
                    raw = item.get("datePublished") or item.get("dateCreated")
                    if dt := _parse_date(raw):
                        return dt
        except Exception:
            continue
    return None

# ② Next.js (__NEXT_DATA__) 対応
def _find_date_from_next_data(soup: bs4.BeautifulSoup) -> Optional[datetime.datetime]:
    # Next.jsの__NEXT_DATA__スクリプトから日付を抽出する関数
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return None
    try:
        data = json.loads(tag.string)
        pageprops = data.get("props", {}).get("pageProps", {})
        for k in ("uploadDate", "publication_date", "datePublished"):
            if dt := _parse_date(pageprops.get(k)):
                return dt
    except Exception:
        pass
    return None

_DATE_META = [
    {"css": "meta[property='article:published_time']", "attr": "content"},
    {"css": "meta[name='pubdate']",                     "attr": "content"},
    {"css": "meta[name='timestamp']",                   "attr": "content"},
    {"css": "meta[itemprop='datePublished']",           "attr": "content"},
    {"css": "meta[name='DC.date']",                     "attr": "content"},
    {"css": "meta[name='dcterms.date']",                "attr": "content"},
    {"css": "meta[property='og:published_time']",       "attr": "content"},
    {"css": "meta[property='og:updated_time']",         "attr": "content"},
    {"css": "meta[property='article:modified_time']",   "attr": "content"},
    {"css": "meta[name='date']",                        "attr": "content"},
    {"css": "meta[name='lastmod']",                     "attr": "content"},
    {"css": "meta[name='DC.date.issued']",              "attr": "content"},
    {"css": "meta[property='og:video:release_date']",   "attr": "content"},
    {"css": "meta[itemprop='uploadDate']",              "attr": "content"},
    {"css": "time[datetime]",                           "attr": "datetime"},
]

def _find_date_from_meta(soup: bs4.BeautifulSoup) -> Optional[datetime.datetime]:
    # metaタグ等から日付を抽出する関数
    for cfg in _DATE_META:
        tags = soup.select(cfg["css"])
        print(f"[META] Checking {cfg['css']} → found {len(tags)}")
        for tag in tags:
            if tag.has_attr(cfg["attr"]):
                print(f"[META] Trying {cfg['css']} = {tag[cfg['attr']]}")
                if dt := _parse_date(tag[cfg["attr"]]):
                    print(f"[META] Parsed date from {cfg['css']}: {dt}")
                    return dt
    meta_tag = soup.find("meta", attrs={"name": "date"})
    if meta_tag and meta_tag.get("content"):
        print("[META-FALLBACK] meta[name='date'] content =", meta_tag["content"])
        if dt := _parse_date(meta_tag["content"]):
            return dt
    return None

def _find_pub_date(html: str, soup: bs4.BeautifulSoup) -> Optional[datetime.datetime]:
    # ページ全体から公開日付を多段階で抽出する関数
    if dt := _find_date_from_meta(soup):
        return dt
    if dt := _find_date_from_next_data(soup):
        return dt
    if dt := _find_date_from_jsonld(soup):
        return dt
    text = soup.get_text(" ", strip=True)
    return _find_date_from_text(text) or _search_date_in_text(text)

def _clean(soup: bs4.BeautifulSoup, selectors: list[str]) -> None:
    # 指定したCSSセレクタの要素をsoupから除去する関数
    for sel in selectors:
        for tag in soup.select(sel):
            tag.decompose()

def extract_defensenews(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # defensenews.comの記事ページから本文と公開日を抽出する関数
    soup = bs4.BeautifulSoup(html, "lxml")
    body = soup.select_one("div.article-body") or soup.article
    if not body:
        return None, None
    _clean(body, [".author-name", ".tags", "script", "style", "aside", "figure",
                  ".related", ".article__related", ".share-links",
                  "div[data-qa='article-related']", "footer"])
    text = body.get_text("\n", strip=True) or None
    dt_tag = soup.select_one("meta[property='article:published_time']") or soup.select_one("time[datetime]")
    pub_dt = _parse_date(dt_tag.get("content") if dt_tag and dt_tag.has_attr("content") else
                         dt_tag["datetime"] if dt_tag and dt_tag.has_attr("datetime") else None)
    return text, pub_dt

# ⑤ DVIDS & DoD 共通 extractor
def extract_dvids_generic(html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # DVIDSやDoD系の汎用記事ページから本文と公開日を抽出する関数
    soup = bs4.BeautifulSoup(html, "lxml")
    # Try to get main description from broad containers
    body = (
        soup.select_one("div.page-content")
        or soup.select_one("div.main-content")
        or soup.select_one("article")
    )
    text = body.get_text("\n", strip=True) if body else soup.get_text("\n", strip=True)
    date_tag = (
        soup.select_one(".release-date")
        or soup.select_one("time[datetime]")
        or soup.select_one("span.date")  # Defense.gov 用
    )
    raw = (
        date_tag["datetime"]
        if date_tag and date_tag.has_attr("datetime")
        else date_tag.get_text(strip=True)
        if date_tag
        else None
    )
    pub_dt = _parse_date(raw) or _find_date_from_meta(soup) or _find_date_from_jsonld(soup)
    return text, pub_dt



def fallback_extract(url: str, html: str) -> Tuple[Optional[str], Optional[datetime.datetime]]:
    # Trafilatura→Readability→Newspaper3k→LLMのカスケードで本文・日付を抽出する関数
    """Three-step cascade (Trafilatura → Readability → Newspaper3k) with date enforcement.
    If all fail and the page came via Playwright, try LLM as the very last resort (50% gate).
    """
    # ① Trafilatura
    try:
        txt = trafilatura.extract(html, include_comments=False, include_tables=False) or None
        if txt:
            txt = txt.strip()
        if txt and len(txt) >= MIN_LENGTH:
            _log_step(url, "trafilatura", "success", content_len=len(txt))
            soup_tmp = bs4.BeautifulSoup(html, "lxml")
            pub_dt = _recover_published_date(html, soup_tmp)
            if pub_dt:
                return txt, pub_dt
            _log_step(url, "date", "failed", reason="missing after trafilatura")
        else:
            _log_step(url, "trafilatura", "failed", reason="empty or short")
    except Exception as e:
        _log_step(url, "trafilatura", "failed", reason=f"error: {e}")

    # ② Readability-lxml
    try:
        doc = RDoc(html)
        sum_html = doc.summary()
        soup = bs4.BeautifulSoup(sum_html or "", "lxml")
        parts = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        txt = "\n".join([s for s in parts if s])
        if txt and len(txt) >= MIN_LENGTH:
            _log_step(url, "readability", "success", content_len=len(txt))
            soup_tmp = bs4.BeautifulSoup(html, "lxml")
            pub_dt = _recover_published_date(html, soup_tmp)
            if pub_dt:
                return txt, pub_dt
            _log_step(url, "date", "failed", reason="missing after readability")
        else:
            _log_step(url, "readability", "failed", reason="empty or short")
    except Exception as e:
        _log_step(url, "readability", "failed", reason=f"error: {e}")

    # ③ Newspaper3k
    try:
        art = Article(url)
        art.set_html(html)
        art.parse()
        body = (art.text or "").strip()
        if body and len(body) >= MIN_LENGTH:
            _log_step(url, "newspaper3k", "success", content_len=len(body))
            soup_tmp = bs4.BeautifulSoup(html, "lxml")
            pub_dt = _recover_published_date(html, soup_tmp)
            if pub_dt:
                return body, pub_dt
            _log_step(url, "date", "failed", reason="missing after newspaper3k")
        else:
            _log_step(url, "newspaper3k", "failed", reason="empty or short")
    except Exception as e:
        _log_step(url, "newspaper3k", "failed", reason=f"error: {e}")

    # ④ LLM（Playwright で描画したページのみ）
    if _LAST_FETCH_WAS_BROWSER and LLM_ENABLE:
        llm_body, llm_dt = _try_llm_extract(url, html)
        if llm_body and len(llm_body) >= MIN_LENGTH and (llm_dt is not None):
            return llm_body, llm_dt

    return None, None


# シグナル語が現れた時点で本文を切る
 # 本文の切り出しやノイズ除去用のシグナル語・正規表現
SENTINELS = (
    "## IMAGE INFO", "## AUDIO INFO", "## NEWS INFO", "## GALLERY", "## MORE LIKE THIS", "About ",
    "Share:", "PUBLIC DOMAIN", "Tags:",
    "相关新闻", "相关阅读"
)

AUTHOR_REGEX    = re.compile(r"^(By|BY|by)\s+.+$", re.M)
COPYRIGHT_REGEX = re.compile(r"©.*?$", re.M)

def postprocess(text: str) -> str:
    # 本文テキストから著者・コピーライト・ノイズ等を除去し整形する関数
    text = AUTHOR_REGEX.sub("", text)
    text = COPYRIGHT_REGEX.sub("", text)
    text = text.replace("see less|", "")
    # Cut everything after an * INFO header (AUDIO/NEWS/IMAGE) if present
    m = re.search(r"\b(?:AUDIO|NEWS|IMAGE)\s*INFO\b", text, re.I)
    if m:
        text = text[:m.start()]
    for s in SENTINELS:
        idx = text.find(s)
        if idx != -1:
            text = text[:idx]
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()

# --- Ensure result always contains 'url' and minimum fields (even on failures) ---
def _ensure_min_fields(url: str, rec: dict | None) -> dict:
    # 結果レコードに必須フィールドを保証し、正規化する関数
    if not isinstance(rec, dict):
        rec = {}
    # always set 'url'
    rec.setdefault("url", url)
    # normalize text fields
    for k in ("title", "content"):
        v = rec.get(k)
        if v is None:
            rec[k] = ""
        elif isinstance(v, str):
            rec[k] = v.strip()
    # normalize 'published' -> ISO string or None
    pub = rec.get("published")
    if isinstance(pub, datetime.datetime):
        try:
            rec["published"] = pub.isoformat()
        except Exception:
            rec["published"] = None
    return rec

def _guard_url_in_result(fn):
    # 例外時でも必ずurlを含む最小レコードを返すデコレータ
    def _wrap(url: str, *args, **kwargs):
        try:
            rec = fn(url, *args, **kwargs)
        except Exception as e:
            # 例外時でも必ず 'url' を含む最小レコードを返す
            _log_step(url, "scrape_one", "fail", reason=str(e)[:200])
            rec = {"title": "", "content": "本文を抽出できませんでした。"}
        return _ensure_min_fields(url, rec)
    return _wrap

@_guard_url_in_result
def scrape_one(url: str) -> dict:
    # 指定URLのページをスクレイピングし、本文・タイトル・公開日等を抽出するメイン関数
    global _LAST_FINAL_URL
    import logging
    logger = logging.getLogger("scraper")
    parsed  = urlparse(url)
    domain  = parsed.netloc.replace("www.", "")
    path    = parsed.path
    base_domain = tldextract.extract(url).registered_domain or domain

    logger.info("Scrape start: %s (domain=%s, path=%s, base_domain=%s)", url, domain, path, base_domain)
    logger.info("記事のクローニングを開始しました: %s", url)
    # Step 0: RSS/Atom full‑text attempt (site‑wide). If success, skip HTML pipeline.
    rss_article = _try_rss_for_url(url)
    if rss_article:
        return rss_article

    html_str = None
    r_headers = {}
    last_exc = None
    last_status = None

        # HTTPリクエストでHTMLを取得（20秒タイムアウト）。失敗時はPlaywrightへフォールバック

    local_headers = dict(HEADERS)
    if domain == "france24.com":
        local_headers.update({
            "Accept-Language": "en-US,en;q=0.9,fr;q=0.7,ja;q=0.6",
            "Referer": "https://www.france24.com/en/",
        })
    if domain == "shobserver.com":
        local_headers.update({
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.6,ja;q=0.5"
        })
    if domain.endswith("defence.gov.au") or domain.endswith("minister.defence.gov.au") or base_domain == "defence.gov.au":
        local_headers.update({
            "Accept-Language": "en-AU,en;q=0.9"
        })

    # One-shot HTTP: 20s read timeout, then fall back to Playwright
    try:
        r = requests.get(url, headers=local_headers, timeout=(10, 20))  # (connect_timeout, read_timeout)
        last_status = r.status_code
        if r.status_code == 403:
            print(f"[HTTP] 403 from {domain} (read_to=20s)")
            # fall through to Playwright fallback below
        else:
            r.raise_for_status()
            html_str = _decode_response(r, domain)
            r_headers = r.headers
    except Exception as e:
        print("[HTTP] fetch error (read_to=20s):", e)
        last_exc = e

    # Detect SPA shell for shobserver.com and force browser render
        # shobserver.comのSPAシェル検出時は必ずPlaywrightで再取得
    if domain == "shobserver.com" and html_str:
        try:
            # Mustache-like placeholders or offline message indicate client-side rendering not executed
            if re.search(r"\{\{[^}]+\}\}", html_str) or "此文章不存在或已下线" in html_str:
                print("[HTTP] shobserver: template shell detected → forcing Playwright fallback")
                html_str = None  # trigger the Playwright branch below
        except Exception:
            pass

    if (
        not html_str and (
            domain in BROWSER_FALLBACK_DOMAINS or
            base_domain in BROWSER_FALLBACK_DOMAINS or
            isinstance(last_exc, (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout)) or
            last_status in {403, 429, 503}
        )
    ):
            # PlaywrightによるブラウザレンダリングでHTMLを取得（WAF/タイムアウト/SPA等）
        print(f"[BROWSER] Trying Playwright for {domain} (reason: fallback set, timeout, or HTTP {last_status})")
        html_str = _browser_fetch(url)

    if html_str:
        logger.info("記事の取得に成功しました: %s (len=%d)", url, len(html_str))

        # HTML取得失敗時はエラー情報を返す

    if not html_str:
        return {"url": url, "title": "FETCH ERROR", "published": None, "content": "Timeout or WAF/JS block. Try Playwright fallback: pip install playwright && playwright install chromium"}

    soup = bs4.BeautifulSoup(html_str, "html.parser")
    # --- Title selection (structure‑first & brand‑stripped) ---
        # タイトル抽出（構造優先・ブランド除去）
    title = _find_title_generic(soup, domain) or "NO TITLE"

    # Trim DVIDS prefixes if present (these appear in <title> sometimes)
        # DVIDS系タイトルの接頭辞を除去
    for prefix in [
        "DVIDS - Video - ",
        "DVIDS - Images - ",
        "DVIDS - News - ",
        "DVIDS - Audio - "
    ]:
        if title.startswith(prefix):
            title = title[len(prefix):].lstrip(" -|–—")
            break

    # Domain‑specific improvements (keep existing special cases)
        # ドメインごとのタイトル抽出補助
    if domain == "vietnam.vn" and (title == "NO TITLE" or "Vietnam.vn" in title):
        t2 = _vietnam_vn_find_title(soup)
        if t2:
            title = t2
    if domain == "rfi.fr":
        t2 = _rfi_find_title(soup)
        if t2:
            title = t2
    if domain == "ct.moreover.com":
        t2 = _moreover_find_title(soup)
        if t2:
            title = t2
    if domain == "idahostatesman.com":
        t2 = _idahostatesman_find_title(soup)
        if t2:
            title = t2
    if domain == "cankaoxiaoxi.com":
        t2 = _cankaoxiaoxi_find_title(soup)
        if t2:
            title = t2
    if domain == "shobserver.com":
        t2 = _shobserver_find_title(soup)
        if t2:
            title = t2

    # Final brand strip & whitespace normalize (belt‑and‑suspenders)
        # ブランド名除去と空白正規化
    title = _strip_site_brand_from_title(title, _site_name_tokens(domain, soup)) if title else "NO TITLE"

    extractor = find_extractor(domain, path)
    content: Optional[str] = None
    pub_dt: Optional[datetime.datetime] = None
    used = None

        # ドメインごとの抽出器があれば優先して利用

    if extractor:
        try:
            content, pub_dt = extractor.func(html_str)
            used = extractor.name
        except Exception as e:
            print(f"[EXTRACTOR ERROR] {extractor.name}:", e)
            content = pub_dt = None

    if (not content or len(content) < MIN_LENGTH) and not (extractor and extractor.no_fallback):
        # 抽出器で十分な本文が得られない場合は汎用カスケードで再抽出
        try:
            fb_text, fb_dt = fallback_extract(url, html_str)
            if not used:
                used = "fallback"
            content = content or fb_text
            pub_dt = pub_dt or fb_dt
        except Exception as e:
            print("[FALLBACK ERROR]", e)

    # Domain-scoped hard fallback for SPA shell pages
        # cankaoxiaoxi.comのSPAシェルはPlaywrightで再取得し再抽出
    if domain == "cankaoxiaoxi.com" and (not content or len(content) < MIN_LENGTH):
        print("[FALLBACK] cankaoxiaoxi.com: trying Playwright hard fallback…")
        try:
            html_rendered = _browser_fetch(url)
        except Exception:
            html_rendered = None
        print(f"[FALLBACK] got rendered HTML: {len(html_rendered) if html_rendered else 0}")
        if html_rendered:
            try:
                # Re-run the domain extractor on rendered HTML
                ext = find_extractor(domain, path)
                if ext:
                    c2, d2 = ext.func(html_rendered)
                    if not c2:
                        print("[CXN] extractor returned empty content for rendered HTML")
                    if c2 and len(c2) > len(content or ""):
                        content = c2
                    if (not pub_dt) and d2:
                        pub_dt = d2
                # Refresh title from rendered DOM (always override the shell title)
                soup2 = bs4.BeautifulSoup(html_rendered, "html.parser")
                t2 = _cankaoxiaoxi_find_title(soup2)
                if t2 and t2.strip():
                    title = t2.strip()
                print(f"[TITLE] cankaoxiaoxi.com: title refreshed from rendered DOM/meta → {title}")
            except Exception as _:
                pass

    if (domain.endswith("crntt.com") or domain.endswith("bj.crntt.com")) and (not title or title == "NO TITLE" or len(title) < 4):
        # crntt.com系でタイトルが不十分な場合は補助関数で再抽出
        t2 = _crntt_find_title(soup)
        if t2:
            title = t2

    if domain == "cankaoxiaoxi.com" and not pub_dt:
        # cankaoxiaoxi.comのURLフラグメントから日付を抽出
        # The router fragment may carry a timestamp like 2025-07-24 15:55
        frag = parsed.fragment or ""
        m = re.search(r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?", frag)
        if not m:
            m = re.search(r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2})?", frag)
        if m:
            pub_dt = _parse_date(m.group(0))

    last_mod = _parse_date(r_headers.get("Last-Modified") if r_headers else None)
    pub_dt = pub_dt or last_mod

        # 本文の後処理（ノイズ除去・改行除去）

    if content:
        content = postprocess(content)
        if content:
            content = content.replace("\\n", "").replace("\n", "")
    else:
        content = "本文を抽出できませんでした。"

    # Log clone success if content is present and not the extraction failure message
        # 本文抽出成功時のログ出力
    if content and content != "本文を抽出できませんでした。":
        logger.info("記事のクローニングに成功しました: %s (content_len=%d)", url, len(content))

    if content and title and content.startswith(title):
        # 本文先頭にタイトルが重複している場合は除去
        content = content[len(title):].lstrip(" :\n")

    return {
        "url": (_LAST_FINAL_URL or url),   # ← ここを url から置換
        "title": title,
        "published": pub_dt.isoformat() if isinstance(pub_dt, datetime.datetime) else None,
        "content": content,
    }

# def main(urls: list[str]) -> None:
    results = []
    total = len(urls)
    for idx, u in enumerate(urls, 1):
        try:
            results.append(scrape_one(u))
        except Exception as e:
            results.append({"url": u, "title": "SCRAPE ERROR", "published": None, "content": f"Scraping failed: {e}"})
        # ログ出力: 件数形式の日本語
        import logging
        logger = logging.getLogger("scraper")
        logger.info("%d件目／合計 %d 件が完了しました。", idx, total)
        print(f"{idx}件目／合計 {total} 件が完了しました。")
    for art in results:
        print(f"=== {art['title']} ===")
        print(f"URL: {art['url']}")
        print(f"Published: {art['published']}\n")
        print(art["content"][:1000], "...\n---\n")
    outdir = Path("output")
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / "articles.json"
    with outpath.open("w", encoding="utf-8") as fp:
        json.dump(results, fp, ensure_ascii=False, indent=2)

# ======================
# Registry bindings here
# ======================

# 各ドメイン・パスごとの抽出器をレジストリに登録する設定ブロック

register_extractor(name="defensenews",        domain="defensenews.com", func=extract_defensenews, priority=50)

register_extractor(name="dvids:image",        domain="dvidshub.net",   func=extract_dvids_image,  path_prefix="/image/", priority=90)
register_extractor(name="dvids:video",        domain="dvidshub.net",   func=extract_dvids_video,  path_prefix="/video/", priority=90)
register_extractor(name="dvids:audio",        domain="dvidshub.net",   func=extract_dvids_audio,  path_prefix="/audio/", priority=90)
register_extractor(name="dvids:news",         domain="dvidshub.net",   func=extract_dvids_news,   path_prefix="/news/",  priority=90)
register_extractor(name="dvids:generic",      domain="dvidshub.net",   func=extract_dvids_generic,                          priority=40)

register_extractor(name="defense:igphoto",    domain="defense.gov",     func=extract_defense_igphoto, path_prefix="/Multimedia/Photos/igphoto/", priority=95, no_fallback=True)
register_extractor(name="defense:generic",    domain="defense.gov",     func=extract_dvids_generic,   priority=40)

register_extractor(name="crntt",              domain="crntt.com",       func=extract_crntt, priority=60)
register_extractor(name="crntt:bj",       domain="bj.crntt.com",    func=extract_crntt, priority=60)

register_extractor(name="vietnam.vn",         domain="vietnam.vn",      func=extract_vietnam_vn, priority=60)
register_extractor(name="rfi",                domain="rfi.fr",          func=extract_rfi, priority=60)
register_extractor(name="france24",           domain="france24.com",    func=extract_france24, priority=60)
register_extractor(name="moreover:ct",       domain="ct.moreover.com", func=extract_moreover_ct, priority=60)
register_extractor(name="idahostatesman",    domain="idahostatesman.com", func=extract_idahostatesman, priority=60)
register_extractor(name="cankaoxiaoxi",     domain="cankaoxiaoxi.com", func=extract_cankaoxiaoxi, priority=60)
register_extractor(name="shobserver:detail", domain="shobserver.com", func=extract_shobserver, path_prefix="/staticsg/res/html/web/newsDetail.html", priority=80)

# ==============
# CLI / main
# ==============

# コマンドライン引数からURLリストを取得する補助関数

def _load_urls_from_args(argv: List[str]) -> List[str]:
    urls: List[str] = []
    if len(argv) <= 1:
        return ["https://www.dvidshub.net/news/499244/multinational-forces-kick-off-kamandag-9-philippines"]
    for arg in argv[1:]:
        if arg.startswith(("http://", "https://")):
            urls.append(arg.strip())
        else:
            try:
                with open(arg, encoding="utf-8") as f:
                    urls.extend([ln.strip() for ln in f if ln.strip()])
            except FileNotFoundError:
                print(f"[WARN] Not a URL and file not found: {arg}")
    return urls

def main(urls: List[str]) -> None:
    # 複数URLをまとめてスクレイピングし、結果をファイル出力するメイン関数
    results = []
    total = len(urls)
    for idx, u in enumerate(urls, 1):
        try:
            res = scrape_one(u)
            results.append(res)
        except Exception as e:
            results.append({"url": u, "title": "SCRAPE ERROR", "published": None, "content": f"Scraping failed: {e}"})
        # ログ出力: 件数形式の日本語
        import logging
        logger = logging.getLogger("scraper")
        logger.info("%d件目／合計 %d 件が完了しました。", idx, total)
    for art in results:
        print(f"=== {art['title']} ===")
        print(f"URL: {art['url']}")
        print(f"Published: {art['published']}\n")
        print(art["content"][:1000], "...\n---\n")
    outdir = Path("output")
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / "articles.json"
    with outpath.open("w", encoding="utf-8") as fp:
        json.dump(results, fp, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    # スクリプトが直接実行された場合の処理（エントリーポイント）
    # コマンドライン実行時のエントリーポイント
    urls = _load_urls_from_args(sys.argv)
    main(urls)


# =========================
# Public return-based API (exposed for main.py)
# =========================

# main.py等から呼び出せる公開API群
from typing import Optional as _Optional
import datetime as _dt

def get_content(url: str) -> str:
    # 指定URLの記事本文テキストのみを返すAPI関数（失敗時は空文字）
    """Return plain article text for the given URL (empty string on failure)."""
    try:
        res = scrape_one(url)
    except Exception:
        res = {"content": "", "published": None}
    content = (res.get("content") or "").strip()
    # Map the generic failure message to empty string for pipeline semantics
    if content == "本文を抽出できませんでした。":
        content = ""
    return content

def get_article_text(url: str) -> str:  # compatibility alias
    # get_contentの互換エイリアス
    return get_content(url)

def extract(url: str) -> dict:
    # 指定URLの記事本文と公開日を辞書で返す互換API関数
    """Compatibility API: return dict with content and published ISO string."""
    try:
        res = scrape_one(url)
    except Exception:
        res = {"content": "", "published": None}
    content = (res.get("content") or "").strip()
    if content == "本文を抽出できませんでした。":
        content = ""
    published = res.get("published")
    return {
        "content": content,
        "published": published if isinstance(published, str) else (published.isoformat() if isinstance(published, _dt.datetime) else None),
    }