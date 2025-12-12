# sprinklr_client.py
import os, json, time, tempfile, re
from typing import Tuple, Dict, Any
import requests

import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler




 # .envファイルの読み込みと環境変数の設定
from dotenv import load_dotenv, find_dotenv  # [ENV]
_ = load_dotenv(find_dotenv(filename=os.getenv("ENV_FILE", ".env"), usecwd=True))  # [ENV]

 # GCSクライアントの利用可否を判定
try:
    from google.cloud import storage
    from google.api_core.exceptions import NotFound, PreconditionFailed
    _GCS_AVAILABLE = True
except Exception:
    _GCS_AVAILABLE = False

# ===== 設定 =====
ENV = os.getenv("SPRINKLR_ENV")  # [ENV]
TOK_PATH = os.getenv("SPRINKLR_TOKENS_URI")  # [ENV]

# 重要：APIとOAuthのベースURLを分離（必要に応じて上書き可能）
API_BASE   = os.getenv("SPRINKLR_API_BASE",   f"https://api2.sprinklr.com/{ENV}")  # [ENV]
OAUTH_BASE = os.getenv("SPRINKLR_OAUTH_BASE", f"https://api3.sprinklr.com/{ENV}")  # [ENV]

# Keyヘッダには client_id を使う想定
CID  = os.environ["SPRINKLR_CLIENT_ID"]  # [ENV]
CSEC = os.environ["SPRINKLR_CLIENT_SECRET"]  # [ENV]

 # gs:// 形式のURLかどうかを判定する関数
def _is_gs(url: str) -> bool:
    return isinstance(url, str) and url.startswith("gs://")

 # gs:// URLをバケット名とオブジェクト名に分割する関数
def _parse_gs(url: str) -> Tuple[str, str]:
    assert _is_gs(url), f"not a gs:// url: {url}"
    rest = url[5:]
    bucket, _, obj = rest.partition("/")
    return bucket, obj

 # GCSクライアントを生成する関数
def _gcs_client() -> storage.Client:
    if not _GCS_AVAILABLE:
        raise RuntimeError("google-cloud-storage が未インストールです。")
    return storage.Client()

 # ローカルファイルにアトミックに書き込む関数
def _atomic_local_write(path: str, data: str) -> None:
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, prefix=".toktmp-", encoding="utf-8") as tf:
        tf.write(data)
        tmp = tf.name
    os.replace(tmp, path)
    os.chmod(path, 0o600)

 # トークン情報をGCSまたはローカルから読み込む関数
def _load_tokens() -> Dict[str, Any]:
    if _is_gs(TOK_PATH):
        client = _gcs_client()
        bkt, obj = _parse_gs(TOK_PATH)
        blob = client.bucket(bkt).blob(obj)
        try:
            blob.reload()
            gen = blob.generation
            raw = blob.download_as_text(encoding="utf-8")
        except NotFound:
            raise FileNotFoundError(f"Sprinklr tokens not found: {TOK_PATH}")
        t = json.loads(raw)
        t["_gen"] = int(gen) if gen is not None else None
        return t
    else:
        with open(TOK_PATH, encoding="utf-8") as f:
            return json.load(f)

 # トークン情報をGCSまたはローカルに保存する関数
def _save_tokens(t):
    data = dict(t)
    prev_gen = data.pop("_gen", None)
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    if _is_gs(TOK_PATH):
        client = _gcs_client()
        bkt, obj = _parse_gs(TOK_PATH)
        blob = client.bucket(bkt).blob(obj)
        for attempt in range(3):
            try:
                if prev_gen is None:
                    blob.upload_from_string(payload, content_type="application/json; charset=utf-8")
                else:
                    blob.upload_from_string(payload, content_type="application/json; charset=utf-8",
                                           if_generation_match=int(prev_gen))
                blob.reload()
                new_gen = blob.generation
                t["_gen"] = int(new_gen) if new_gen is not None else None
                return
            except PreconditionFailed:
                blob.reload()
                try:
                    latest = json.loads(blob.download_as_text(encoding="utf-8"))
                except NotFound:
                    latest = {}
                prev_gen = blob.generation
                latest.update(data)
                payload = json.dumps(latest, ensure_ascii=False, indent=2)
                time.sleep(0.4 * (attempt + 1))
        raise RuntimeError("Failed to save tokens to GCS due to concurrent updates.")
    else:
        _atomic_local_write(TOK_PATH, payload)

 # リフレッシュトークンを使ってアクセストークンを更新する関数
def _refresh(toks, **kw):
    print("!!_refresh!!")
    print(toks["refresh_token"])
    r = requests.post(
        f"{OAUTH_BASE}/oauth/token",
        headers={"Content-Type":"application/x-www-form-urlencoded"},
        data={
            "grant_type":"refresh_token",
            "client_id":CID,
            "client_secret":CSEC,
            "refresh_token":toks["refresh_token"],
        },
        timeout=60
    )
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        print("===IF SPRINKLR API ERROR ===")
        print("Status:", r.status_code)
        print("URL:", r.url)
        print("Response Text:", r.text)
        try:
            print("ERROR JSON Response:", r.json())
        except Exception:
            print("!!JSON Response: <not json>")
        print(json.dumps(kw.get("json"), indent=2, ensure_ascii=False))
        raise
    # r.raise_for_status()
    new = r.json()
    toks.update(new)  # access_token / refresh_token を上書き
    _save_tokens(toks)
    return toks

def log_sprinklr_payload(payload):
    logging.info({
        "message": "Sprinklr Request Payload",
        "payload": payload,
        "operation": {
            "id": payload.get("reportName") or payload.get("reportId") or "unknown",
            "producer": "chronoro-sprinklr-client"
        }
    })


 # Sprinklr APIを呼び出す共通関数
def spr(method: str, path: str, **kw) -> Any:
    """
    業務API呼び出しの共通関数。
    - Authorization: Bearer <access_token>（GCSから読込）
    - Key: <client_id> を送付
    - 401/403 の場合は自動refreshして1回だけ再試行
    """
    toks = _load_tokens()
    headers = kw.pop("headers", {})
    headers.update({
        "Authorization": f"Bearer {toks['access_token']}",
        "Key": CID,
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    url = API_BASE + path
    
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client)
    logger = logging.getLogger("sprinklr")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    

    if "json" in kw:
        try:
            log_sprinklr_payload(kw.get("json"))
            # print(json.dumps({"message": "Sprinklr Request Payload", "payload": kw["json"]}, ensure_ascii=False))
        except Exception:
            print("2.[WARN] !!Could not print JSON payload")
                
    r = requests.request(method, url, headers=headers, timeout=120, **kw)
    print("====1.request_key====")
    print(method)
    print(url)
    print(headers)
    print('~~~~~~~~~~~~~~~')
    print(r.status_code)
    print(r.text)
    print("====   ====")



    
    if r.status_code >= 400:
        try:
            print("3.JSON Response:", r.json())
        except:
            print("4.JSON Response: <not json>")
        print("5.===== SENT PAYLOAD =====")
        try:
            print(json.dumps(kw.get("json"), indent=2, ensure_ascii=False))
        except:
            print("6.<payload parse error>")
            print("Status:", r.status_code)
            print("URL:", r.url)
            print("Response Text:", r.text)
        print("=========================")
    
    
    
        
    if r.status_code in (401, 403) and "refresh_token" in toks:
        print("!!Access Info !!")
        print(os.getenv("SPRINKLR_CLIENT_ID"))
        print(os.getenv("SPRINKLR_CLIENT_SECRET"))
        print(os.getenv("SPRINKLR_TOKENS_URI"))
        print(TOK_PATH)
        print("!!Access Info !!")
            
        toks = _refresh(toks)
        headers["Authorization"] = f"Bearer {toks['access_token']}"
        r = requests.request(method, url, headers=headers, timeout=120, **kw)
        
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            print("===~~ SPRINKLR API ERROR DETECTED ~~===")
            print("Status:", r.status_code)
            print("URL:", r.url)
            print("Response Text:", r.text)
            print("=========================")
            raise  # ← 再スロー
        #  ここまで追加
        
        
    r.raise_for_status()
    # レポート系は text を返す構成もあるが、ここでは JSON 想定
    try:
        return r.json()
    except Exception:
        return r.text
    
