#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# 既定：ペイロードに書かれた startTime/endTime をそのまま使う
data_from_sprinklr = fetch_rows_simple(
    payload_path="payloads/payload_bouei_hakusho.json",
)

# 明示的に期間を上書きしたい場合（use_payload_time=False）
data_from_sprinklr = fetch_rows_simple(
    payload_path="payloads/payload_bouei_hakusho.json",
    start="2025-07-15T15:00:00",
    end="2025-07-20T15:00:00",
    use_payload_time=False,
)

"""
import os
import json
import time
import argparse
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from typing import Optional

from sprinklr_client import spr  # ユーザ環境のSprinklrクライアント

PATH = "/api/v2/reports/query"   # V2 エンドポイント  # [ENV hardcoded]　守秘内容でないので修正しない


# -----------------------------
# 基本ユーティリティ
# -----------------------------
def load_payload(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def override_time_range(payload: dict, start_ms: Optional[int], end_ms: Optional[int]) -> dict:
    p = deepcopy(payload)
    if start_ms is not None:
        p["startTime"] = start_ms
    if end_ms is not None:
        p["endTime"] = end_ms
    return p


def jst_to_epoch_ms(s: str) -> int:
    """
    'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS', または
    'YYYY-MM-DDTHH:MM[:SS]' 形式の JST 文字列を epoch(ms) に変換する。
    """
    if not s:
        return None  # type: ignore
    s = s.strip().replace("T", " ")
    jst = timezone(timedelta(hours=9))
    fmts = ["%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"]
    last_err = None
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            dt = dt.replace(tzinfo=jst)
            return int(dt.timestamp() * 1000)
        except Exception as e:
            last_err = e
            continue
    raise ValueError(
        f"Invalid JST datetime format: {s}. Expected one of: "
        "YYYY-MM-DD[ HH:MM[:SS]] or YYYY-MM-DDTHH:MM[:SS]"
    )


def set_page(payload: dict, page: int, page_size: Optional[int] = None) -> dict:
    p = deepcopy(payload)
    p["page"] = page
    if page_size is not None:
        p["pageSize"] = page_size
    return p


def run_once(payload: dict):
    resp = spr("POST", PATH, json=payload)
    return resp


def run_paged(payload: dict, max_pages: int = 1, sleep_sec: float = 0.5):
    """
    固定ページ数だけ回す簡易版（必要な場合のみ使用）
    """
    page = int(payload.get("page", 0))
    page_size = int(payload.get("pageSize", 20))
    all_rows = []
    for i in range(max_pages):
        req = set_page(payload, page + i, page_size)
        resp = run_once(req)
        rows = None
        for key in ("data", "rows", "tableData", "items", "result"):
            if isinstance(resp, dict) and key in resp:
                rows = resp[key]
                break
        if rows is None:
            all_rows.append(resp)
        else:
            if not rows:
                break
            if isinstance(rows, list):
                all_rows.extend(rows)
            else:
                all_rows.append(rows)
        time.sleep(sleep_sec)
    return all_rows


def run_all(payload: dict, sleep_sec: float = 0.5, max_pages: Optional[int] = None):
    """
    hasMore が False になるまで、または返却件数が pageSize を下回るまで全ページ取得。
    max_pages が指定された場合は、そのページ数で上限。
    戻り値は各ページの生レスポンスのリスト。
    """
    start_page = int(payload.get("page", 0))
    page_size = int(payload.get("pageSize", 20))
    all_resps = []
    i = 0
    while True:
        req = set_page(payload, start_page + i, page_size)
        resp = run_once(req)
        all_resps.append(resp)

        # 停止条件1: hasMore
        has_more = None
        if isinstance(resp, dict):
            d = resp.get("data")
            if isinstance(d, dict) and "hasMore" in d:
                has_more = bool(d.get("hasMore"))

        if max_pages is not None and (i + 1) >= max_pages:
            break
        if has_more is not None:
            if not has_more:
                break
        else:
            # 停止条件2: 返却行が pageSize 未満
            rows = None
            if isinstance(resp, dict):
                d = resp.get("data")
                if isinstance(d, dict) and isinstance(d.get("data"), list):
                    rows = d.get("data")
                else:
                    for key in ("rows", "tableData", "items", "result"):
                        if isinstance(resp.get(key), list):
                            rows = resp.get(key)
                            break
            if rows is None or len(rows) < page_size:
                break

        i += 1
        time.sleep(sleep_sec)

    return all_resps


def sanitize_payload(raw: dict) -> dict:
    """UI由来の冗長フィールドを削除し、API向けの最小形に整える"""
    keep_top = {
        "report", "reportingEngine", "startTime", "endTime", "timeZone",
        "page", "pageSize", "filters", "groupBys", "projections", "sorts",
        "jsonResponse"
    }
    p = {k: raw.get(k) for k in keep_top if k in raw}
    p["jsonResponse"] = True

    # filters
    raw_filters = raw.get("filters") or []
    flt = []
    for f in raw_filters:
        if not isinstance(f, dict):
            continue
        nf = {k: v for k, v in f.items() if k in ("dimensionName", "filterType", "values")}
        if nf.get("values"):
            flt.append(nf)
    p["filters"] = flt

    # groupBys
    raw_gbs = raw.get("groupBys") or []
    gbs = []
    for g in raw_gbs:
        if not isinstance(g, dict):
            continue
        ng = {k: v for k, v in g.items() if k in ("heading", "dimensionName", "groupType")}
        gbs.append(ng)
    p["groupBys"] = gbs

    # projections
    raw_prj = raw.get("projections") or []
    prj = []
    for pr in raw_prj:
        if not isinstance(pr, dict):
            continue
        np = {k: v for k, v in pr.items() if k in ("heading", "measurementName", "aggregateFunction")}
        prj.append(np)
    p["projections"] = prj

    # sorts
    raw_sorts = raw.get("sorts") or []
    sorts = []
    for s in raw_sorts:
        if not isinstance(s, dict):
            continue
        ns = {k: v for k, v in s.items() if k in ("heading", "order")}
        if ns:
            sorts.append(ns)
    if sorts:
        p["sorts"] = sorts

    return p


def override_query(payload: dict, query: Optional[str]) -> dict:
    if not query:
        return payload
    p = deepcopy(payload)
    new_filters = []
    found = False
    for f in p.get("filters", []):
        if f.get("dimensionName") == "QUERY":
            new_filters.append({"dimensionName": "QUERY", "filterType": "IN", "values": [query]})
            found = True
        else:
            new_filters.append(f)
    if not found:
        new_filters.append({"dimensionName": "QUERY", "filterType": "IN", "values": [query]})
    p["filters"] = new_filters
    return p


def add_stream_fields(payload: dict) -> dict:
    """ストリーム用の代表フィールドを付与（必要時）"""
    p = deepcopy(payload)
    p["streamRequestInfo"] = {
        "streamFields": [
            {"name": "PERMALINK"},
            {"name": "WEB_TITLE_ES_MESSAGE_ID"},
            {"name": "MEDIA_SOURCE_NAME"},
        ]
    }
    return p


def parse_rows_from_response(resp: dict) -> list[dict]:
    """
    Sprinklr reports/query の返却をテーブル行の配列に正規化する。
    代表的なパターン:
      - {"data": {"data": [ {<col>:<val>, ...}, ... ], "hasMore": bool}, "errors": []}
      - {"rows": [...]}
    """
    if isinstance(resp, dict):
        if "data" in resp and isinstance(resp["data"], dict) and "data" in resp["data"]:
            rows = resp["data"]["data"]
            if isinstance(rows, list):
                return rows
        for k in ("rows", "tableData", "items", "result"):
            if k in resp and isinstance(resp[k], list):
                return resp[k]
    # 既知フォーマット以外はそのまま 1 要素として返す
    return [resp]


def gather_rows_from_data(data_obj) -> list[dict]:
    """run_all / batching の返却（list[resp] or resp）から行配列を集約"""
    if isinstance(data_obj, list):
        rows = []
        for chunk in data_obj:
            rows.extend(parse_rows_from_response(chunk))
        return rows
    return parse_rows_from_response(data_obj)


def _extract_row_key(row: dict) -> Optional[str]:
    """
    重複判定用のキーを抽出。
    優先度: ES_MESSAGE_ID.universalMessageId -> snMsgId -> PERMALINK -> PERMALINK_1
    フォールバック: WEB_TITLE + MEDIA_SOURCE_NAME の合成キー。
    """
    # ES_MESSAGE_ID オブジェクト（UIエクスポートだと suffix 付きのことが多い）
    for k in ("ES_MESSAGE_ID_0", "ES_MESSAGE_ID"):
        v = row.get(k)
        if isinstance(v, dict):
            if v.get("universalMessageId"):
                return str(v["universalMessageId"])
            if v.get("snMsgId"):
                return str(v["snMsgId"])
    # PERMALINK
    for k in ("PERMALINK", "PERMALINK_1"):
        if k in row and row[k]:
            return str(row[k])
    # タイトル+媒体でのフォールバック
    title = row.get("WEB_TITLE_ES_MESSAGE_ID_2") or row.get("WEB_TITLE_ES_MESSAGE_ID")
    source = row.get("MEDIA_SOURCE_NAME_3") or row.get("MEDIA_SOURCE_NAME")
    if title and source:
        return f"{title}||{source}"
    return None


def dedup_rows(rows: list[dict]) -> list[dict]:
    """抽出キーで重複排除"""
    seen = set()
    uniq = []
    for r in rows:
        key = _extract_row_key(r) or json.dumps(r, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    return uniq


def ensure_groupbys(payload: dict, add_es_id: bool = False) -> dict:
    """
    groupBys が空/未設定なら PERMALINK を最低限追加。
    add_es_id=True の場合は ES_MESSAGE_ID も追加。
    """
    p = deepcopy(payload)
    gbs = p.get("groupBys") or []
    names = {g.get("dimensionName") for g in gbs if isinstance(g, dict)}

    def _mk(dim, head=None):
        return {"dimensionName": dim, "groupType": "FIELD", "heading": head or dim}

    changed = False
    if "PERMALINK" not in names:
        gbs.append(_mk("PERMALINK"))
        changed = True
    if add_es_id and "ES_MESSAGE_ID" not in names:
        gbs.append(_mk("ES_MESSAGE_ID"))
        changed = True
    if changed:
        p["groupBys"] = gbs
    return p


def _get_topic_ids(payload: dict) -> list[str]:
    for f in payload.get("filters", []):
        if f.get("dimensionName") == "TOPIC_IDS":
            vals = f.get("values", [])
            return [str(v) for v in vals if v]
    return []


def _set_topic_ids(payload: dict, ids: list[str]) -> dict:
    p = deepcopy(payload)
    new_filters = []
    found = False
    for f in p.get("filters", []):
        if f.get("dimensionName") == "TOPIC_IDS":
            if ids:
                new_filters.append({"dimensionName": "TOPIC_IDS", "filterType": "IN", "values": ids})
            # ids が空なら TOPIC_IDS フィルタ自体を外す
            found = True
        else:
            new_filters.append(f)
    if not found and ids:
        new_filters.append({"dimensionName": "TOPIC_IDS", "filterType": "IN", "values": ids})
    p["filters"] = new_filters
    return p


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def run_with_topic_batches(payload: dict, max_ids: int, sleep_sec: float = 0.5):
    """
    TOPIC_IDS が max_ids を超える場合に分割して複数回叩き、結果を結合する。
    それぞれのバッチは run_all で全ページ取得する。
    """
    ids = _get_topic_ids(payload)
    if not ids or len(ids) <= max_ids:
        return run_all(payload, sleep_sec=sleep_sec)
    print(f"[INFO] Splitting TOPIC_IDS: {len(ids)} -> batches of {max_ids}")
    combined = []
    for idx, batch in enumerate(_chunks(ids, max_ids), 1):
        p2 = _set_topic_ids(payload, batch)
        print(f"[INFO] Batch {idx}: {len(batch)} ids")
        part = run_all(p2, sleep_sec=sleep_sec)
        if isinstance(part, list):
            combined.extend(part)
        else:
            combined.append(part)
        time.sleep(sleep_sec)
    return combined


# -----------------------------
# 関数化エントリポイント
# -----------------------------
def fetch_sprinklr_rows(
    start: Optional[str],
    end: Optional[str],
    payload_path: str,
    query: Optional[str] = None,
    *,
    override_time: bool = False,  # 既定は「上書きしない」
    topic_batch_size: int = 10,
    sleep_sec: float = 0.5,
    max_pages: Optional[int] = None,   # 使わない場合は run_all の既定で自動停止
    add_es_id: bool = True,
    debug: bool = False,
) -> list[dict]:
    """
    指定ペイロードを用いて Sprinklr /api/v2/reports/query を実行し、
    JST文字列の start/end を（override_time=True の場合に）適用し、重複排除した行配列を返す。
    既定値では override_time=False のため、ペイロードの startTime/endTime を尊重します。

    Returns:
        list[dict]: 正規化＆重複排除済みの行。代表キーは ES_MESSAGE_ID / PERMALINK / (TITLE||SOURCE)
    """
    payload = load_payload(payload_path)

    # 時間範囲: JST 文字列 → epoch(ms)（override_time=True の場合にのみ上書き）
    if override_time:
        start_ms = jst_to_epoch_ms(start) if start else None
        end_ms = jst_to_epoch_ms(end) if end else None
        if start_ms is not None or end_ms is not None:
            payload = override_time_range(payload, start_ms, end_ms)

    if query:
        payload = override_query(payload, query)

    # API最小形へ整形 + 必要フィールドの保証
    payload = sanitize_payload(payload)
    payload = ensure_groupbys(payload, add_es_id=add_es_id)
    payload = add_stream_fields(payload)

    if debug:
        print("[DEBUG] outgoing payload:")
        print(json.dumps(payload, ensure_ascii=False, indent=2)[:4000])

    # 実行（TOPIC_IDS が多い場合は分割）
    try:
        ids = _get_topic_ids(payload)
        if ids and len(ids) > topic_batch_size:
            data = run_with_topic_batches(payload, topic_batch_size, sleep_sec=sleep_sec)
        else:
            data = run_all(payload, sleep_sec=sleep_sec, max_pages=max_pages)
    except Exception as e:
        import requests as _rq
        if isinstance(e, _rq.exceptions.HTTPError) and getattr(e, "response", None) is not None:
            print(f"[ERROR] status={e.response.status_code} {e.response.text[:4000]}")
        else:
            print(f"[ERROR] {e}")
        raise

    # 行へ正規化 + 重複排除して返す
    rows = gather_rows_from_data(data)
    before = len(rows)
    rows = dedup_rows(rows)
    after = len(rows)
    if debug and after != before:
        print(f"[INFO] Dedup rows: {before} -> {after}")
    return rows


# -----------------------------
# CLI エントリポイント
# -----------------------------

def fetch_rows_simple(
    payload_path: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    use_payload_time: bool = True,
) -> list[dict]:
    """
    最小インターフェース: ペイロードのファイルパスを渡し、必要に応じて JST 文字列の start/end を指定。
    既定ではペイロードの startTime/endTime を尊重します（use_payload_time=True）。

    例:
        # 1) ペイロードに記載の startTime/endTime をそのまま使用（推奨・既定）
        rows = fetch_rows_simple(
            payload_path="payloads/all.json",
        )

        # 2) 明示的に期間を指定して上書きしたい場合
        rows = fetch_rows_simple(
            payload_path="payloads/all.json",
            start="2025-09-04T15:00:00",
            end="2025-09-05T15:00:00",
            use_payload_time=False,  # 上書きするので False
        )
    """
    return fetch_sprinklr_rows(
        start=start,
        end=end,
        payload_path=payload_path,
        override_time=not use_payload_time,
        debug=False,
    )


# -----------------------------
# CLI エントリポイント
# -----------------------------
def main():
    # 軽い認証チェック（必須ではない）
    try:
        info = spr("GET", "/api/v2/me")
        who = str(info)[:200]
        print(f"[AUTH] /api/v2/me OK @ {who}")
    except Exception as e:
        print(f"[AUTH][ERROR] Sprinklr 認証に失敗: {e})")

    DEFAULT_PAYLOADS = os.getenv("DEFAULT_PAYLOADS")
    
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", default=DEFAULT_PAYLOADS)  # [ENV hardcoded]
    ap.add_argument("--start", default=None, help='JST日時 "YYYY-MM-DD[THH:MM[:SS]]"')
    ap.add_argument("--end",   default=None, help='JST日時 "YYYY-MM-DD[THH:MM[:SS]]"')
    ap.add_argument("--apply-start-end", action="store_true",
                    help="--start/--end でペイロードの startTime/endTime を上書きする")
    ap.add_argument("--query", default=None, help='QUERY を上書き（例: --query \'site:"defense.gov"\'）')
    ap.add_argument("--to-csv", default="", help="取得行をCSV保存（pandas必須）")
    ap.add_argument("--out",    default="", help="生レスポンスJSONの保存先")
    ap.add_argument("--debug", action="store_true", help="送信前のpayloadを表示")
    args = ap.parse_args()

    rows = fetch_sprinklr_rows(
        start=args.start,
        end=args.end,
        payload_path=args.payload,
        query=args.query,
        override_time=args.apply_start_end,  # 既定は False（上書きしない）
        debug=args.debug,
    )

    # 出力処理
    if args.to_csv:
        try:
            import pandas as pd
        except Exception:
            raise RuntimeError("pandas が必要です。`pip install pandas` を実行してください。")
        import pandas as pd
        df = pd.DataFrame(rows)
        df.to_csv(args.to_csv, index=False, encoding="utf-8")
        print(f"[OK] wrote CSV: {args.to_csv} (rows={len(df)})")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"[OK] wrote: {args.out}")
    elif not args.to_csv:
        print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()