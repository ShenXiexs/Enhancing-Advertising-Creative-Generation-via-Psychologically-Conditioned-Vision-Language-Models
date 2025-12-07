# part1_titles_first.py (with progress logs + save white background image path)
# -*- coding: utf-8 -*-
import argparse
import os, re, io, time, json, base64, chardet, pandas as pd, requests
from PIL import Image
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ======== 基础配置 ========
CSV_PATH        = "白底商品信息类目.csv"   # 需包含: id, ori_title, brand(或 creative_id_brand), image_url, level_one_category_name...
OUT_DIR         = "out_step1"
OUT_XLSX        = os.path.join(OUT_DIR, "step1_titles.xlsx")
OUT_JSONL       = os.path.join(OUT_DIR, "step1_titles.jsonl")

FILENAME_FMT    = "{id}.jpg"   # 生成 out_step1/white_bg/{id}.jpg

SAMPLE_NUM      = 10          # 若需抽样，填整数；否则设为 None
RAND_SEED       = 125

# —— VLM（仅用于标题）——
OLLAMA_HOST     = "http://localhost:11434"
MODEL_TITLE     = "qwen2.5vl:7b"
USE_MODEL_TITLE = True

MAX_SEND_WIDTH  = 1200
JPEG_QUALITY    = 92
REQUEST_TIMEOUT = 180
DEBUG_PRINT     = True


PRINT_EVERY     = 1          # 每多少条打印一次进度（1 = 每条都打）

# —— 标题规范 ——（中文=1；英文字母=0.5；忽略空格与“·”）
MIN_FINAL_VISIBLE_LEN = 8
MAX_FINAL_VISIBLE_LEN = 12
MAX_FINAL_LEN         = 12
MAX_OPTIMIZATION_ROUNDS = 5
MAX_NORMALIZE_TRIES   = MAX_OPTIMIZATION_ROUNDS
ALLOWED_CHARS_RE = re.compile(r'^[A-Za-z\u4e00-\u9fa5\s]+$')

SYSTEM_PROMPT_FOR_TITLE_JSON = (
    "你是电商文案助手。结合【照片】与【标题/品牌】生成一个“品牌+品类”的短标题。要求：\n"
    "1) 标题中只能使用中文或英文品牌，不要中英混排；仅允许中文、英文字母与空格；禁止数字及其它标点；\n"
    "2) 若品牌字段同时包含中文与英文名，标题中只保留中文名；若没有中文名则可保留英文名；\n"
    "3) 理想长度为 8–12 个“汉字等价”字符（中文=1；英文字母=0.5；空格=0.5），目标为 10；\n"
    "4) 标题必须保留品牌，并突出核心品类，避免型号、容量、年份等数字信息；\n"
    "5) 不要促销/活动/夸张词；忽略店铺后缀（官方旗舰店/旗舰店/专卖店/专营店/旗舰）；\n"
    "6) 严格输出 JSON：例如 {\"promo_title\":\"海尔小优移动屏\"} 或 {\"promo_title\":\"Jil Sander单肩包\"}。"
)

SYSTEM_PROMPT_FOR_SIMPLIFY_JSON = (
    "你是电商短标题精简助手。基于给定的【候选标题】进行二次精简：\n"
    "• 仅允许中文、英文字母、空格；禁止数字及其它标点；\n"
    "• 目标长度 8–12 个“汉字等价”，需要时可略短或略长，但最终≤12；\n"
    "• 保留品牌名称与核心品类；\n"
    "严格输出 JSON，如 {\"promo_title\":\"...\"}。"
)

SYSTEM_PROMPT_FOR_EXPAND_JSON = (
    "你是电商短标题补全助手。基于给定的【候选标题】进行适度扩充：\n"
    "• 仅允许中文、英文字母、空格；禁止数字及其它标点；\n"
    "• 目标长度 8–12 个“汉字等价”，尽量靠近 10；\n"
    "• 必须保留品牌名称，结合【原始标题】补足核心品类或关键信息，避免营销词与数字；\n"
    "严格输出 JSON，如 {\"promo_title\":\"...\"}。"
)

# ======== 工具函数 ========
def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate short promo titles (Part1)."
    )
    parser.add_argument(
        "--model",
        choices=["7b", "32b"],
        default="7b",
        help="Specify which qwen2.5vl model size to call via Ollama (default: 7b).",
    )
    parser.add_argument(
        "--csv-path",
        default=CSV_PATH,
        help="Path to the source CSV/XLSX file (default: 白底商品信息类目.csv).",
    )
    parser.add_argument(
        "--out-dir",
        default=OUT_DIR,
        help="Directory to store outputs (default: out_step1).",
    )
    default_sample = SAMPLE_NUM if SAMPLE_NUM is not None else 0
    parser.add_argument(
        "--sample-num",
        type=int,
        default=default_sample,
        help="Number of rows to sample. <=0 means use the full dataset (default: current SAMPLE_NUM setting).",
    )
    parser.add_argument(
        "--rand-seed",
        type=int,
        default=RAND_SEED,
        help="Random seed used when sampling rows (default: 125).",
    )
    return parser.parse_args()


def read_any_table(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    for enc in ("utf-8-sig", "gb18030", "utf-8", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    with open(path, "rb") as f:
        enc = chardet.detect(f.read(65536)).get("encoding") or "utf-8"
    return pd.read_csv(path, encoding=enc, errors="ignore")

def make_http_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.mount("http://", HTTPAdapter(max_retries=retry)); s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def _pil_from_bytes(b: bytes) -> Image.Image:
    im = Image.open(io.BytesIO(b)); im.load(); return im.convert("RGB")

def fetch_image(src: str, sess: requests.Session) -> Image.Image:
    if not src: raise ValueError("empty image source")
    if src.startswith("//"): src = "https:" + src
    if src.startswith("http"):
        r = sess.get(src, timeout=20); r.raise_for_status(); return _pil_from_bytes(r.content)
    with open(src, "rb") as f:
        return _pil_from_bytes(f.read())

def to_b64(img: Image.Image, max_w=MAX_SEND_WIDTH, quality=JPEG_QUALITY) -> str:
    w, h = img.size
    if max_w and w > max_w:
        r = max_w / float(w); img = img.resize((int(w*r), int(h*r)), Image.LANCZOS)
    buf = io.BytesIO(); img.save(buf, "JPEG", quality=quality, optimize=True, subsampling=0)
    return base64.b64encode(buf.getvalue()).decode("ascii")

def save_image_original(img: Image.Image, save_path: str, quality: int = 95):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    img.convert("RGB").save(save_path, "JPEG", quality=quality, optimize=True, subsampling=0)

def vlm_chat_json(model: str, b64_image: str, system_prompt: str, user_text: str,
                  num_predict=80, temperature=0.2):
    payload = {"model": model, "messages": [
        {"role":"system","content":system_prompt},
        {"role":"user","content":user_text,"images":[b64_image]},
    ], "stream": False, "format":"json",
       "options":{"num_predict":num_predict,"temperature":temperature,"top_p":0.9,"repeat_penalty":1.1}}
    r = requests.post(f"{OLLAMA_HOST}/api/chat", json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    raw = (r.json().get("message") or {}).get("content","") or ""
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.I)
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.S | re.I)
    try:
        obj = json.loads(raw)
    except Exception:
        obj = None
    return raw, obj

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def visible_units(s: str) -> float:
    if not s:
        return 0.0
    total = 0.0
    for ch in str(s):
        if ch.isspace():
            total += 0.5
        elif re.fullmatch(r"[\u4e00-\u9fa5]", ch):
            total += 1.0
        else:
            total += 0.5
    return total

def validate_title(raw: str):
    if not raw or not str(raw).strip():
        return False, "empty"
    s = normalize_spaces(str(raw))
    if not ALLOWED_CHARS_RE.fullmatch(s):
        return False, "invalid_chars"
    if visible_units(s) > MAX_FINAL_VISIBLE_LEN:
        return False, f"> {MAX_FINAL_VISIBLE_LEN}_units"
    return True, "ok"

def build_failure_feedback(s: str, max_len: int) -> str:
    reasons = []
    if not s or not str(s).strip(): reasons.append("空标题")
    else:
        if not ALLOWED_CHARS_RE.fullmatch(normalize_spaces(s)): reasons.append("含数字或非法字符")
        if visible_units(s) > max_len: reasons.append(f"等价长度超限(>{max_len})")
    return "；".join(reasons) or "未达成合规目标"

def simplify_title_again_via_vlm(b64_image: str, cand: str, brand: str = "", max_len: int = MAX_FINAL_VISIBLE_LEN):
    user = f"CANDIDATE: {cand}\nBRAND: {brand}"
    raw, obj = vlm_chat_json(MODEL_TITLE, b64_image, SYSTEM_PROMPT_FOR_SIMPLIFY_JSON,
                             user, num_predict=60, temperature=0.2)
    val = ""
    if isinstance(obj, dict):
        val = normalize_spaces(str(obj.get("promo_title","")).strip())
    return raw or "", val

def expand_title_again_via_vlm(b64_image: str, cand: str, brand: str = "", ori_title: str = "",
                               min_len: int = MIN_FINAL_VISIBLE_LEN, max_len: int = MAX_FINAL_VISIBLE_LEN):
    user = (
        f"CANDIDATE: {cand}\n"
        f"BRAND: {brand}\n"
        f"ORIGINAL_TITLE: {ori_title}"
    )
    raw, obj = vlm_chat_json(MODEL_TITLE, b64_image, SYSTEM_PROMPT_FOR_EXPAND_JSON,
                             user, num_predict=80, temperature=0.2)
    val = ""
    if isinstance(obj, dict):
        val = normalize_spaces(str(obj.get("promo_title","")).strip())
    return raw or "", val

# ======== 进度 & 统计 ========
def _fmt_eta(done, total, start_ts):
    if done == 0: return "ETA --:--"
    elapsed = time.time() - start_ts
    rate = elapsed / done
    remain = (total - done) * rate
    mm, ss = divmod(int(remain), 60)
    return f"ETA {mm:02d}:{ss:02d}"

# ======== 主流程（仅做标题 + 保存白底图路径到 white_bg_image） ========
def main():
    args = parse_args()
    global MODEL_TITLE, CSV_PATH, OUT_DIR, OUT_XLSX, OUT_JSONL, SAMPLE_NUM, RAND_SEED
    MODEL_TITLE = f"qwen2.5vl:{args.model}"
    CSV_PATH = args.csv_path or CSV_PATH
    OUT_DIR = args.out_dir or OUT_DIR
    OUT_XLSX = os.path.join(OUT_DIR, os.path.basename(OUT_XLSX))
    OUT_JSONL = os.path.join(OUT_DIR, os.path.basename(OUT_JSONL))
    SAMPLE_NUM = args.sample_num if (args.sample_num and args.sample_num > 0) else None
    RAND_SEED = args.rand_seed if args.rand_seed is not None else RAND_SEED
    print("===> [START] Title generation (Part1)")
    print(f"Config: USE_MODEL_TITLE={USE_MODEL_TITLE}, SAMPLE_NUM={SAMPLE_NUM}, MODEL_TITLE={MODEL_TITLE}")
    os.makedirs(OUT_DIR, exist_ok=True)

    print(f"[Load] Input CSV/XLSX: {CSV_PATH}")
    df = read_any_table(CSV_PATH)
    total_all = len(df)
    if SAMPLE_NUM:
        df = df.sample(n=min(SAMPLE_NUM, total_all), random_state=RAND_SEED).reset_index(drop=True)
    total = len(df)
    print(f"[Info] Total rows (after sampling): {total} / original {total_all}")

    sess = make_http_session()
    if USE_MODEL_TITLE:
        try:
            requests.get(f"{OLLAMA_HOST}/api/version", timeout=5)
            print(f"[Check] Ollama reachable: {OLLAMA_HOST}")
        except Exception as e:
            print(f"[Warn] Ollama not reachable: {e}  → Titles may not be generated")

    records, durations = [], []
    cnt_ok, cnt_invalid, cnt_offline, cnt_img_fail = 0, 0, 0, 0

    t_start = time.time()
    for i, row in df.iterrows():
        idx = i + 1
        pid  = str(row.get("id", idx))
        title= str(row.get("ori_title","")).strip()
        brand= str(row.get("brand","") or row.get("creative_id_brand","")).strip()
        url  = str(row.get("image_url","") or row.get("creative_id_image","")).strip()
        lvl1 = str(row.get("level_one_category_name","")).strip()
        price= row.get("creative_id_price","");  price = "" if pd.isna(price) else price
        promo= row.get("creative_id_promotion",""); promo = "限时秒杀" if (pd.isna(promo) or not str(promo).strip()) else promo

        if idx % PRINT_EVERY == 0:
            print(f"\n[{idx}/{total}] id={pid} | title='{title[:30]}' | brand='{brand[:20]}' | { _fmt_eta(idx-1, total, t_start) }", flush=True)

        t0 = time.time()
        # 读图 + 保存白底图本地文件
        b64_img = ""
        white_bg_path = ""  # 新增：用于写入输出的白底图本地路径
        if url:
            try:
                im = fetch_image(url, sess)
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print("  - Image: OK")
                # 保存原尺寸 JPG 到 out_step1/white_bg/{id}.jpg
                white_bg_path = os.path.join(OUT_DIR, FILENAME_FMT.format(id=pid))
                save_image_original(im, white_bg_path, quality=95)
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - Saved white BG: {white_bg_path}")
                # 若需要标题推理，则转 b64
                if USE_MODEL_TITLE:
                    b64_img = to_b64(im)
            except Exception as e:
                cnt_img_fail += 1
                white_bg_path = ""
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - Image: FAIL ({e})")
        else:
            if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                print("  - Image: SKIP (no URL)")

        # 标题生成
        promo_title_json_val = ""
        promo_title_candidate = ""
        promo_title_normalized = ""
        promo_title_normalized_raw = ""
        promo_title_is_valid = False
        promo_title_reason = "empty"
        promo_title_retry_times = 0
        promo_title_feedback = ""

        if b64_img and USE_MODEL_TITLE:
            try:
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print("  - VLM(title): calling ...")
                text_payload = f"PRODUCT TEXT FIELDS (TITLE + BRAND ONLY)\n- Title: {title}\n- Brand: {brand}\n"
                promo_title_json_val, obj = vlm_chat_json(MODEL_TITLE, b64_img, SYSTEM_PROMPT_FOR_TITLE_JSON,
                                                          text_payload, num_predict=80, temperature=0.2)
                if isinstance(obj, dict):
                    promo_title_candidate = normalize_spaces(str(obj.get("promo_title","")).strip())

                optimization_notes = []
                if promo_title_candidate:
                    for _ in range(MAX_OPTIMIZATION_ROUNDS):
                        units = visible_units(promo_title_candidate)
                        if units > MAX_FINAL_VISIBLE_LEN:
                            promo_title_retry_times += 1
                            s_raw, s_val = simplify_title_again_via_vlm(b64_img, promo_title_candidate, brand, MAX_FINAL_VISIBLE_LEN)
                            if s_raw:
                                optimization_notes.append("[SIMPLIFY] " + s_raw)
                            if s_val:
                                promo_title_candidate = s_val
                                continue
                            break

                        if units < MIN_FINAL_VISIBLE_LEN:
                            promo_title_retry_times += 1
                            e_raw, e_val = expand_title_again_via_vlm(b64_img, promo_title_candidate, brand, title,
                                                                      MIN_FINAL_VISIBLE_LEN, MAX_FINAL_VISIBLE_LEN)
                            if e_raw:
                                optimization_notes.append("[EXPAND] " + e_raw)
                            if e_val:
                                promo_title_candidate = e_val
                                continue
                            break
                        break

                    if visible_units(promo_title_candidate) > MAX_FINAL_VISIBLE_LEN:
                        promo_title_retry_times += 1
                        s_raw, s_val = simplify_title_again_via_vlm(b64_img, promo_title_candidate, brand, MAX_FINAL_VISIBLE_LEN)
                        if s_raw:
                            optimization_notes.append("[FINAL_SIMPLIFY] " + s_raw)
                        if s_val:
                            promo_title_candidate = s_val

                if optimization_notes:
                    promo_title_shorten = promo_title_candidate
                    promo_title_normalized = promo_title_candidate
                    promo_title_normalized_raw = "\n".join(optimization_notes)

                promo_title_is_valid, promo_title_reason = validate_title(promo_title_candidate)
                if not promo_title_is_valid:
                    promo_title_feedback = build_failure_feedback(promo_title_candidate, MAX_FINAL_VISIBLE_LEN)

            except Exception as e:
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - VLM(title): FAIL ({e})")
        else:
            promo_title_candidate = ""
            promo_title_is_valid = False
            promo_title_reason = "offline_or_no_image"

        promo_title_final = promo_title_candidate or "待审标题"
        title_visible_len = round(visible_units(promo_title_candidate), 1) if promo_title_candidate else 0.0
        is_over_length = title_visible_len > MAX_FINAL_LEN

        if promo_title_is_valid:
            cnt_ok += 1
            if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                print(f"  - Title OK: '{promo_title_final}' (units={title_visible_len:.1f})  | elapsed {time.time()-t0:.2f}s")
        else:
            if promo_title_reason == "offline_or_no_image":
                cnt_offline += 1
            else:
                cnt_invalid += 1
            if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                print(f"  - Title NOT OK: reason={promo_title_reason} (units={title_visible_len:.1f}) → final='{promo_title_final}'  | elapsed {time.time()-t0:.2f}s")

        records.append({
            "id": pid, "ori_title": title, "brand": brand, "image_url": url,
            "level_one_category_name": lvl1,
            "promo_title_json_val": promo_title_json_val,
            "promo_title_candidate": promo_title_candidate,
            "promo_title_shorten": promo_title_normalized,
            "promo_title_shorten_raw": promo_title_normalized_raw,
            "promo_title_final": promo_title_final,
            "promo_title_is_valid": str(promo_title_is_valid).upper(),
            "promo_title_reason": promo_title_reason,
            "promo_title_retry_times": promo_title_retry_times,
            "promo_title_feedback": promo_title_feedback,
            "price": price,
            "promotion": promo,
            # 新增：白底图本地路径
            "white_bg_image": white_bg_path,
            "title_visible_len": title_visible_len,
            "is_over_length": str(is_over_length).upper(),
        })

        durations.append(time.time() - t0)

        # 简要进度行（每 PRINT_EVERY 条）
        if idx % PRINT_EVERY == 0:
            done = idx
            print(f"  -> Progress: {done}/{total} | OK={cnt_ok} invalid={cnt_invalid} offline/noimg={cnt_offline} img_fail={cnt_img_fail} | { _fmt_eta(done, total, t_start) }", flush=True)

    if not records:
        print("⚠️ 没有有效记录"); return

    # 写盘
    print("\n[Write] Saving outputs ...")
    os.makedirs(OUT_DIR, exist_ok=True)
    df_out = pd.DataFrame(records)
    df_out.to_excel(OUT_XLSX, index=False)
    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for r in records: f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total_elapsed = time.time() - t_start
    avg = sum(durations)/len(durations)
    print("\n===> [DONE] Title generation (Part1)")
    print(f"Output Excel : {OUT_XLSX}")
    print(f"Output JSONL : {OUT_JSONL}")
    print(f"Summary      : total={len(records)}, OK={cnt_ok}, invalid={cnt_invalid}, offline/noimg={cnt_offline}, img_fail={cnt_img_fail}")
    print(f"Timing       : total_elapsed={total_elapsed:.2f}s, avg_per_row={avg:.2f}s")

if __name__ == "__main__":
    main()
