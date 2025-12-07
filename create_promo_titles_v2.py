# part1_titles_first_min_no_category_lenflag.py — 去除“仅品类兜底” + 保留 is_over_length 标记列（彻底移除 normalize 版本）
# -*- coding: utf-8 -*-
import os, re, io, time, json, base64, chardet, pandas as pd, requests
from PIL import Image
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# =========================
# 基础配置
# =========================
CSV_PATH = "白底商品信息类目.csv"  # 需包含: id, ori_title, brand(或 creative_id_brand), image_url, level_one_category_name...
OUT_DIR = os.path.join("out_step1")
OUT_XLSX = os.path.join(OUT_DIR, "step1_titles.xlsx")
OUT_JSONL = os.path.join(OUT_DIR, "step1_titles.jsonl")

FILENAME_FMT = "{id}.jpg"  # 生成 out_step1/{id}.jpg
SAMPLE_NUM = 20         # 若需抽样，填整数；否则设为 None
RAND_SEED = 5

# 结果保留策略
# True  : 最终导出保留所有样本（含 is_over_length==True）
# False : 最终导出会过滤掉 is_over_length==True 的样本
KEEP_OVER_LENGTH = True

# =========================
# 模型设置（本步骤仅文本，不送图）
# =========================
OLLAMA_HOST = "http://localhost:11434"
MODEL_TITLE = "qwen2.5vl:7b"
USE_MODEL_TITLE = True

MAX_SEND_WIDTH = 1200
JPEG_QUALITY = 92
REQUEST_TIMEOUT = 180
DEBUG_PRINT = True
PRINT_EVERY = 1

# =========================
# 标题合规口径
# - 中文=1；英文字母=0.5；空格=0.5；数字与任意符号=0.5（※本次更新）
# - 硬性合规：≤12
# - 理想区间：8–12（非强制，仅用于优化牵引与质量指标）
# - 正则口径（校验用）：仅允许 [中文/英文字母/空格]；不允许“·”与其它标点/数字（提示词口径一致）
#   注：即使校验不允许数字/符号，长度计算仍会把数字/符号按 0.5 计入，避免统计偏小
# =========================
MIN_FINAL_VISIBLE_LEN = 8
MAX_FINAL_VISIBLE_LEN = 12
MAX_FINAL_LEN = 12
ALLOWED_CHARS_RE = re.compile(r'^[A-Za-z\u4e00-\u9fa5\s]+$')

# 迭代纠偏最多轮数（通常 1–2 轮即可收敛）
MAX_OPTIMIZATION_ROUNDS = 5

# =========================
# 提示词（严格按你的版本，不做任何改动）
# =========================
SYSTEM_PROMPT_FOR_TITLE_JSON = (
    "你是电商文案助手。结合商品的标题和品牌名称生成一个“品牌+品类”的短标题。要求：\n"
    "1) 标题中允许中文或英文品牌，但不能混排；仅允许中文、英文字母、空格；禁止数字及其它标点；\n"
    "2) 若品牌字段同时包含中文与英文名，标题中只保留中文名；若没有中文名则可保留英文名；\n"
    "3) 严格长度在 10 个“汉字等价”字符：中文=1；英文字母=0.5；空格=0.5；\n"
    "4) 标题里要保留品牌名称；\n"
    "5) 不要促销/活动/夸张词；避免年份、期数、页码等数字信息；忽略店铺后缀（官方旗舰店/旗舰店/专卖店/专营店/旗舰）；\n"
    "6) 严格输出 JSON：例如 {\"promo_title\":\"海尔小优移动屏\"} 或 {\"promo_title\":\"Jil Sander单肩包\"}。"
)

SYSTEM_PROMPT_FOR_SIMPLIFY_JSON = (
    "你是电商短标题精简助手。基于给定的【候选标题】进行二次精简：\n"
    "• 仅允许中文、英文字母、空格；禁止数字及其它标点；\n"
    "• 严格长度在 10 个“汉字等价”字符：中文=1；英文字母=0.5；空格=0.5；\n"
    "• 尽量保留“品牌+品类”核心；\n"
    "• 要保留品牌名称来避免生成仅仅包含商品品类的标题；\n"
    "• **严格长度在 10 个“汉字等价”字符**；\n"
    "严格输出 JSON，如 {\"promo_title\":\"...\"}。"
)

SYSTEM_PROMPT_FOR_EXPAND_JSON = (
    "你是电商短标题补全助手。基于给定的【候选标题】进行二次增加信息，使其更完整但不过度冗长：\n"
    "• 仅允许中文、英文字母、空格；禁止数字及其它标点；\n"
    "• 目标长度在 10 个“汉字等价”字符：中文=1；英文字母=0.5；空格=0.5；\n"
    "• 必须保留品牌名称，并结合【原始标题】补足关键品类或核心属性用词（如“单肩包/凉感衫/保温杯”等），避免营销词与数字；\n"
    "• 保持“品牌+品类”为主；不要添加型号、容量、尺寸等数字信息；\n"
    "• **严格长度在 10 个“汉字等价”字符**；\n"
    "严格输出 JSON，如 {\"promo_title\":\"...\"}。"
)

# =========================
# 工具函数
# =========================
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
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
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
        r = max_w / float(w); img = img.resize((int(w * r), int(h * r)), Image.LANCZOS)
    buf = io.BytesIO(); img.save(buf, "JPEG", quality=quality, optimize=True, subsampling=0)
    return base64.b64encode(buf.getvalue()).decode("ascii")

def save_image_original(img: Image.Image, save_path: str, quality: int = 95):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    img.convert("RGB").save(save_path, "JPEG", quality=quality, optimize=True, subsampling=0)

def vlm_chat_json(model: str, b64_image: str, system_prompt: str, user_text: str,
                  num_predict=80, temperature=0.2):
    # 本步骤不把图片传给模型（仅文本）
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        "stream": False, "format": "json",
        "options": {"num_predict": num_predict, "temperature": temperature, "top_p": 0.9, "repeat_penalty": 1.1}
    }
    r = requests.post(f"{OLLAMA_HOST}/api/chat", json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    raw = (r.json().get("message") or {}).get("content", "") or ""
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
    """
    等价长度计数（对统计与执法统一口径）：
      - 中文字符：+1.0
      - 空白（空格/制表/换行等）：+0.5
      - 其它任意非空白字符（英文字母、数字、标点、Emoji、各种符号等）：+0.5
    说明：
      * 这样即便模型偶发输出数字/符号，长度也会被如实计入，避免统计偏小。
      * 校验层仍由 ALLOWED_CHARS_RE 控制是否允许这些字符（当前不允许），两者口径独立。
    """
    if not s:
        return 0.0
    total = 0.0
    for ch in str(s):
        if ch.isspace():
            total += 0.5
        elif re.match(r'[\u4e00-\u9fa5]', ch):
            total += 1.0
        else:
            # 英文、数字以及任意其它可见符号统一按 0.5 计
            total += 0.5
    return total

def validate_title(raw: str):
    """
    合规校验（硬性）：
      1) 字符集：仅中文/英文字母/空格（由 ALLOWED_CHARS_RE 控制）
      2) 长度：visible_units ≤ 12
    不强制下限（短于 8 也算合规，只在优化阶段尝试拉近）。
    """
    s = normalize_spaces(str(raw or ""))
    if not s:
        return False, "empty"
    if not ALLOWED_CHARS_RE.fullmatch(s):
        return False, "invalid_chars"
    if visible_units(s) > MAX_FINAL_VISIBLE_LEN:
        return False, f"> {MAX_FINAL_VISIBLE_LEN}_units"
    return True, "ok"

def build_failure_feedback(s: str, max_len: int) -> str:
    reasons = []
    if not s or not str(s).strip():
        reasons.append("空标题")
    else:
        if not ALLOWED_CHARS_RE.fullmatch(normalize_spaces(s)):
            reasons.append("含数字或非法字符")
        if visible_units(s) > max_len:
            reasons.append(f"等价长度超限(>{max_len})")
    return "；".join(reasons) or "未达成合规目标"

def simplify_title_again_via_vlm(b64_image: str, cand: str, brand: str,
                                 max_len: int = MAX_FINAL_VISIBLE_LEN):
    user = f"CANDIDATE: {cand}\nBrand: {brand}"
    raw, obj = vlm_chat_json(MODEL_TITLE, b64_image, SYSTEM_PROMPT_FOR_SIMPLIFY_JSON,
                             user, num_predict=60, temperature=0.2)
    val = normalize_spaces(str(obj.get("promo_title", "")).strip()) if isinstance(obj, dict) else ""
    return raw or "", val

def expand_title_again_via_vlm(b64_image: str, cand: str, brand: str,
                               min_len: int = MIN_FINAL_VISIBLE_LEN,
                               max_len: int = MAX_FINAL_VISIBLE_LEN,
                               ori_title: str = None):
    user = f"CANDIDATE: {cand}\nBrand: {brand}\nOriginalTitle: {ori_title or ''}"
    raw, obj = vlm_chat_json(MODEL_TITLE, b64_image, SYSTEM_PROMPT_FOR_EXPAND_JSON,
                             user, num_predict=80, temperature=0.2)
    val = normalize_spaces(str(obj.get("promo_title", "")).strip()) if isinstance(obj, dict) else ""
    return raw or "", val

# =========================
# 进度 & 统计
# =========================
def _fmt_eta(done, total, start_ts):
    if done == 0: return "ETA --:--"
    elapsed = time.time() - start_ts
    remain = (total - done) * (elapsed / done)
    mm, ss = divmod(int(remain), 60)
    return f"ETA {mm:02d}:{ss:02d}"

# =========================
# 主流程
# =========================
def main():
    print("===> [START] Title generation (Part1)")
    print(f"Config: USE_MODEL_TITLE={USE_MODEL_TITLE}, SAMPLE_NUM={SAMPLE_NUM}, MODEL_TITLE={MODEL_TITLE}, KEEP_OVER_LENGTH={KEEP_OVER_LENGTH}")
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

    records = []
    cnt_ok = 0
    cnt_offline = 0
    cnt_img_fail = 0
    durations, t_start = [], time.time()

    for i, row in df.iterrows():
        idx = i + 1
        pid = str(row.get("id", idx))
        title = str(row.get("ori_title", "")).strip()
        brand = str(row.get("brand", "") or row.get("creative_id_brand", "")).strip()
        url = str(row.get("image_url", "") or row.get("creative_id_image", "")).strip()
        lvl1 = str(row.get("level_one_category_name", "")).strip()
        price = "" if pd.isna(row.get("creative_id_price", "")) else row.get("creative_id_price", "")
        promo = row.get("creative_id_promotion", "")
        promo = "限时秒杀" if (pd.isna(promo) or not str(promo).strip()) else promo

        if idx % PRINT_EVERY == 0:
            print(f"\n[{idx}/{total}] id={pid} | title='{title[:30]}' | brand='{brand[:20]}' | {_fmt_eta(idx - 1, total, t_start)}", flush=True)

        t0 = time.time()
        # 读图 + 保存白底图（留给后续环节，当前不送图给模型）
        b64_img, white_bg_path = "", ""
        if url:
            try:
                im = fetch_image(url, sess)
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print("  - Image: OK")
                white_bg_path = os.path.join(OUT_DIR, FILENAME_FMT.format(id=pid))
                save_image_original(im, white_bg_path, quality=95)
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - Saved white BG: {white_bg_path}")
                # b64_img = to_b64(im)   # 当前步骤不送图给模型，如需启用可解注
            except Exception as e:
                cnt_img_fail += 1
                white_bg_path = ""
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - Image: FAIL ({e})")
        else:
            if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                print("  - Image: SKIP (no URL)")

        # 标题生成流水线（无“仅品类兜底”）
        promo_title_final, promo_title_reason = "", "empty"
        title_visible_len = 0.0

        if USE_MODEL_TITLE:
            try:
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print("  - VLM(title): calling ...")
                user_fields = (
                    "PRODUCT TEXT FIELDS (TITLE + BRAND ONLY)\n"
                    f"- Title: {title}\n"
                    f"- Brand: {brand}\n"
                )
                _, obj = vlm_chat_json(MODEL_TITLE, b64_image="", system_prompt=SYSTEM_PROMPT_FOR_TITLE_JSON,
                                       user_text=user_fields, num_predict=80, temperature=0.2)
                cand = normalize_spaces(str(obj.get("promo_title", "")).strip()) if isinstance(obj, dict) else ""

                # 迭代纠偏：>12 必精简；<8 先增补并复检；8–12 直接收敛
                for _ in range(MAX_OPTIMIZATION_ROUNDS):
                    vu = visible_units(cand)

                    # 1) 超长 → 精简并继续复检
                    if vu > MAX_FINAL_VISIBLE_LEN:  # >12
                        _, s_val = simplify_title_again_via_vlm("", cand, brand, MAX_FINAL_VISIBLE_LEN)
                        cand = s_val or cand
                        continue

                    # 2) 过短 → 增补后复检；若过头则当场精简
                    if vu < MIN_FINAL_VISIBLE_LEN:  # <8
                        _, e_val = expand_title_again_via_vlm("", cand, brand,
                                                              MIN_FINAL_VISIBLE_LEN, MAX_FINAL_VISIBLE_LEN,
                                                              ori_title=title)
                        cand = e_val or cand

                        vu2 = visible_units(cand)
                        if vu2 > MAX_FINAL_VISIBLE_LEN:
                            _, s_val = simplify_title_again_via_vlm("", cand, brand, MAX_FINAL_VISIBLE_LEN)
                            cand = s_val or cand
                            continue
                    else:
                        # 3) 8–12：直接收敛
                        break

                # 循环结束后的保底精简（极少数仍可能超长）
                if visible_units(cand) > MAX_FINAL_VISIBLE_LEN:
                    _, s_val = simplify_title_again_via_vlm("", cand, brand, MAX_FINAL_VISIBLE_LEN)
                    cand = s_val or cand

                ok, promo_title_reason = validate_title(cand)  # ≤12 合规
                promo_title_final = cand
                title_visible_len = round(visible_units(promo_title_final), 1)

            except Exception as e:
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - VLM(title): FAIL ({e})")
                promo_title_reason = "exception"
                title_visible_len = 0.0
        else:
            promo_title_reason = "offline_or_no_image"
            title_visible_len = 0.0

        # 是否超字数（基于最终 promo_title_final）
        is_over_length = False
        if promo_title_final:
            is_over_length = (visible_units(promo_title_final) > MAX_FINAL_LEN)  # >12
            title_visible_len = round(visible_units(promo_title_final), 1)

        # OK 的定义与执法口径一致：≤12 即 ok
        if promo_title_final and promo_title_reason == "ok":
            cnt_ok += 1
            if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                print(f"  - Title OK: '{promo_title_final}' (units={title_visible_len:.1f})  | elapsed {time.time() - t0:.2f}s")
        else:
            if promo_title_reason == "offline_or_no_image":
                cnt_offline += 1
            if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                print(f"  - Title NOT OK: reason={promo_title_reason} over_len={is_over_length} → final='{promo_title_final}' (units={title_visible_len:.1f}) | elapsed {time.time() - t0:.2f}s")

        # 写入记录（不包含 compliant_len / ideal_len）
        records.append({
            "id": pid,
            "ori_title": title,
            "brand": brand,
            "image_url": url,
            "level_one_category_name": lvl1,
            "price": price,
            "promotion": promo,
            "promo_title_final": promo_title_final,
            "white_bg_image": white_bg_path,
            "is_over_length": is_over_length,        # True/False（>12）
            "title_visible_len": title_visible_len,  # 可见等价长度
        })

        durations.append(time.time() - t0)

        if idx % PRINT_EVERY == 0:
            done = idx
            # 进度行不再打印 invalid 数量
            print(f"  -> Progress: {done}/{total} | OK(≤12)={cnt_ok} offline/noimg={cnt_offline} img_fail={cnt_img_fail} | {_fmt_eta(done, total, t_start)}", flush=True)

    if not records:
        print("⚠️ 没有有效记录")
        return

    # 写盘（根据 KEEP_OVER_LENGTH 过滤）
    print("\n[Write] Saving outputs ...")
    os.makedirs(OUT_DIR, exist_ok=True)
    df_out_all = pd.DataFrame(records)
    if KEEP_OVER_LENGTH:
        df_out = df_out_all
    else:
        df_out = df_out_all[~df_out_all["is_over_length"].astype(bool)].copy()

    # 导出 Excel / JSONL
    df_out.to_excel(OUT_XLSX, index=False)
    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for r in df_out.to_dict("records"):
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # 汇总质量指标（仅输出合规与理想区间的占比）
    total_elapsed = time.time() - t_start
    avg = sum(durations) / len(durations) if durations else 0.0
    dropped_len = len(df_out_all) - len(df_out)
    compliant_rate = float((df_out_all["title_visible_len"] <= 12).mean()) if len(df_out_all) else 0.0
    ideal_rate = float(((df_out_all["title_visible_len"] >= 8) & (df_out_all["title_visible_len"] <= 12)).mean()) if len(df_out_all) else 0.0

    print("\n===> [DONE] Title generation (Part1)")
    print(f"Output Excel : {OUT_XLSX}")
    print(f"Output JSONL : {OUT_JSONL}")
    # 不再打印 invalid 数量
    print(f"Summary      : total_processed={len(records)}, OK(≤12)={cnt_ok}, offline/noimg={cnt_offline}, img_fail={cnt_img_fail}")
    print(f"Post-filter  : kept={len(df_out)}, dropped_by_len_filter={dropped_len} (KEEP_OVER_LENGTH={KEEP_OVER_LENGTH})")
    print(f"Quality      : compliant_rate(≤12)={compliant_rate:.3f}, ideal_rate(8–12)={ideal_rate:.3f}")
    print(f"Timing       : total_elapsed={total_elapsed:.2f}s, avg_per_row={avg:.2f}s")

if __name__ == "__main__":
    main()
