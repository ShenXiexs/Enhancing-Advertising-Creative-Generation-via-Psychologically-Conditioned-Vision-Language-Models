# part2_prompts_after_titles.py (with progress logs + local_image column)
# -*- coding: utf-8 -*-
import argparse
import os, re, io, json, base64, time, random, pandas as pd, requests, chardet
from datetime import datetime
from pathlib import Path
from PIL import Image
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
try:
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover - optional dep
    np = None
try:
    import torch
except ModuleNotFoundError:  # pragma: no cover - optional dep
    torch = None

# ======== 基础配置 ========
TITLES_XLSX          = "out_step1/step1_titles.xlsx"            # Part1 输出
MAP_CSV_PATH         = "step_one_to_super_category_map.csv"     # 含: level_one_category_name, super_category
TRIAD_PROMPTS_PATH   = "step_one_triad_prompts_22cats.csv"      # 含: Category, Style Priority 1/2/3
STYLE_DESC_PATH      = "step_one_background_description.csv"    # 含: background style, description
OUT_DIR              = "out_step1"
DEFAULT_PROMPTS_NAME = "step1_prompts.xlsx"

# —— VLM（仅用于背景一句话）——
OLLAMA_HOST          = "http://localhost:11434"
MODEL_PROMPT         = "qwen2.5vl:7b"
USE_MODEL_PROMPT     = True
MAX_SEND_WIDTH       = 1200
JPEG_QUALITY         = 92
REQUEST_TIMEOUT      = 180

# —— 日志/打印频率 ——
DEBUG_PRINT          = True
PRINT_EVERY          = 1    # 每多少条打印一次（1=每条都打）

# ======== MBTI 相关默认设置 ========
MBTI_PROFILES_PATH   = "mbti_profiles.csv"
MBTI_JOIN_KEY        = "id"
MBTI_PLAN_CHOICES    = ("none", "A", "B")
MBTI_TYPE_CHOICES    = (
    "ESTJ","ESTP","ESFJ","ESFP","ENTJ","ENTP","ENFJ","ENFP",
    "ISTJ","ISTP","ISFJ","ISFP","INTJ","INTP","INFJ","INFP"
)
MBTI_MODE_CHOICES    = ("concat", "inline")

# ======== Big Five 默认设置 ========
BIG5_PROFILES_PATH   = "big_five_profiles.csv"
BIG5_JOIN_KEY        = "id"
BIG5_PLAN_CHOICES    = ("none", "A", "B")
BIG5_MODE_CHOICES    = MBTI_MODE_CHOICES
BIG5_TRAIT_ALIASES   = {
    "o": "Openness",
    "op": "Openness",
    "open": "Openness",
    "openness": "Openness",
    "c": "Conscientiousness",
    "con": "Conscientiousness",
    "conscientiousness": "Conscientiousness",
    "e": "Extraversion",
    "ext": "Extraversion",
    "extraversion": "Extraversion",
    "a": "Agreeableness",
    "agr": "Agreeableness",
    "agreeableness": "Agreeableness",
    "n": "Neuroticism",
    "neu": "Neuroticism",
    "neuroticism": "Neuroticism",
}

# ======== system prompt 模板 ========
BASE = (
    "You are an art director for product photography and image editing.\n\n"
    "INPUTS: one product PHOTO.\n"
    "TASK: Return EXACTLY FOUR English sentence that describes the environment/background AROUND the product, "
    "while keeping the product itself unchanged and fully visible. Avoid generic phrases like “on a clean white background”."
)
TAIL = (
    'Use cinematic lighting, depth, and realistic shadows. Include 3–8 tasteful props only when appropriate, '
    'and describe at least two concrete scene elements. No people, no on-image text, no logos, no clutter. '
    'English only, ending with "4k".'
)

# ======== I/O & HTTP ========
def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate background prompts (Part2)."
    )
    parser.add_argument(
        "--persona-kind",
        choices=["auto", "mbti", "big5", "none"],
        default="auto",
        help="Select persona source: mbti, big5, or none. auto infers from other args.",
    )
    parser.add_argument(
        "--model",
        choices=["7b", "32b"],
        default="7b",
        help="Specify which qwen2.5vl model size to call via Ollama (default: 7b).",
    )
    parser.add_argument(
        "--mbti-plan",
        choices=MBTI_PLAN_CHOICES,
        default="none",
        help="Append MBTI persona instructions to prompt column. Plan C is ignored.",
    )
    parser.add_argument(
        "--mbti-profiles",
        default=MBTI_PROFILES_PATH,
        help="CSV file that maps IDs to MBTI persona metadata.",
    )
    parser.add_argument(
        "--mbti-key",
        default=MBTI_JOIN_KEY,
        help="Column used to join mbti_profiles onto step1_titles (default: id).",
    )
    parser.add_argument(
        "--mbti-type",
        choices=[""] + list(MBTI_TYPE_CHOICES),
        default="",
        help="Optional MBTI type override (e.g. ENFJ). When provided, all rows use the same persona row.",
    )
    parser.add_argument(
        "--exp-name",
        default="",
        help="Optional experiment/run tag used to rename the output Excel (e.g. planA_0320_1500).",
    )
    parser.add_argument(
        "--mbti-mode",
        choices=MBTI_MODE_CHOICES,
        default="concat",
        help="How to apply MBTI persona: concat to prompt text (default) or inline style guidance.",
    )
    parser.add_argument(
        "--big5-plan",
        choices=BIG5_PLAN_CHOICES,
        default="none",
        help="Big Five persona plan (A=详细，B=精简).",
    )
    parser.add_argument(
        "--big5-profiles",
        default=BIG5_PROFILES_PATH,
        help="CSV file that maps IDs to Big Five persona metadata.",
    )
    parser.add_argument(
        "--big5-key",
        default=BIG5_JOIN_KEY,
        help="Column used to join big_five_profiles (default: id).",
    )
    parser.add_argument(
        "--big5-types",
        default="",
        help="Comma/space separated Big Five traits with level, e.g. 'Openness:High,Conscientiousness:Low'.",
    )
    parser.add_argument(
        "--big5-mode",
        choices=BIG5_MODE_CHOICES,
        default="concat",
        help="How to apply Big Five persona: concat or inline.",
    )
    parser.add_argument(
        "--disable-triad",
        action="store_true",
        help="Skip category triad routing and use the base system prompt only.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=125,
        help="Set a deterministic seed for Python, NumPy, and Torch (<=0 to skip).",
    )
    return parser.parse_args()


def _sanitize_tag(tag: str) -> str:
    tag = (tag or "").strip()
    if not tag:
        return ""
    cleaned = re.sub(r"[^0-9A-Za-z_\-]+", "_", tag)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    for enc in ("utf-8-sig","gb18030","utf-8","latin1"):
        try: return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError: continue
    with open(path,"rb") as f:
        enc = chardet.detect(f.read(65536)).get("encoding") or "utf-8"
    return pd.read_csv(path, encoding=enc, encoding_errors="ignore")

def _find_col(df: pd.DataFrame, candidates):
    cols = list(df.columns)
    lower_to_col = {}
    for c in cols:
        key = str(c).strip().lower()
        if key and key not in lower_to_col:
            lower_to_col[key] = c

    # 1) exact (case-insensitive) match first
    for cand in candidates:
        cand_key = str(cand).strip().lower()
        if cand_key and cand_key in lower_to_col:
            return lower_to_col[cand_key]

    # 2) fuzzy match (substring / token-ish)
    for cand in candidates:
        cand_key = str(cand).strip().lower()
        if not cand_key:
            continue
        for c in cols:
            c_key = str(c).strip().lower()
            if cand_key and cand_key in c_key:
                return c
    return None


def _resolve_col(df: pd.DataFrame, name: str):
    if not name:
        return None
    if name in df.columns:
        return name
    return _find_col(df, [name])


def _text_or_empty(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    s = str(val).strip()
    return "" if not s or s.lower() == "nan" else s

def make_http_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.mount("http://", HTTPAdapter(max_retries=retry)); s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def _pil_from_bytes(b: bytes) -> Image.Image:
    im = Image.open(io.BytesIO(b)); im.load(); return im.convert("RGB")

def fetch_image(src: str, sess: requests.Session) -> Image.Image:
    if src.startswith("//"): src = "https:" + src
    if src.startswith("http"):
        r = sess.get(src, timeout=20); r.raise_for_status(); return _pil_from_bytes(r.content)
    with open(src,"rb") as f: return _pil_from_bytes(f.read())

def to_b64(img: Image.Image, max_w=MAX_SEND_WIDTH, quality=JPEG_QUALITY) -> str:
    w,h = img.size
    if max_w and w>max_w:
        r = max_w/float(w); img = img.resize((int(w*r), int(h*r)), Image.LANCZOS)
    buf = io.BytesIO(); img.save(buf,"JPEG",quality=quality, optimize=True, subsampling=0)
    return base64.b64encode(buf.getvalue()).decode("ascii")

def chat_once(model, b64_image, system_prompt, user_text="", host=OLLAMA_HOST, timeout=REQUEST_TIMEOUT):
    payload = {"model": model, "messages":[
        {"role":"system","content":system_prompt},
        {"role":"user","content":user_text,"images":[b64_image]}
    ], "stream": False, "options":{"num_predict":160,"temperature":0.5,"top_p":0.9,"repeat_penalty":1.1}}
    r = requests.post(f"{host}/api/chat", json=payload, timeout=timeout); r.raise_for_status()
    out = (r.json().get("message") or {}).get("content","").strip()
    if not out: raise ValueError("empty model output")
    return out

def normalize_background_line(s: str) -> str:
    s = s.strip().strip('\'"“”‘’').strip()
    return re.sub(r'^\s*BACKGROUND\s*:\s*', '', s, flags=re.I)


# ======== MBTI persona helpers ========
def load_mbti_profiles(plan: str, path: str, join_key: str, target_type: str = "") -> pd.DataFrame:
    if plan == "none":
        return pd.DataFrame()
    if not os.path.exists(path):
        raise FileNotFoundError(f"mbti_profiles file not found: {path}")
    df = read_any(path)
    join_col = _resolve_col(df, join_key)
    if not join_col:
        raise ValueError(f"mbti_profiles missing join key column '{join_key}'")

    rename_map = {}

    type_col = _find_col(df, ["mbti_type", "type", "persona_type"])
    if target_type:
        if not type_col:
            raise ValueError("mbti_profiles 缺少 mbti_type 列，无法按类型过滤")
        filt = df[type_col].astype(str).str.upper() == target_type.upper()
        df = df[filt]
        if df.empty:
            raise ValueError(f"mbti_profiles 中找不到类型 {target_type}")

    if plan == "A":
        profile_col = _find_col(df, ["plan_a_profile", "persona_explicit", "explicit_profile", "profile_text"])
        if not type_col or not profile_col:
            raise ValueError("mbti_profiles missing columns for plan A (need mbti_type + explicit profile)")
        rename_map[type_col] = "mbti_type"
        rename_map[profile_col] = "mbti_profile_explicit"
    else:
        style_col = _find_col(df, ["plan_b_style", "style_label", "style_short", "style"])
        profile_col = _find_col(df, ["plan_b_profile", "persona_implicit", "implicit_profile", "style_description", "description"])
        if not style_col or not profile_col:
            raise ValueError("mbti_profiles missing columns for plan B (need style + implicit profile)")
        rename_map[style_col] = "mbti_style"
        rename_map[profile_col] = "mbti_profile_implicit"
        if type_col:
            rename_map[type_col] = "mbti_type"

    do_col = _find_col(df, ["mbti_do", "behavior_do", "do", "guidance_do"])
    avoid_col = _find_col(df, ["mbti_avoid", "behavior_avoid", "avoid", "guidance_avoid"])
    if do_col:
        rename_map[do_col] = "mbti_guidance_do"
    if avoid_col:
        rename_map[avoid_col] = "mbti_guidance_avoid"

    keep_cols = [join_col] + [c for c in rename_map.keys()]
    keep_cols = list(dict.fromkeys(keep_cols))
    subset = df[keep_cols].copy()
    subset = subset.rename(columns=rename_map)
    subset = subset.rename(columns={join_col: "__mbti_join_key__"})
    return subset


def build_mbti_block(row: pd.Series, plan: str) -> str:
    if plan == "none":
        return ""
    do_text = _text_or_empty(row.get("mbti_guidance_do"))
    avoid_text = _text_or_empty(row.get("mbti_guidance_avoid"))

    if plan == "A":
        profile = _text_or_empty(row.get("mbti_profile_explicit"))
        persona_type = _text_or_empty(row.get("mbti_type"))
        if not profile:
            return ""
        lines = ["[Persona Instruction]"]
        if persona_type:
            lines.append(f"You are writing with the communication style of {persona_type}.")
        else:
            lines.append("You are writing with a specific empathetic communication style.")
        lines.append("Profile:")
        lines.append(profile)
        prefs = []
        if do_text:
            prefs.append(f"- Do: {do_text}")
        if avoid_text:
            prefs.append(f"- Avoid: {avoid_text}")
        if prefs:
            lines.append("Behavior Preferences:")
            lines.extend(prefs)
        lines.append("Important:")
        lines.append("- This affects writing tone and descriptive style only.")
        lines.append("- Do NOT change product meaning or invent facts.")
        lines.append("- Do NOT mention MBTI unless user asks.")
        return "\n".join(lines)

    # plan B
    profile = _text_or_empty(row.get("mbti_profile_implicit"))
    style_short = _text_or_empty(row.get("mbti_style"))
    if not profile:
        return ""
    lines = ["[Style Context]"]
    if style_short:
        lines.append(f"Style: {style_short}")
    lines.append("Description:")
    lines.append(profile)
    prefs = []
    if do_text:
        prefs.append(f"- Do: {do_text}")
    if avoid_text:
        prefs.append(f"- Avoid: {avoid_text}")
    lines.append("Guidance:")
    if prefs:
        lines.extend(prefs)
    lines.append("- Style affects tone & descriptive emphasis only, not content or correctness.")
    return "\n".join(lines)


# ======== Big Five persona helpers ========
def normalize_big5_trait(name: str) -> str:
    raw = (name or "").strip()
    key = re.sub(r"[^a-z]", "", raw.lower())
    return BIG5_TRAIT_ALIASES.get(key, raw.title() or "Unknown")


def parse_big5_tokens(raw: str):
    tokens = []
    for tok in re.split(r"[,\n]+", raw or ""):
        tok = tok.strip()
        if not tok:
            continue
        m = re.match(r"([A-Za-z]+)[\\s:_\\-]*([+\\-]?|High|Low|H|L)$", tok, flags=re.I)
        if not m or not m.group(2):
            raise ValueError(f"无法解析 Big Five 维度：{tok}")
        trait = normalize_big5_trait(m.group(1))
        lv_raw = m.group(2).lower()
        level = "High" if lv_raw in ("high", "h", "+", "hi", "1") else "Low"
        tokens.append((trait, level))
    if not tokens:
        raise ValueError("big5_types 为空，请提供至少一个维度，例如 'Openness:High'")
    return tokens


def load_big5_profiles(path: str, join_key: str):
    def _read_csv_sniff_sep(csv_path: str) -> pd.DataFrame:
        # Only used as a fallback when columns look wrong (e.g., wrong delimiter).
        for enc in ("utf-8-sig", "gb18030", "utf-8", "latin1"):
            try:
                return pd.read_csv(csv_path, encoding=enc, sep=None, engine="python")
            except UnicodeDecodeError:
                continue
            except Exception:
                continue
        with open(csv_path, "rb") as f:
            enc = chardet.detect(f.read(65536)).get("encoding") or "utf-8"
        return pd.read_csv(csv_path, encoding=enc, sep=None, engine="python")

    def _read_csv_noquote(csv_path: str, sep: str) -> pd.DataFrame:
        # Handles files where each whole row is quoted, e.g. "a,b,c" (single-column under default parser).
        for enc in ("utf-8-sig", "gb18030", "utf-8", "latin1"):
            try:
                return pd.read_csv(csv_path, encoding=enc, sep=sep, engine="python", quotechar="\0")
            except UnicodeDecodeError:
                continue
            except Exception:
                continue
        with open(csv_path, "rb") as f:
            enc = chardet.detect(f.read(65536)).get("encoding") or "utf-8"
        return pd.read_csv(csv_path, encoding=enc, encoding_errors="ignore", sep=sep, engine="python", quotechar="\0")

    p = Path(path)
    if not p.exists():
        script_dir = Path(__file__).resolve().parent
        searched = []
        if not p.is_absolute():
            alt = script_dir / p
            searched.append(str(alt))
            if alt.exists():
                p = alt
        if not p.exists():
            patterns = ("big_five_profiles*.csv", "big*five*profiles*.csv", "big5*profiles*.csv")
            candidates = []
            for pat in patterns:
                candidates.extend(script_dir.glob(pat))
            # Prefer case-insensitive basename match, else accept a single unique candidate.
            by_name = [c for c in candidates if c.name.lower() == p.name.lower()]
            uniq = sorted({c.resolve() for c in (by_name or candidates)})
            if len(uniq) == 1:
                p = uniq[0]
                print(f"[WARN] big_five_profiles not found at '{path}', auto-using '{p}'", flush=True)
            else:
                cwd = Path.cwd()
                hint = (
                    f"big_five_profiles file not found: {path}\n"
                    f"cwd={cwd}\n"
                    f"script_dir={script_dir}\n"
                    f"searched={searched or 'n/a'}\n"
                    f"candidates={[c.name for c in candidates]}\n"
                    "请把 big_five_profiles.csv 放到当前目录/脚本目录，或显式传入 --big5-profiles <path>"
                )
                raise FileNotFoundError(hint)
    df = read_any(str(p))
    df.columns = [str(c).strip() for c in df.columns]
    # Special case: some exports wrap each whole line in quotes, resulting in one big column like:
    # columns=['id,big_five_type,Type,big_five_do,big_five_avoid']
    if len(df.columns) == 1:
        sole = df.columns[0]
        sole_l = sole.lower()
        # Only attempt when it looks like the header is embedded in the single column name.
        if "big_five_type" in sole_l and ("type" in sole_l) and ("big_five_do" in sole_l):
            sep = "\t" if ("\t" in sole and "," not in sole) else ","
            df_fix = _read_csv_noquote(str(p), sep=sep)
            # Strip surrounding quotes introduced by disabling quote parsing
            df_fix.columns = [str(c).strip().strip('"').strip("'") for c in df_fix.columns]
            for c in df_fix.columns:
                if df_fix[c].dtype == object:
                    df_fix[c] = df_fix[c].map(lambda x: x.strip().strip('"').strip("'") if isinstance(x, str) else x)
            df = df_fix
            df.columns = [str(c).strip() for c in df.columns]
            print(f"[WARN] big_five_profiles parsed as whole-line-quoted; re-read with quote disabled (sep={sep!r}).", flush=True)
    trait_col = _find_col(df, ["big_five_type", "trait", "dimension"])
    level_col = _find_col(df, ["Type", "level", "high_low", "polarity"])
    do_col = _find_col(df, ["big_five_do", "do", "positive"])
    avoid_col = _find_col(df, ["big_five_avoid", "avoid", "negative"])
    # If the CSV was read with the wrong delimiter, multiple keys may match the same single column.
    if (p.suffix.lower() in (".csv", ".txt")) and (
        (not trait_col) or (not level_col) or (not do_col) or (len({trait_col, level_col, do_col}) < 3)
    ):
        df2 = _read_csv_sniff_sep(str(p))
        df2.columns = [str(c).strip() for c in df2.columns]
        trait_col2 = _find_col(df2, ["big_five_type", "trait", "dimension"])
        level_col2 = _find_col(df2, ["Type", "level", "high_low", "polarity"])
        do_col2 = _find_col(df2, ["big_five_do", "do", "positive"])
        avoid_col2 = _find_col(df2, ["big_five_avoid", "avoid", "negative"])
        if trait_col2 and level_col2 and do_col2 and len({trait_col2, level_col2, do_col2}) >= 3:
            df, trait_col, level_col, do_col, avoid_col = df2, trait_col2, level_col2, do_col2, avoid_col2
            print(f"[WARN] big_five_profiles delimiter auto-detected; columns={list(df.columns)}", flush=True)
    if not trait_col or not level_col or not do_col:
        raise ValueError(
            "big_five_profiles 缺少必要列：big_five_type/Type/big_five_do。"
            f"实际读取到的列：{list(df.columns)}。"
            "请确认该文件是带表头的 CSV（逗号分隔，含引号转义），或用 --big5-profiles 指向正确文件。"
        )
    if len({trait_col, level_col, do_col}) < 3:
        raise ValueError(
            "big_five_profiles 列名匹配发生冲突（通常是 CSV 分隔符不对导致整行被读成一列）。"
            f"trait_col={trait_col}, level_col={level_col}, do_col={do_col}, columns={list(df.columns)}。"
        )
    rename_map = {
        trait_col: "big5_trait",
        level_col: "big5_level",
        do_col: "big5_do",
    }
    if avoid_col:
        rename_map[avoid_col] = "big5_avoid"
    join_col = _resolve_col(df, join_key) if join_key else None
    if join_col and join_col in df.columns and join_col not in rename_map:
        rename_map[join_col] = "__big5_join_key__"
    keep_cols = list(rename_map.keys())
    subset = df[keep_cols].rename(columns=rename_map).copy()
    subset["big5_trait"] = subset["big5_trait"].map(normalize_big5_trait)
    subset["big5_level"] = subset["big5_level"].astype(str).str.strip().str.title().map(
        lambda x: "High" if x.lower().startswith("h") else "Low"
    )
    return subset


def select_big5_rows(df: pd.DataFrame, tokens):
    rows = []
    for trait, level in tokens:
        mask = (df["big5_trait"].str.lower() == trait.lower()) & (df["big5_level"].str.lower() == level.lower())
        hit = df[mask]
        if hit.empty:
            raise ValueError(f"big_five_profiles 中找不到 {trait} ({level})")
        rows.append(hit.iloc[0])
    return rows


def build_big5_block(rows, plan: str) -> str:
    if not rows:
        return ""
    plan = (plan or "A").upper()
    if plan not in ("A", "B"):
        plan = "A"
    if plan == "B":
        lines = [
            "[Tone Hints]",
            "Blend these Big Five cues into tone and word choice; keep product facts unchanged.",
            "Focus on:",
        ]
        for r in rows:
            desc = _text_or_empty(r.get("big5_do")) or f"{r.get('big5_trait')} ({r.get('big5_level')})"
            lines.append(f"- {r.get('big5_trait')} ({r.get('big5_level')}): {desc}")
        avoid_items = [_text_or_empty(r.get("big5_avoid")) for r in rows if _text_or_empty(r.get("big5_avoid"))]
        if avoid_items:
            lines.append("Avoid:")
            lines.extend(f"- {a}" for a in avoid_items)
        lines.append("Do not mention Big Five explicitly.")
        return "\n".join(lines)

    lines = [
        "[Persona Instruction]",
        "Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.",
        "Traits:",
    ]
    for r in rows:
        trait = _text_or_empty(r.get("big5_trait")) or "Trait"
        level = _text_or_empty(r.get("big5_level")) or "Level"
        desc = _text_or_empty(r.get("big5_do"))
        avoid = _text_or_empty(r.get("big5_avoid"))
        if desc:
            lines.append(f"- {trait} ({level}): {desc}")
        else:
            lines.append(f"- {trait} ({level})")
        if avoid:
            lines.append(f"  Avoid: {avoid}")
    lines.append("Important:")
    lines.append("- This affects tone only; do not invent or alter product facts.")
    lines.append("- Do not mention the Big Five or psychology terms unless requested.")
    return "\n".join(lines)


def big5_label_from_tokens(tokens) -> str:
    def level_code(level: str) -> str:
        return "H" if str(level).lower().startswith("h") or str(level).startswith("+") else "L"
    codes = []
    for trait, level in tokens:
        t = normalize_big5_trait(trait)
        key = re.sub(r"[^A-Z]", "", BIG5_TRAIT_ALIASES.get(t.lower(), t)) or t[:1].upper()
        code = key[0].upper()
        codes.append(f"{code}{level_code(level)}")
    return "_".join(codes)

# ======== triad & style 映射 ========
def load_triad(triad_path: str) -> dict:
    df = read_any(triad_path)
    cat = _find_col(df, ["Category"]); s1=_find_col(df,["Style Priority 1"]); s2=_find_col(df,["Style Priority 2"]); s3=_find_col(df,["Style Priority 3"])
    if not cat or not (s1 or s2 or s3): raise ValueError("triad 缺列：Category / Style Priority 1/2/3")
    m={}
    for _,r in df.iterrows():
        k = str(r.get(cat,"")).strip()
        if not k: continue
        arr = [str(r.get(c,"")).strip() for c in [s1,s2,s3] if c]
        arr = [x for x in arr if x and x.lower()!="nan"]
        seen=set(); uniq=[]
        for x in arr:
            if x not in seen: seen.add(x); uniq.append(x)
        if uniq and (k not in m or len(uniq)>len(m[k])): m[k]=uniq
    return m

def load_style_desc(style_path: str) -> dict:
    df = read_any(style_path)
    sc = _find_col(df, ["background style","style","风格"])
    dc = _find_col(df, ["description","定义","说明","desc"])
    if not sc or not dc: raise ValueError("风格定义表缺列：background style / description")
    return {str(r[sc]).strip(): str(r[dc]).strip() for _,r in df.iterrows() if str(r[sc]).strip() and str(r[dc]).strip()}

def build_system_prompt(category: str, triad_map: dict, style_desc_map: dict) -> str:
    styles = triad_map.get(category, []) or []
    items = []
    for s in styles:
        desc = style_desc_map.get(s)
        if desc: items.append(f"- {s} — {desc}")
    if not items:
        return (BASE + "\n\n" + TAIL).strip()
    return (BASE + "\n\n" + "Choose ONE background style by product type:\n" +
            "\n".join(items) + "\n" + TAIL).strip()

# ======== 进度 & 统计 ========
def _fmt_eta(done, total, start_ts):
    if done == 0: return "ETA --:--"
    elapsed = time.time() - start_ts
    rate = elapsed / done
    remain = (total - done) * rate
    mm, ss = divmod(int(remain), 60)
    return f"ETA {mm:02d}:{ss:02d}"


def seed_everything(seed: int):
    if seed and seed > 0:
        os.environ["PYTHONHASHSEED"] = str(seed)
        random.seed(seed)
        if np is not None:
            np.random.seed(seed)
        if torch is not None:
            torch.manual_seed(seed)
            if torch.cuda.is_available():
                torch.cuda.manual_seed_all(seed)
        print(f"[Seed] Global seed set to {seed}")

# ======== 主流程（生成 super_category + prompt，并并回） ========
def main():
    args = parse_args()
    seed_everything(args.seed)
    global MODEL_PROMPT
    MODEL_PROMPT = f"qwen2.5vl:{args.model}"
    persona_kind = args.persona_kind
    big5_hint = (args.big5_plan != "none") or bool((args.big5_types or "").strip())
    mbti_hint = args.mbti_plan != "none"
    if persona_kind == "auto":
        if big5_hint and mbti_hint:
            raise ValueError("MBTI 与 Big Five 参数同时提供，请用 --persona-kind 选择其一")
        if big5_hint:
            persona_kind = "big5"
        elif mbti_hint:
            persona_kind = "mbti"
        else:
            persona_kind = "none"

    raw_plan = args.mbti_plan
    mbti_plan = raw_plan if raw_plan == "none" else raw_plan.upper()
    mbti_mode = args.mbti_mode
    mbti_type_override = (args.mbti_type or "").upper()
    if persona_kind != "mbti":
        mbti_plan = "none"
        mbti_type_override = ""
    if mbti_type_override == "NONE":
        mbti_plan = "none"
        mbti_type_override = ""

    big5_plan = args.big5_plan if args.big5_plan == "none" else args.big5_plan.upper()
    if persona_kind != "big5":
        big5_plan = "none"
    big5_mode = args.big5_mode
    big5_tokens = []
    big5_block = ""
    big5_label = ""

    exp_tag = _sanitize_tag(args.exp_name)
    triad_enabled = not args.disable_triad
    if not exp_tag:
        if persona_kind == "mbti" and mbti_plan != "none":
            exp_tag = f"{mbti_plan.lower()}_{datetime.now():%m%d%H%M}"
        elif persona_kind == "big5":
            exp_tag = f"big5_{datetime.now():%m%d%H%M}"
    out_name = DEFAULT_PROMPTS_NAME
    if exp_tag:
        out_name = f"step1_prompts_{exp_tag}.xlsx"
    out_xlsx_path = os.path.join(OUT_DIR, out_name)

    print("===> [START] Prompt generation (Part2)")
    persona_mbti_type = mbti_type_override or "mixed"
    print(
        "Config: "
        f"USE_MODEL_PROMPT={USE_MODEL_PROMPT}, MODEL_PROMPT={MODEL_PROMPT}, "
        f"PERSONA={persona_kind}, MBTI_PLAN={mbti_plan}, MBTI_TYPE={persona_mbti_type if persona_kind=='mbti' else 'n/a'}, MBTI_MODE={mbti_mode}, "
        f"BIG5_PLAN={big5_plan}, BIG5_TYPES={args.big5_types or 'n/a'}, BIG5_MODE={big5_mode}, "
        f"TRIAD_ENABLED={triad_enabled}, EXP_TAG={exp_tag or 'default'}"
    )
    os.makedirs(OUT_DIR, exist_ok=True)

    # 载入数据
    print(f"[Load] Titles from: {TITLES_XLSX}")
    base   = read_any(TITLES_XLSX)  # 来自 Part1
    print(f"[Load] Map from: {MAP_CSV_PATH}")
    map_df = read_any(MAP_CSV_PATH)
    if triad_enabled:
        print(f"[Load] Triad from: {TRIAD_PROMPTS_PATH}")
        triad  = load_triad(TRIAD_PROMPTS_PATH)
    else:
        print("[Info] Triad routing disabled; using base system prompt.")
        triad = {}
    print(f"[Load] Style descriptions from: {STYLE_DESC_PATH}")
    sdesc  = load_style_desc(STYLE_DESC_PATH)

    mbti_enabled = mbti_plan != "none"
    mbti_profile_col = ""
    base_key = _resolve_col(base, args.mbti_key)
    big5_enabled = persona_kind == "big5"
    big5_rows = []

    if mbti_enabled:
        print(f"[MBTI] Loading profiles from: {args.mbti_profiles}")
        mbti_df = load_mbti_profiles(mbti_plan, args.mbti_profiles, args.mbti_key, target_type=mbti_type_override)
        if mbti_type_override:
            persona_cols = [c for c in mbti_df.columns if c != "__mbti_join_key__"]
            persona_row = mbti_df.iloc[0]
            for col in persona_cols:
                base[col] = persona_row[col]
            mbti_profile_col = "mbti_profile_explicit" if mbti_plan == "A" else "mbti_profile_implicit"
            available = len(base) if mbti_profile_col in base.columns else 0
            print(f"[MBTI] Override type={mbti_type_override} 应用于所有 {len(base)} 行")
        else:
            if not base_key:
                raise ValueError("MBTI plan enabled but base join key could not be resolved")
            base = base.merge(mbti_df, left_on=base_key, right_on="__mbti_join_key__", how="left")
            base.drop(columns=["__mbti_join_key__"], inplace=True)
            mbti_profile_col = "mbti_profile_explicit" if mbti_plan == "A" else "mbti_profile_implicit"
            available = base[mbti_profile_col].notna().sum()
            print(f"[MBTI] Personas available: {available}/{len(base)} rows")
    if big5_enabled:
        if not args.big5_types.strip():
            raise ValueError("--big5-types 不能为空；示例：'Openness:High,Conscientiousness:Low'")
        if big5_plan == "none":
            big5_plan = "A"
        big5_tokens = parse_big5_tokens(args.big5_types)
        big5_df = load_big5_profiles(args.big5_profiles, args.big5_key)
        big5_rows = select_big5_rows(big5_df, big5_tokens)
        big5_block = build_big5_block(big5_rows, big5_plan)
        big5_label = big5_label_from_tokens(big5_tokens)
        print(f"[Big5] Profiles: {big5_label} | plan={big5_plan} | mode={big5_mode} | tokens={big5_tokens}")
    if exp_tag:
        base["experiment_tag"] = exp_tag

    total = len(base)
    print(f"[Info] Rows to process: {total}")

    # 构建大类映射
    map_df.columns = [str(c).strip() for c in map_df.columns]
    base.columns = [str(c).strip() for c in base.columns]
    map_src_col = _find_col(map_df, ["level_one_category_name", "level_one", "level1", "一级", "Column1", "orig"])
    map_dst_col = _find_col(map_df, ["super_category", "super", "大类", "Column2", "target"])
    if not map_src_col or not map_dst_col:
        cols = list(map_df.columns)
        if len(cols) >= 2:
            map_src_col, map_dst_col = cols[0], cols[1]
            print(
                f"[WARN] 分类映射表列名不标准，使用前两列作为映射：{map_src_col}/{map_dst_col}",
                flush=True,
            )
        else:
            raise ValueError(f"step_one_to_super_category_map.csv 列不足，columns={cols}")
    base_level_one_col = _find_col(base, ["level_one_category_name", "level_one", "Category", "category", "一级类目"])
    if not base_level_one_col:
        raise ValueError(f"step1_titles 缺少一级类目列，columns={list(base.columns)}")
    m = map_df.set_index(map_src_col)[map_dst_col].to_dict()
    base["super_category"] = base[base_level_one_col].map(m).fillna("其他")

    # 模型连通性
    sess = make_http_session()
    if USE_MODEL_PROMPT:
        try:
            requests.get(f"{OLLAMA_HOST}/api/version", timeout=5)
            print(f"[Check] Ollama reachable: {OLLAMA_HOST}")
        except Exception as e:
            print(f"[Warn] Ollama not reachable: {e}  → prompts will fall back")

    # 统计
    prompts = []
    cnt_model_ok, cnt_fallback, cnt_noimg, cnt_fetch_fail = 0, 0, 0, 0
    cnt_mbti_attached, cnt_mbti_missing = 0, 0
    cnt_big5_attached, cnt_big5_missing = 0, 0

    t_start = time.time()
    for i, r in base.iterrows():
        idx = i + 1
        url = str(r.get("image_url","")).strip()
        cat = str(r.get("super_category","其他"))
        sys_prompt = build_system_prompt(cat, triad, sdesc)
        persona_instruction = ""
        persona_mode_current = None
        if mbti_enabled:
            persona_instruction = build_mbti_block(r, mbti_plan)
            persona_mode_current = mbti_mode
        elif big5_enabled:
            persona_instruction = big5_block
            persona_mode_current = big5_mode
        sys_prompt_inline = sys_prompt
        if persona_instruction and persona_mode_current == "inline":
            sys_prompt_inline = (sys_prompt + "\n\n" + persona_instruction).strip()
        triad_hit = ("Choose ONE background style by product type:" in sys_prompt)

        if idx % PRINT_EVERY == 0:
            print(f"\n[{idx}/{total}] id={r.get('id')} | super_category='{cat}' | triad_hit={triad_hit} | { _fmt_eta(idx-1, total, t_start) }", flush=True)

        one_line = "A premium studio scene with textured materials and controlled highlights, realistic shadows, 4k"
        t0 = time.time()

        if USE_MODEL_PROMPT and url:
            try:
                im = fetch_image(url, sess)
                b64 = to_b64(im)
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print("  - Image: OK  | calling VLM(prompt) ...")
                raw = chat_once(MODEL_PROMPT, b64, sys_prompt_inline, user_text="")
                one_line = normalize_background_line(raw)
                cnt_model_ok += 1
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - Prompt OK: '{one_line[:90]}...'  | elapsed {time.time()-t0:.2f}s")
            except Exception as e:
                cnt_fetch_fail += 1
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - Prompt FAIL ({e}) → use fallback")
                cnt_fallback += 1
        else:
            if not url:
                cnt_noimg += 1
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print("  - Image: MISSING → use fallback")
            else:
                cnt_fallback += 1
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print("  - VLM off → use fallback")

        if persona_instruction and persona_mode_current:
            if persona_mode_current == "concat":
                combined = (one_line.strip() + "\n\n" + persona_instruction).strip()
                one_line = combined or one_line
                if mbti_enabled:
                    cnt_mbti_attached += 1
                else:
                    cnt_big5_attached += 1
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - Persona appended (concat, kind={persona_kind})")
            else:
                if mbti_enabled:
                    cnt_mbti_attached += 1
                else:
                    cnt_big5_attached += 1
                if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                    print(f"  - Persona applied inline (kind={persona_kind})")
        elif persona_mode_current:
            if mbti_enabled:
                cnt_mbti_missing += 1
            else:
                cnt_big5_missing += 1
            if DEBUG_PRINT and idx % PRINT_EVERY == 0:
                print("  - Persona missing → skip")

        prompts.append(one_line)

        if idx % PRINT_EVERY == 0:
            done = idx
            print(f"  -> Progress: {done}/{total} | model_ok={cnt_model_ok} fallback={cnt_fallback} no_img={cnt_noimg} fetch_fail={cnt_fetch_fail} | { _fmt_eta(done, total, t_start) }", flush=True)

    base["prompt"] = prompts

    # === 新增：local_image 列（格式：id_qwen_image；若无 id 列，用行号） ===

    base["qwen_image_filenames"] = base["id"].astype(str) + "_qwen_image"

    # 写盘
    print("\n[Write] Saving Excel ...")
    base.to_excel(out_xlsx_path, index=False)

    total_elapsed = time.time() - t_start
    avg = total_elapsed / max(total,1)
    print("\n===> [DONE] Prompt generation (Part2)")
    print(f"Output Excel : {out_xlsx_path}")
    print(f"Summary      : total={total}, model_ok={cnt_model_ok}, fallback={cnt_fallback}, no_img={cnt_noimg}, fetch_fail={cnt_fetch_fail}")
    if mbti_enabled:
        print(f"MBTI        : plan={mbti_plan}, appended={cnt_mbti_attached}, missing_profile={cnt_mbti_missing}")
    if big5_enabled:
        print(f"BigFive     : plan={big5_plan}, appended={cnt_big5_attached}, missing_profile={cnt_big5_missing}, profile={big5_label or args.big5_types}")
    print(f"Timing       : total_elapsed={total_elapsed:.2f}s, avg_per_row={avg:.2f}s")

if __name__ == "__main__":
    main()
