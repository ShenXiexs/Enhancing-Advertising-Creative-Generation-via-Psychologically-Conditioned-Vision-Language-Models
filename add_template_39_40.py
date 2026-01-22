# -*- coding: utf-8 -*-
"""
Use ComfyUI output product images (800×800) as the base,
then paste the template (780×800) at the top-left corner (0,0).
Copy is read from the Excel "promotion" column, and all text is placed in
the bottom-left, stacked upward in order. Outputs to out_step4 and prints
average runtime.
"""

import argparse
import os
import time
import glob
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
import re  # Added for English-letter detection.

# ------------ Configuration (tune as needed) ------------

EXCEL_PATH    = "out_step1/step1_prompts.xlsx"  # Excel path; requires: id, price, promo_title_final, promotion
COMFY_OUTPUT  = "out_step2"                     # ComfyUI output directory
TEMPLATE_PATH = "template_39_40.png"             # Template image (780×800) with alpha
RESULT_DIR    = "output_39_40"                     # Output directory
FONT_PATH     = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyh.ttc"    # Microsoft YaHei
FONT_PATH_BOLD = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyhbd.ttc"
# Text colors
PROMO_COLOR   = "red"    # Promotion text in red
OTHER_COLOR   = "white"  # Price and title in white

# Font sizes
FONT_SIZE_PROMO = 28
FONT_SIZE_PRICE = 48
FONT_SIZE_TITLE = 58      # Initial title size
FONT_SIZE_TITLE_MIN = 12  # Min title size (keep single line; no truncation)

# Bottom-left padding
MARGIN_X = 20
MARGIN_Y = 60

# Template opacity (0~1). 0.8 means 80%.
TEMPLATE_OPACITY = 0.8

# ------------------------------------------------

def safe_text(v: object) -> str:
    """Safely cast any value to string; None/NaN -> empty string."""
    try:
        return "" if pd.isna(v) else str(v)
    except Exception:
        return "" if v is None else str(v)

def fmt_price(v: object) -> str:
    """Price formatting rules:
       - Use standard format (up to 2 decimals, trim trailing zeros).
       - If decimal form length (including dot) >= 5, show integer only.
       - On parse failure, return the original string.
    """
    s = safe_text(v).strip()
    if not s:
        return ""
    try:
        f = float(s)
        # Build the standard display (2 decimals, trim trailing zeros).
        if f.is_integer():
            candidate = f"{int(f)}"
        else:
            candidate = f"{f:.2f}".rstrip("0").rstrip(".")
        cand = candidate.replace(",", "")
        # Rule: if decimal string length (incl. dot) >= 5, show integer only.
        if "." in cand and len(cand) >= 5:
            return f"{int(f)}"
        return candidate
    except Exception:
        return s


def load_prompts(path):
    df = pd.read_excel(path)
    for c in ("id","price","promo_title_final","promotion"):
        if c not in df.columns:
            raise RuntimeError(f"缺少 Excel 列: {c}")
    return df.to_dict("records")

def find_output_image(output_dir, id_):
    # Allow any extension (png/jpg/webp, etc.).
    files = glob.glob(os.path.join(output_dir, f"*{id_}*.*"))
    return files[0] if files else None

def load_font(size: int):
    try:
        return ImageFont.truetype(FONT_PATH, size)
    except Exception:
        return ImageFont.load_default()

# ===== New: fit title on one line (English letters two sizes smaller) =====

def _is_eng_letter(ch: str) -> bool:
    return bool(re.match(r"[A-Za-z]", ch))

def _measure_mixed_width(draw: ImageDraw.ImageDraw, text: str, font_base, font_eng) -> float:
    """Measure width per character: English uses smaller font; others use base font."""
    w = 0.0
    for ch in text:
        font = font_eng if _is_eng_letter(ch) else font_base
        w += draw.textlength(ch, font=font)
    return w

def fit_title_font_one_line_mixed(draw: ImageDraw.ImageDraw, text: str,
                                  start_size: int, min_size: int, max_width: int):
    """
    Only reduce font size (no wrapping) until width <= max_width or min_size.
    Chinese uses the base size; English letters use base size - 2.
    Returns (font_base, font_eng).
    """
    for sz in range(start_size, min_size - 1, -1):
        font_base = load_font(sz)
        font_eng  = load_font(max(min_size, sz - 1))  # English letters two sizes smaller.
        if _measure_mixed_width(draw, text, font_base, font_eng) <= max_width:
            return font_base, font_eng
    # If it still doesn't fit, use min size for both.
    return load_font(min_size), load_font(min_size)

# =================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Overlay 39x40 template and render text.")
    parser.add_argument(
        "--prompts-file",
        default=EXCEL_PATH,
        help="Excel file containing id, price, promo_title_final, promotion (default: out_step1/step1_prompts.xlsx).",
    )
    parser.add_argument(
        "--comfy-output",
        default=COMFY_OUTPUT,
        help="Directory containing ComfyUI outputs (default: out_step2).",
    )
    parser.add_argument(
        "--result-dir",
        default=RESULT_DIR,
        help="Directory to save final posters (default: output_39_40).",
    )
    parser.add_argument(
        "--template",
        default=TEMPLATE_PATH,
        help="Template PNG path (default: template_39_40.png).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    excel_path = args.prompts_file or EXCEL_PATH
    comfy_output = args.comfy_output or COMFY_OUTPUT
    result_dir = args.result_dir or RESULT_DIR
    template_path = args.template or TEMPLATE_PATH
    os.makedirs(result_dir, exist_ok=True)

    os.makedirs(RESULT_DIR, exist_ok=True)
    records = load_prompts(excel_path)

    # Load and resize the template to 780×800.
    tmpl = Image.open(template_path).convert("RGBA")
    if tmpl.size != (780,800):
        tmpl = tmpl.resize((780,800), Image.LANCZOS)

    # Apply 80% opacity to the template.
    if TEMPLATE_OPACITY < 1.0:
        a = tmpl.getchannel("A")
        a = a.point(lambda p: int(p * TEMPLATE_OPACITY))
        tmpl.putalpha(a)

    # Fixed-size fonts (promotion / price).
    try:
        font_p  = ImageFont.truetype(FONT_PATH, FONT_SIZE_PROMO)
        font_pr = ImageFont.truetype(FONT_PATH, FONT_SIZE_PRICE)
    except Exception:
        font_p = font_pr = ImageFont.load_default()

    total_time = 0.0
    count = 0

    for rec in records:
        id_     = safe_text(rec.get("id"))
        price   = fmt_price(rec.get("price"))
        title   = safe_text(rec.get("promo_title_final"))
        promo   = safe_text(rec.get("promotion"))  # Read copy from Excel (safe cast).
        print(f"\n[开始] id={id_}")

        src = find_output_image(comfy_output, id_)
        if not src:
            print("  跳过：未找到渲染图")
            continue

        t0 = time.time()
        prod = Image.open(src).convert("RGBA")
        # Ensure base image is 800×800.
        if prod.size != (800,800):
            prod = prod.resize((800,800), Image.LANCZOS)

        # 1. Use the product image as the base.
        canvas = prod.copy()

        # 2. Paste the template at (0,0) with alpha (80% opacity).
        canvas.paste(tmpl, (0,0), tmpl)

        # 3. Draw three lines of copy in the bottom-left; positions unchanged.
        draw = ImageDraw.Draw(canvas)
        y_price = 800 - MARGIN_Y - FONT_SIZE_TITLE
        y_title = y_price - 5 - FONT_SIZE_PRICE*0.5
        y_promo = y_title - 10 - FONT_SIZE_PROMO

        # -- promotion (position/size unchanged)
        if promo:
            draw.text((MARGIN_X+5,  y_promo-15),  promo,        font=font_p,  fill=PROMO_COLOR)

        # -- price (position/size unchanged; fmt_price handles >=10000 as int)
        if price:
            draw.text((MARGIN_X+10, y_price),    f"¥{price}",  font=font_pr, fill=OTHER_COLOR)

        # -- title: English letters two sizes smaller; only shrink size to fit one line.
        if title:
            title_x = MARGIN_X + 210
            max_width = 800 - title_x  # Max drawable width (to right edge).

            font_base, font_eng = fit_title_font_one_line_mixed(
                draw, title, FONT_SIZE_TITLE, FONT_SIZE_TITLE_MIN, max_width
            )

            # Draw per character (English letters use smaller font).
            x = title_x
            for ch in title:
                fnt = font_eng if _is_eng_letter(ch) else font_base
                draw.text((x, y_title), ch, font=fnt, fill=OTHER_COLOR)
                x += draw.textlength(ch, font=fnt)

        # 4. Save
        out_path = os.path.join(result_dir, f"{id_}_final.png")
        canvas.convert("RGB").save(out_path, quality=95)

        dt = time.time() - t0
        total_time += dt
        count += 1
        print(f"  完成 ✅ 用时 {dt:.1f}s → {out_path}")

    if count:
        print(f"\n全部 {count} 张完成，平均用时 {total_time/count:.1f}s/张")
    else:
        print("\n未处理任何图片，请检查匹配。")

if __name__=='__main__':
    main()
