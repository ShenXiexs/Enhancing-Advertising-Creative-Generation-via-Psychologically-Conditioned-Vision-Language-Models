# -*- coding: utf-8 -*-
"""
Scale the product image (800×800) to 500×500, crop 15px from top and bottom
(resulting in 500×470), place it in the left area of the template, paste the
1280×720 template, and render copy. The right-side text is centered within the
pink background box (X:679-1184) with auto font-size adjustment.
Promotion copy is bold black; the title uses a darker pink-gray in bold.
Price is shown as "¥XXX" plus a small suffix character; the symbol and suffix
are smaller and vertically aligned with the number within the flame icon area,
in white. Adjust SMALL_OFFSET to tweak the vertical offset of the symbol and
suffix.
"""

import argparse
import os
import time
import glob
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

# ------------ Configuration ------------
EXCEL_PATH    = "out_step1/step1_prompts.xlsx"
COMFY_OUTPUT  = r"out_step2"
TEMPLATE_PATH = "template_16_9.png"  # 1280×720 template
RESULT_DIR    = "output_16_9"

# Font paths
FONT_PATH_REG  = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyh.ttc"
FONT_PATH_BOLD = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyhbd.ttc"

# (Optional) template opacity (0~1). 1.0 is fully opaque.
TEMPLATE_OPACITY = 1.0

# Region coordinates (do not change)
PINK_X1, PINK_X2   = 679, 1184  # Pink box X range
FLAME_X1, FLAME_X2 = 776, 1074  # Flame icon X range
FLAME_Y1, FLAME_Y2 = 400, 516   # Flame icon Y range

# Colors (do not change)
TITLE_COLOR = "#8F6D7A"  # Darker pink-gray
PROMO_COLOR = "#000000"  # Black
PRICE_COLOR = "#FFFFFF"  # White

# Font sizes (do not change)
SIZE_TITLE   = 54
SIZE_PROMO   = 50
SIZE_NUM     = 48
SIZE_SMALL   = 28  # ¥ and the "qi/start" marker
SMALL_OFFSET = 12  # Vertical offset for symbol and the "qi/start" marker

# Product image config (do not change)
PROD_SIZE       = (500, 500)
CROP_TOP_BOTTOM = 15    # Crop 15px top/bottom -> 500×470
PROD_POS        = (100, 125)

# Text Y coordinates (do not change)
Y_TITLE = 150
Y_PROMO = 260

# Final output size (do not change)
FINAL_SIZE = (1280, 720)
# ----------------------------------------

# -- Robust helper functions aligned with the 39-40 script --

def safe_text(v: object) -> str:
    """Safely cast any value to string; None/NaN -> empty string."""
    try:
        return "" if pd.isna(v) else str(v)
    except Exception:
        return "" if v is None else str(v)

def fmt_price(v: object) -> str:
    """Price formatting rules:
       - Use two decimals, trim trailing zeros.
       - If decimal length (incl. dot) >= 5, show integer only.
       - On parse failure, return the original string.
    """
    s = safe_text(v).strip()
    if not s:
        return ""
    try:
        f = float(s)
        if f.is_integer():
            candidate = f"{int(f)}"
        else:
            candidate = f"{f:.2f}".rstrip("0").rstrip(".")
        cand = candidate.replace(",", "")
        if "." in cand and len(cand) >= 5:
            return f"{int(f)}"
        return candidate
    except Exception:
        return s

def load_font(path: str, size: int):
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()

# -- Measure width using 39-40 method (getmask + bbox) --
def fit_font(text, font_path, init_size, max_w):
    size = init_size
    while size >= 12:
        font = load_font(font_path, size)
        mask = font.getmask(text)
        bbox = mask.getbbox()
        w = bbox[2]-bbox[0] if bbox else mask.size[0]
        if w <= max_w:
            return font
        size -= 2
    return load_font(font_path, 12)

def load_prompts(path):
    import pandas as pd

    df = pd.read_excel(path)

    # Only require these four columns: id / promo_title_final / price / promotion.
    required = ["id", "promo_title_final", "price", "promotion"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Excel 缺失列: {', '.join(missing)}")

    # Use promo_title_final directly as banner_title.
    df["banner_title"] = df["promo_title_final"].fillna("").astype(str).str.strip()

    # Return the standard four columns compatible with 39-40.
    return df[["id", "price", "banner_title", "promotion"]].to_dict("records")



def find_output_image(dir_, id_):
    # Allow any extension; return the first match containing id.
    files = glob.glob(os.path.join(dir_, f"*{id_}*.*"))
    return files[0] if files else None

def text_width(text, font):
    mask = font.getmask(text)
    bbox = mask.getbbox()
    return (bbox[2]-bbox[0]) if bbox else mask.size[0]

def text_height(font):
    # Approximate height using uppercase baseline characters.
    bbox = font.getbbox("Hg")
    return bbox[3]-bbox[1]

def parse_args():
    parser = argparse.ArgumentParser(description="Overlay 16:9 template onto ComfyUI outputs.")
    parser.add_argument(
        "--prompts-file",
        default=EXCEL_PATH,
        help="Excel file containing id, price, promo_title_final, promotion.",
    )
    parser.add_argument(
        "--comfy-output",
        default=COMFY_OUTPUT,
        help="Directory containing ComfyUI outputs.",
    )
    parser.add_argument(
        "--result-dir",
        default=RESULT_DIR,
        help="Directory to save generated posters.",
    )
    parser.add_argument(
        "--template",
        default=TEMPLATE_PATH,
        help="Template PNG path (expected 1280x720).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    excel_path = args.prompts_file or EXCEL_PATH
    comfy_output = args.comfy_output or COMFY_OUTPUT
    result_dir = args.result_dir or RESULT_DIR
    template_path = args.template or TEMPLATE_PATH
    os.makedirs(result_dir, exist_ok=True)
    records = load_prompts(excel_path)

    # Load template and resize to 1280×720.
    tmpl = Image.open(template_path).convert("RGBA")
    if tmpl.size != FINAL_SIZE:
        tmpl = tmpl.resize(FINAL_SIZE, Image.LANCZOS)
    if TEMPLATE_OPACITY < 1.0:
        a = tmpl.getchannel("A")
        a = a.point(lambda p: int(p * TEMPLATE_OPACITY))
        tmpl.putalpha(a)

    pink_w  = PINK_X2 - PINK_X1
    flame_w = FLAME_X2 - FLAME_X1
    flame_center_y = (FLAME_Y1 + FLAME_Y2) / 2

    total_time, count = 0.0, 0

    for rec in records:
        id_   = safe_text(rec.get("id"))
        price = fmt_price(rec.get("price"))
        title = safe_text(rec.get("banner_title"))
        promo = safe_text(rec.get("promotion"))

        print(f"\n[开始] id={id_}")
        src = find_output_image(comfy_output, id_)
        if not src:
            print("  × 未找到渲染图")
            continue

        t0 = time.time()
        # Product image: resize -> crop.
        prod = Image.open(src).convert("RGBA").resize(PROD_SIZE, Image.LANCZOS)
        w,h = prod.size
        prod = prod.crop((0, CROP_TOP_BOTTOM, w, h - CROP_TOP_BOTTOM))

        # Canvas
        canvas = tmpl.copy()
        draw = ImageDraw.Draw(canvas)
        canvas.paste(prod, PROD_POS, prod)

        # Title (use 39-40 width measure + center)
        font_t = fit_font(title, FONT_PATH_BOLD, SIZE_TITLE, pink_w)
        w_t = text_width(title, font_t)
        x_t = PINK_X1 + (pink_w - w_t)//2
        draw.text((x_t, Y_TITLE), title, font=font_t, fill=TITLE_COLOR)

        # Promotion (same as above)
        font_p = fit_font(promo, FONT_PATH_BOLD, SIZE_PROMO, pink_w)
        w_p = text_width(promo, font_p)
        x_p = PINK_X1 + (pink_w - w_p)//2
        draw.text((x_p, Y_PROMO), promo, font=font_p, fill=PROMO_COLOR)

        # Price (measure/layout like 39-40: "¥ " + number + " qi/start")
        font_num = fit_font(price, FONT_PATH_BOLD, SIZE_NUM, flame_w)
        h_num = text_height(font_num)
        w_num = text_width(price, font_num)

        font_sm = load_font(FONT_PATH_REG, SIZE_SMALL)
        sym_txt = "¥ "
        qi_txt  = " 起"
        w_sym = text_width(sym_txt, font_sm); h_sym = text_height(font_sm)
        w_qi  = text_width(qi_txt,  font_sm); h_qi  = text_height(font_sm)

        sp = 4
        total_w = w_sym + sp + w_num + sp + w_qi
        x0 = FLAME_X1 + (flame_w - total_w)//2

        y_sym = int(flame_center_y - h_sym/2 + SMALL_OFFSET)
        y_num = int(flame_center_y - h_num/2)
        y_qi  = int(flame_center_y - h_qi/2 + SMALL_OFFSET)

        draw.text((x0, y_sym), sym_txt, font=font_sm,  fill=PRICE_COLOR)
        draw.text((x0 + w_sym + sp, y_num), price,   font=font_num, fill=PRICE_COLOR)
        draw.text((x0 + w_sym + sp + w_num + sp, y_qi), qi_txt, font=font_sm, fill=PRICE_COLOR)

        # Save
        out_path = os.path.join(result_dir, f"{id_}_1280x720.png")
        canvas.convert("RGB").save(out_path, quality=95)

        dt = time.time() - t0
        total_time += dt; count += 1
        print(f"  完成 ✅ 用时 {dt:.1f}s → {out_path}")

    if count:
        print(f"\n全部 {count} 张，平均 {total_time/count:.1f}s/张")
    else:
        print("\n⚠️ 未生成任何图片，请检查输入路径。")

if __name__ == '__main__':
    main()
