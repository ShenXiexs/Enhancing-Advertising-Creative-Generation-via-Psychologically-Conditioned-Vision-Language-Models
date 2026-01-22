# -*- coding: utf-8 -*-
"""
3:2 poster overlay:
- Canvas 960x640
- Cover the whole canvas with the product image (under the template)
- Overlay a 960x640 PNG template (with alpha)
- Top-left: title + promo; bottom-right: price (¥ number 起), using 39-40 layout logic
- Excel must include: id, promo_title_final, price, promotion
"""

import argparse
import os
import time
import glob
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

# ========== Paths ==========
EXCEL_PATH    = "out_step1/step1_prompts.xlsx"
COMFY_OUTPUT  = "out_step2"
TEMPLATE_PATH = "template_3_2.png"   # Expected 960x640 PNG template with alpha
RESULT_DIR    = "output_3_2"

# ========== Fonts ==========
FONT_PATH_REG  = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyh.ttc"
FONT_PATH_BOLD = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyhbd.ttc"

# (Optional) template opacity (0~1). 1.0 is fully opaque.
TEMPLATE_OPACITY = 1.0

# ========== Canvas size (3:2) ==========
FINAL_SIZE = (960, 640)   # width, height

# ========== Text layout (tune for template) ==========
# Title: top-left region
TITLE_X = 430
TITLE_Y = 565
TITLE_MAX_W = FINAL_SIZE[0] - TITLE_X * 1  # Auto width
# PROMO_GAP_Y = 22       # Vertical gap between title and promo
SIZE_TITLE  = 50  # Base title size (shrinks to fit)
# SIZE_PROMO  = 25      # Base promo size (shrinks to fit)
TITLE_COLOR = "#FFFFFF"
# PROMO_COLOR = "#111111"

# Price: bottom-right price box area (estimated from template; tweakable)
PRICE_X1, PRICE_X2 = 50, 160  # X range for price display
PRICE_Y1, PRICE_Y2 = 550, 620   # Y range for price display (box bounds)
PRICE_COLOR = "#FFFFFF"
SIZE_NUM     = 60      # Base number size
SIZE_SMALL   = 30       # ¥ and the "qi/start" marker
SMALL_OFFSET = 5       # Slight downward offset for ¥ and the "qi/start" marker

# Promotion text:
PROMO_X = 30
PROMO_Y = 500  # Y range for promo display (box bounds)
PROMO_MAX_W = FINAL_SIZE[0] - PROMO_X * 1  # Auto width
PROMO_COLOR = "#FFFFFF"
SIZE_PROMO  = 30      # Base promo size (shrinks to fit)
# ========== Misc ==========
# If the template bottom has strong texture, text stroke improves readability.
USE_TEXT_STROKE = True  # Enable stroke
STROKE_W = 4
STROKE_COLOR = "#e87c6b"  # Adjusted stroke color


# ---------------- Utilities ----------------
def safe_text(v: object) -> str:  # Safely cast any object to string
    # Convert NaN/None from Excel to "" and cast others to str.
    try:
        return "" if pd.isna(v) else str(v)
    except Exception:
        return "" if v is None else str(v)

def fmt_price(v: object) -> str:  # Format price (integer or up to 2 decimals)
    s = safe_text(v).strip()
    if not s:
        return ""
    try:
        f = float(s)
        if f.is_integer():
            candidate = f"{int(f)}"
        else:
            candidate = f"{f:.2f}".rstrip("0").rstrip(".")  # Trim trailing zeros/dot
        cand = candidate.replace(",", "")
        if "." in cand and len(cand) >= 5:  # If decimal is too long, use integer
            return f"{int(f)}"
        return candidate
    except Exception:
        return s

def load_font(path: str, size: int):   # Load font file with size
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()

# 39-40 width measurement (mask + bbox)
def text_width(text, font):  # Precise width of text under the given font
    # font - ImageFont instance
    mask = font.getmask(text)  # Pixel mask rendered by the font
    bbox = mask.getbbox()  # Non-empty bounds: (left, top, right, bottom)
    return (bbox[2]-bbox[0]) if bbox else mask.size[0]  # Fallback to mask width

def text_height(font):  # Estimate height using "Hg" sample
    bbox = font.getbbox("Hg")  # Non-empty bounds: (left, top, right, bottom)
    return bbox[3]-bbox[1] 

def fit_font(text, font_path, init_size, max_w, min_size=12):  # Auto fit size to max_w
    size = init_size
    while size >= min_size:
        font = load_font(font_path, size)
        if text_width(text, font) <= max_w:
            return font
        size -= 2
    return load_font(font_path, min_size)

def load_prompts(path):  # Load required text fields from Excel into dicts
    df = pd.read_excel(path)
    # Only require: id / promo_title_final / price / promotion
    required = ["id", "promo_title_final", "price", "promotion"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Excel 缺失列: {', '.join(missing)}")
    # Use promo_title_final as banner_title
    df["banner_title"] = df["promo_title_final"].fillna("").astype(str).str.strip()
    return df[["id", "price", "banner_title", "promotion"]].to_dict("records") 

def find_output_image(dir_, id_):  # Find first file containing id_ under dir_
    files = glob.glob(os.path.join(dir_, f"*{id_}*.*"))
    # Match pattern like out_step2/*001*.* for any extension
    return files[0] if files else None

def place_cover(img: Image.Image, size_wh):
    """Scale image to cover size_wh while preserving aspect ratio; center-crop overflow."""
    W, H = size_wh
    iw, ih = img.size
    if iw == 0 or ih == 0:
        return Image.new("RGBA", size_wh, (255,255,255,255))
    r = max(W/iw, H/ih)  # Use max ratio to fully cover target
    nw, nh = int(iw*r), int(ih*r)  # New image size after scaling
    img2 = img.resize((nw, nh), Image.LANCZOS)  # LANCZOS resize
    # Center crop to W×H
    x0 = (nw - W)//2
    y0 = (nh - H)//2
    return img2.crop((x0, y0, x0+W, y0+H))

def draw_text(draw, pos, text, font, fill, stroke=False):
    # font - ImageFont instance
    # fill - text color
    # stroke - enable stroke (True/False)
    if not stroke or not USE_TEXT_STROKE:
        draw.text(pos, text, font=font, fill=fill)
    else:
        draw.text(pos, text, font=font, fill=fill, stroke_width=STROKE_W, stroke_fill=STROKE_COLOR)

# ---------------- Main logic ----------------
def parse_args():
    parser = argparse.ArgumentParser(description="Overlay 3:2 template onto ComfyUI outputs.")
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
        help="Directory to save final posters.",
    )
    parser.add_argument(
        "--template",
        default=TEMPLATE_PATH,
        help="Template PNG path (expected 960x640).",
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

    # Template processing
    tmpl = Image.open(template_path).convert("RGBA")  # Load template as RGBA
    if tmpl.size != FINAL_SIZE:  # Force resize to FINAL_SIZE
        tmpl = tmpl.resize(FINAL_SIZE, Image.LANCZOS)
    if TEMPLATE_OPACITY < 1.0:  # Apply overall opacity to alpha channel
        a = tmpl.getchannel("A")
        a = a.point(lambda p: int(p * TEMPLATE_OPACITY))
        tmpl.putalpha(a)

    # Price display position
    price_box_w  = PRICE_X2 - PRICE_X1 
    price_center_y = (PRICE_Y1 + PRICE_Y2) / 2

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

        # 1) Background: cover with full product image
        bg = Image.open(src).convert("RGBA")
        bg = place_cover(bg, FINAL_SIZE)

        # 2) Overlay template
        canvas = bg.copy()  # Copy background
        canvas.alpha_composite(tmpl)  # Composite template on top

        # 3) Copy
        draw = ImageDraw.Draw(canvas)  # Init drawer

        # Title (top-left, auto width)
        font_t = fit_font(title, FONT_PATH_BOLD, SIZE_TITLE, TITLE_MAX_W)
        draw_text(draw, (TITLE_X, TITLE_Y), title, font_t, TITLE_COLOR, stroke=True)

        # Price (bottom-right; center within price box)
        # Number
        font_num = fit_font(price, FONT_PATH_BOLD, SIZE_NUM, price_box_w)
        w_num = text_width(price, font_num)
        h_num = text_height(font_num)

        # ¥ and the "qi/start" marker
        font_sm = load_font(FONT_PATH_REG, SIZE_SMALL)
        sym_txt = "¥"
        qi_txt  = "起"
        w_sym = text_width(sym_txt, font_sm); h_sym = text_height(font_sm)  # Size of ¥
        w_qi  = text_width(qi_txt,  font_sm); h_qi  = text_height(font_sm)  # Size of qi/start marker

        sp = 10  # Spacing between ¥, price, and qi/start
        total_w = w_sym + sp + w_num + sp + w_qi
        x0 = PRICE_X1 + (price_box_w - total_w)//2  # Start X to center price group

        # Vertically align all three to price_center_y
        y_sym = int(price_center_y - h_sym/2 + SMALL_OFFSET)  # Manual offset
        y_num = int(price_center_y - h_num/2)
        y_qi  = int(price_center_y - h_qi/2 + SMALL_OFFSET)

        draw_text(draw, (x0, y_sym), sym_txt, font_sm, PRICE_COLOR, stroke=True)
        draw_text(draw, (x0 + w_sym + sp, y_num), price, font_num, PRICE_COLOR, stroke=True)
        draw_text(draw, (x0 + w_sym + sp + w_num + sp, y_qi), qi_txt, font_sm, PRICE_COLOR, stroke=True)

        # Promotion (above price) - treat like title
        font_p = fit_font(promo, FONT_PATH_BOLD, SIZE_PROMO, PROMO_MAX_W)
        draw_text(draw, (PROMO_X, PROMO_Y), promo, font_p, PROMO_COLOR, stroke=True)

        # 4) Save (keep 960×640)
        out_path = os.path.join(result_dir, f"{id_}_960x640.png")
        canvas.convert("RGB").save(out_path, quality=95)
        dt = time.time() - t0
        total_time += dt; count += 1
        print(f"  完成 ✅ 用时 {dt:.1f}s → {out_path}")

    if count:
        print(f"\n全部 {count} 张，平均 {total_time/count:.1f}s/张")
    else:
        print("\n⚠️ 未生成任何图片，请检查输入路径/模板。")

if __name__ == '__main__':
    main()
