# -*- coding: utf-8 -*-
"""
将产品图（800×800）缩放为 500×500，纵向上下各裁掉 15 像素（变为 500×470），
放入模板左侧区域，贴上 1280×720 大小的模板图，并写入文案。
右侧文字自动根据粉色背景框（X:679-1184）居中显示并调整字号；
促销语加粗黑体；标题使用更深粉灰色加粗；
价格显示“¥XXX起”，符号与“起”字号较小，与数字一起垂直居中对齐于火焰图标区域，白色。
可以通过 SMALL_OFFSET 调整符号与“起”的垂直偏移。
"""

import argparse
import os
import time
import glob
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

# ------------ 配置区域 ------------
EXCEL_PATH    = "out_step1/step1_prompts.xlsx"
COMFY_OUTPUT  = r"out_step2"
TEMPLATE_PATH = "template_16_9.png"  # 1280×720 模板
RESULT_DIR    = "output_16_9"

# 字体路径
FONT_PATH_REG  = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyh.ttc"
FONT_PATH_BOLD = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyhbd.ttc"

# （可选）模板整体透明度（0~1），1.0 为不透明
TEMPLATE_OPACITY = 1.0

# 区域坐标（不改动）
PINK_X1, PINK_X2   = 679, 1184  # 粉色框 X 范围
FLAME_X1, FLAME_X2 = 776, 1074  # 火焰图标 X 范围
FLAME_Y1, FLAME_Y2 = 400, 516   # 火焰图标 Y 范围

# 颜色（不改动）
TITLE_COLOR = "#8F6D7A"  # 更深粉灰色
PROMO_COLOR = "#000000"  # 黑色
PRICE_COLOR = "#FFFFFF"  # 白色

# 字号（不改动）
SIZE_TITLE   = 54
SIZE_PROMO   = 50
SIZE_NUM     = 48
SIZE_SMALL   = 28  # ¥ 和 起
SMALL_OFFSET = 12  # 符号与“起”的垂直下移偏移

# 产品图配置（不改动）
PROD_SIZE       = (500, 500)
CROP_TOP_BOTTOM = 15    # 上下各裁 15px -> 500×470
PROD_POS        = (100, 125)

# 文本 Y 坐标（不改动）
Y_TITLE = 150
Y_PROMO = 260

# 最终输出尺寸（不改动）
FINAL_SIZE = (1280, 720)
# ----------------------------------------

# —— 与 39-40 脚本一致的稳健辅助函数 ——

def safe_text(v: object) -> str:
    """将任意值安全地转为字符串；None/NaN → 空串。"""
    try:
        return "" if pd.isna(v) else str(v)
    except Exception:
        return "" if v is None else str(v)

def fmt_price(v: object) -> str:
    """价格显示规则：
       - 常规两位小数，去尾零
       - 若包含小数点且总长度（含小数点）≥5，则仅显示整数部分
       - 解析失败则原样返回
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

# —— 回归 39-40 的测宽方式（getmask + bbox） ——
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

    # 仅要求这四列存在：id / promo_title_final / price / promotion
    required = ["id", "promo_title_final", "price", "promotion"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Excel 缺失列: {', '.join(missing)}")

    # 直接用 promo_title_final 作为 banner_title
    df["banner_title"] = df["promo_title_final"].fillna("").astype(str).str.strip()

    # 返回与 39-40 兼容的标准四列
    return df[["id", "price", "banner_title", "promotion"]].to_dict("records")



def find_output_image(dir_, id_):
    # 支持任意扩展名，匹配包含 id 的第一张图
    files = glob.glob(os.path.join(dir_, f"*{id_}*.*"))
    return files[0] if files else None

def text_width(text, font):
    mask = font.getmask(text)
    bbox = mask.getbbox()
    return (bbox[2]-bbox[0]) if bbox else mask.size[0]

def text_height(font):
    # 近似高度，用大写基准字符
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

    # 加载模板并校正到 1280×720
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
        # 产品图：缩放→裁剪
        prod = Image.open(src).convert("RGBA").resize(PROD_SIZE, Image.LANCZOS)
        w,h = prod.size
        prod = prod.crop((0, CROP_TOP_BOTTOM, w, h - CROP_TOP_BOTTOM))

        # 画布
        canvas = tmpl.copy()
        draw = ImageDraw.Draw(canvas)
        canvas.paste(prod, PROD_POS, prod)

        # 标题（沿用 39-40 的测宽 + 居中）
        font_t = fit_font(title, FONT_PATH_BOLD, SIZE_TITLE, pink_w)
        w_t = text_width(title, font_t)
        x_t = PINK_X1 + (pink_w - w_t)//2
        draw.text((x_t, Y_TITLE), title, font=font_t, fill=TITLE_COLOR)

        # 促销（同上）
        font_p = fit_font(promo, FONT_PATH_BOLD, SIZE_PROMO, pink_w)
        w_p = text_width(promo, font_p)
        x_p = PINK_X1 + (pink_w - w_p)//2
        draw.text((x_p, Y_PROMO), promo, font=font_p, fill=PROMO_COLOR)

        # 价格（完全按 39-40 的“¥ ” + 数字 + “ 起” 方式来测量与排版）
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

        # 保存
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
