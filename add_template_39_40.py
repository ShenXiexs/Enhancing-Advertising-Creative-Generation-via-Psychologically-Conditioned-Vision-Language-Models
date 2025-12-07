# -*- coding: utf-8 -*-
"""
把 ComfyUI output 的产品图（800×800）当底图，
再将模板图（780×800）贴到左上角 (0,0)。
文案从 Excel 的 promotion 列读取，文字统一放在左下角，按顺序依次向上排列。
输出到 out_step4，并打印平均用时。
"""

import argparse
import os
import time
import glob
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
import re  # ← 为识别英文字母新增

# ------------ 配置区（请根据需要微调） ------------

EXCEL_PATH    = "out_step1/step1_prompts.xlsx"  # Excel 路径，需包含：id, price, promo_title_final, promotion
COMFY_OUTPUT  = "out_step2"                     # ComfyUI 输出目录
TEMPLATE_PATH = "template_39_40.png"             # 模板图 (780×800)，保留透明通道
RESULT_DIR    = "output_39_40"                     # 结果保存目录
FONT_PATH     = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyh.ttc"    # 微软雅黑
FONT_PATH_BOLD = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyhbd.ttc"
# 文字颜色
PROMO_COLOR   = "red"    # promotion 文案使用红色
OTHER_COLOR   = "white"  # 价格和标题使用白色

# 字号
FONT_SIZE_PROMO = 28
FONT_SIZE_PRICE = 48
FONT_SIZE_TITLE = 58      # 标题初始字号
FONT_SIZE_TITLE_MIN = 12  # 标题允许缩小的最小字号（保证不换行、不截断）

# 左下角内边距
MARGIN_X = 20
MARGIN_Y = 60

# 模板整体透明度（0~1），这里设为 0.8（80%）
TEMPLATE_OPACITY = 0.8

# ------------------------------------------------

def safe_text(v: object) -> str:
    """将任意值安全地转为字符串；None/NaN → 空串。"""
    try:
        return "" if pd.isna(v) else str(v)
    except Exception:
        return "" if v is None else str(v)

def fmt_price(v: object) -> str:
    """价格显示规则：
       - 先按常规格式生成（至多两位小数，去尾零）
       - 若带小数点的价格字符串长度（包含小数点）≥ 5，则仅显示整数部分
       - 解析失败则原样返回
    """
    s = safe_text(v).strip()
    if not s:
        return ""
    try:
        f = float(s)
        # 先生成常规显示（两位小数，去尾零）
        if f.is_integer():
            candidate = f"{int(f)}"
        else:
            candidate = f"{f:.2f}".rstrip("0").rstrip(".")
        cand = candidate.replace(",", "")
        # 规则：若包含小数点且总长度（含小数点）≥ 5 → 只显示整数
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
    # 允许任意扩展名（png/jpg/webp等）
    files = glob.glob(os.path.join(output_dir, f"*{id_}*.*"))
    return files[0] if files else None

def load_font(size: int):
    try:
        return ImageFont.truetype(FONT_PATH, size)
    except Exception:
        return ImageFont.load_default()

# ===== 新增：标题单行自适应（英文字母比中文小两个字号） =====

def _is_eng_letter(ch: str) -> bool:
    return bool(re.match(r"[A-Za-z]", ch))

def _measure_mixed_width(draw: ImageDraw.ImageDraw, text: str, font_base, font_eng) -> float:
    """逐字测量宽度：英文字母用更小的字体，其余用基准字体。"""
    w = 0.0
    for ch in text:
        font = font_eng if _is_eng_letter(ch) else font_base
        w += draw.textlength(ch, font=font)
    return w

def fit_title_font_one_line_mixed(draw: ImageDraw.ImageDraw, text: str,
                                  start_size: int, min_size: int, max_width: int):
    """
    仅缩小字号（不换行），直到整段文本宽度 <= max_width 或降到 min_size。
    中文用基准字号；英文字母使用（基准字号-2）。
    返回：(font_base, font_eng)
    """
    for sz in range(start_size, min_size - 1, -1):
        font_base = load_font(sz)
        font_eng  = load_font(max(min_size, sz - 1))  # 英文字母小两个字号（不低于 min_size）
        if _measure_mixed_width(draw, text, font_base, font_eng) <= max_width:
            return font_base, font_eng
    # 放不下就用最小字号（英文字母同最小字号）
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

    # 加载并调整模板到 780×800
    tmpl = Image.open(template_path).convert("RGBA")
    if tmpl.size != (780,800):
        tmpl = tmpl.resize((780,800), Image.LANCZOS)

    # 模板整体透明度 80%
    if TEMPLATE_OPACITY < 1.0:
        a = tmpl.getchannel("A")
        a = a.point(lambda p: int(p * TEMPLATE_OPACITY))
        tmpl.putalpha(a)

    # 固定字号的字体（promotion / price）
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
        promo   = safe_text(rec.get("promotion"))  # 从 Excel 读取文案（安全转字符串）
        print(f"\n[开始] id={id_}")

        src = find_output_image(comfy_output, id_)
        if not src:
            print("  跳过：未找到渲染图")
            continue

        t0 = time.time()
        prod = Image.open(src).convert("RGBA")
        # 确保底图 800×800
        if prod.size != (800,800):
            prod = prod.resize((800,800), Image.LANCZOS)

        # 1. 直接用产品图做底
        canvas = prod.copy()

        # 2. 将模板贴到左上 (0,0)，保留透明（已设置 80% 透明度）
        canvas.paste(tmpl, (0,0), tmpl)

        # 3. 在左下角依次写三行文案 —— 位置保持不变
        draw = ImageDraw.Draw(canvas)
        y_price = 800 - MARGIN_Y - FONT_SIZE_TITLE
        y_title = y_price - 5 - FONT_SIZE_PRICE*0.5
        y_promo = y_title - 10 - FONT_SIZE_PROMO

        # —— promotion（位置/字号不变）
        if promo:
            draw.text((MARGIN_X+5,  y_promo-15),  promo,        font=font_p,  fill=PROMO_COLOR)

        # —— price（位置/字号不变；fmt_price 已实现 ≥10000 仅整数）
        if price:
            draw.text((MARGIN_X+10, y_price),    f"¥{price}",  font=font_pr, fill=OTHER_COLOR)

        # —— title：英文字母小两个字号；只缩放字号以适配一行宽度，不换行，不截断，位置不变
        if title:
            title_x = MARGIN_X + 210
            max_width = 800 - title_x  # 允许绘制的最大宽度（到右边界）

            font_base, font_eng = fit_title_font_one_line_mixed(
                draw, title, FONT_SIZE_TITLE, FONT_SIZE_TITLE_MIN, max_width
            )

            # 逐字绘制（英文字母用小两号字体）
            x = title_x
            for ch in title:
                fnt = font_eng if _is_eng_letter(ch) else font_base
                draw.text((x, y_title), ch, font=fnt, fill=OTHER_COLOR)
                x += draw.textlength(ch, font=fnt)

        # 4. 保存
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
