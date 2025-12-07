# -*- coding: utf-8 -*-
"""
3:2 版海报叠加：
- 画布 960x640
- 整张产品图 cover 铺底（在模板底下）
- 叠加 960x640 模板 PNG（带透明叠层）
- 左上：标题 + 促销语；右下：价格（¥ 数字 起），沿用 39-40 的排版逻辑
- Excel 需含列：id, promo_title_final, price, promotion
"""

import argparse
import os
import time
import glob
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

# ========== 基本路径 ==========
EXCEL_PATH    = "out_step1/step1_prompts.xlsx"
COMFY_OUTPUT  = "out_step2"
TEMPLATE_PATH = "template_3_2.png"   # 期望 960x640 的 PNG 模板（带透明）
RESULT_DIR    = "output_3_2"

# ========== 字体 ==========
FONT_PATH_REG  = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyh.ttc"
FONT_PATH_BOLD = r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyhbd.ttc"

# （可选）模板整体透明度（0~1），1.0 为不透明
TEMPLATE_OPACITY = 1.0

# ========== 画布尺寸（3:2）==========
FINAL_SIZE = (960, 640)   # 宽, 高

# ========== 文案布局（可按模板微调）==========
# 标题：左上区域
TITLE_X = 430
TITLE_Y = 565
TITLE_MAX_W = FINAL_SIZE[0] - TITLE_X * 1# 自适应宽度
# PROMO_GAP_Y = 22       # 标题与促销之间的垂直间距
SIZE_TITLE  = 50  # 标题基准字号（会按宽度收缩）
# SIZE_PROMO  = 25      # 促销基准字号（会按宽度收缩）
TITLE_COLOR = "#FFFFFF"
# PROMO_COLOR = "#111111"

# 价格：右下价格框区域（根据模板大致估计，可调整）
PRICE_X1, PRICE_X2 = 50, 160  # 价格显示可用 X 范围
PRICE_Y1, PRICE_Y2 = 550, 620   # 价格显示可用 Y 范围（框的上下边）
PRICE_COLOR = "#FFFFFF"
SIZE_NUM     = 60      # 数字基准字号
SIZE_SMALL   = 30       # ¥ 与 “起”
SMALL_OFFSET = 5       # 让 ¥ 与 “起”相对数字微微下移

# 促销信息:
PROMO_X = 30
PROMO_Y = 500  # 促销显示可用 Y 范围（框的上下边)
PROMO_MAX_W = FINAL_SIZE[0] -PROMO_X * 1# 自适应宽度
PROMO_COLOR = "#FFFFFF"
SIZE_PROMO  = 30      # 促销基准字号（会按宽度收缩）
# ========== 其它 ==========
# 如果你模板底部有较强纹理，文字描边可以提高可读性；默认关闭
USE_TEXT_STROKE = True # 开启描边
STROKE_W = 4
STROKE_COLOR = "#e87c6b" # 修改了描边的颜色


# ---------------- 工具函数 ----------------
def safe_text(v: object) -> str:  # 安全地将任意对象转换为字符串
    # 将Excel中读取到的NaN, None转成""; 其他的转成字符串类型
    try:
        return "" if pd.isna(v) else str(v)
    except Exception:
        return "" if v is None else str(v)

def fmt_price(v: object) -> str:  # 将任意价格格式化为字符串（整数 or 保留两位小数）
    s = safe_text(v).strip()
    if not s:
        return ""
    try:
        f = float(s)
        if f.is_integer():
            candidate = f"{int(f)}"
        else:
            candidate = f"{f:.2f}".rstrip("0").rstrip(".") # 去掉尾0; 避免出现以小数点结尾
        cand = candidate.replace(",", "")
        if "." in cand and len(cand) >= 5: # 特殊处理「带小数点但太长的数字」
            return f"{int(f)}"
        return candidate
    except Exception:
        return s

def load_font(path: str, size: int):   # 加载字体文件和字号
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()

# 39-40 的测宽方式（mask+bbox）
def text_width(text, font): # 精准计算一段文字在指定字体下的实际显示宽度
    # font - 字体对象
    mask = font.getmask(text) # 会根据字体渲染出这段文字的像素掩码（黑白图）;
    bbox = mask.getbbox() # 获取文字在像素掩码中实际非空部分的边界框; 返回格式是 (left, top, right, bottom)，是像素坐标
    return (bbox[2]-bbox[0]) if bbox else mask.size[0] # mask.size 表示这个掩码的大小，返回一个元组 (width, height)

def text_height(font): # 计算"Hg"在指定字体下的实际显示高度;"Hg" 被广泛用作标准高度估计样本
    bbox = font.getbbox("Hg") # 获取文字在像素掩码中实际非空部分的边界框; 返回格式是 (left, top, right, bottom)，是像素坐标
    return bbox[3]-bbox[1] 

def fit_font(text, font_path, init_size, max_w, min_size=12): # 根据最大允许宽度 max_w，自动调整文字字体大小（字号）来适配，使文字不超出这个宽度
    size = init_size
    while size >= min_size:
        font = load_font(font_path, size)
        if text_width(text, font) <= max_w:
            return font
        size -= 2
    return load_font(font_path, min_size)

def load_prompts(path):  # 从Excel中提取生成图像所需文本信息,并存储在字典列表中
    df = pd.read_excel(path)
    # 仅要求这四列存在：id / promo_title_final / price / promotion
    required = ["id", "promo_title_final", "price", "promotion"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Excel 缺失列: {', '.join(missing)}")
    # 用 promo_title_final 作为 banner_title
    df["banner_title"] = df["promo_title_final"].fillna("").astype(str).str.strip()
    return df[["id", "price", "banner_title", "promotion"]].to_dict("records") 

def find_output_image(dir_, id_):  # 在指定文件夹 dir_ 中查找包含某个 id_的文件路径，找到就返回第一个匹配的文件，否则返回 None
    files = glob.glob(os.path.join(dir_, f"*{id_}*.*")) 
    # 生成匹配模式的路径 - E.g. out_step2/*001*.* - 查找文件名中包含 "001" 的所有文件，不限扩展名
    return files[0] if files else None

def place_cover(img: Image.Image, size_wh):
    """将 img 以 cover 方式等比铺满 size_wh，超出居中裁切。"""
    W, H = size_wh
    iw, ih = img.size
    if iw == 0 or ih == 0:
        return Image.new("RGBA", size_wh, (255,255,255,255))
    r = max(W/iw, H/ih) # 计算缩放比例r, 取最大值以确保原图能完整铺满目标区域
    nw, nh = int(iw*r), int(ih*r) # 确定缩放后的image_size
    img2 = img.resize((nw, nh), Image.LANCZOS) # 使用插值算法LANCZOS 进行缩放生成新图img2
    # 中心裁切到 W×H
    x0 = (nw - W)//2
    y0 = (nh - H)//2
    return img2.crop((x0, y0, x0+W, y0+H))

def draw_text(draw, pos, text, font, fill, stroke=False): 
    #  font - 字体对象（ImageFont.truetype(...)）- 明确字体和字号
    #  fill - 文字填充颜色
    # stroke - 是否启用描边（True / False）开关参数
    if not stroke or not USE_TEXT_STROKE:
        draw.text(pos, text, font=font, fill=fill)
    else:
        draw.text(pos, text, font=font, fill=fill, stroke_width=STROKE_W, stroke_fill=STROKE_COLOR)

# ---------------- 主逻辑 ----------------
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

    # 模板处理
    tmpl = Image.open(template_path).convert("RGBA") # 打开模板图 TEMPLATE_PATH，并统一转为 RGBA 模式（带透明通道）
    if tmpl.size != FINAL_SIZE: # 如果模板尺寸不一致，强制调整为 FINAL_SIZE
        tmpl = tmpl.resize(FINAL_SIZE, Image.LANCZOS)
    if TEMPLATE_OPACITY < 1.0: # 如果设置了不透明度 < 1.0，则对透明通道乘上不透明比例
        a = tmpl.getchannel("A")
        a = a.point(lambda p: int(p * TEMPLATE_OPACITY))
        tmpl.putalpha(a)

    # 价格显示位置
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

        # 1) 背景：整张产品图 cover 铺底
        bg = Image.open(src).convert("RGBA")
        bg = place_cover(bg, FINAL_SIZE)

        # 2) 叠模板
        canvas = bg.copy() # 复制背景
        canvas.alpha_composite(tmpl) # 将模板图叠加在上方

        # 3) 文案
        draw = ImageDraw.Draw(canvas) # 初始化画笔

        # 标题（左上、自适应宽度）
        font_t = fit_font(title, FONT_PATH_BOLD, SIZE_TITLE, TITLE_MAX_W)
        draw_text(draw, (TITLE_X, TITLE_Y), title, font_t, TITLE_COLOR, stroke=True)

        # 价格（右下区域，水平居中在价格框内 / 垂直居中）
        # 数字
        font_num = fit_font(price, FONT_PATH_BOLD, SIZE_NUM, price_box_w)
        w_num = text_width(price, font_num)
        h_num = text_height(font_num)

        # ¥ 与 “起”
        font_sm = load_font(FONT_PATH_REG, SIZE_SMALL)
        sym_txt = "¥"
        qi_txt  = "起"
        w_sym = text_width(sym_txt, font_sm); h_sym = text_height(font_sm) # ¥的高宽
        w_qi  = text_width(qi_txt,  font_sm); h_qi  = text_height(font_sm) # 起的高宽

        sp = 10 # ￥ + 价格 + 起 之间用两个间隔 sp = 6px
        total_w = w_sym + sp + w_num + sp + w_qi
        x0 = PRICE_X1 + (price_box_w - total_w)//2 # 计算价格区域的起点, 要让“￥价格起”整体居中

        # 三个文本都垂直对齐到 price_center_y 这一水平中线附近
        y_sym = int(price_center_y - h_sym/2 + SMALL_OFFSET) # SMALL_OFFSET - 人为微调的小偏移量
        y_num = int(price_center_y - h_num/2)
        y_qi  = int(price_center_y - h_qi/2 + SMALL_OFFSET)

        draw_text(draw, (x0, y_sym), sym_txt, font_sm, PRICE_COLOR, stroke=True)
        draw_text(draw, (x0 + w_sym + sp, y_num), price, font_num, PRICE_COLOR, stroke=True)
        draw_text(draw, (x0 + w_sym + sp + w_num + sp, y_qi), qi_txt, font_sm, PRICE_COLOR, stroke=True)

        # 促销（在价格上方）-  当做标题处理
        font_p = fit_font(promo, FONT_PATH_BOLD, SIZE_PROMO, PROMO_MAX_W)
        draw_text(draw, (PROMO_X, PROMO_Y), promo, font_p, PROMO_COLOR, stroke=True)

        # 4) 保存（保持 960×640）
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
