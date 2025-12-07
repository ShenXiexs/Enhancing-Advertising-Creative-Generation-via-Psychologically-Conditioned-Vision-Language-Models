# -*- coding: utf-8 -*-
"""
将 out_step1 与 out_step2 的图片拼接（左：原图，右：生成图），并在底部添加白底文字：
- super_category
- ori_title
- promo_title_final
- prompt

- 读取 out_step1/step1_prompts.xlsx
- 列要求：id, local_image, qwen_image_filenames, ori_title, promo_title_final, prompt, super_category
- 右图在 ./out_step2/ 下按 qwen_image_filenames.* 匹配（多张时优先无序号，再按名字排序）
- 两图统一为 800x800（保持比例，超出则缩放，空白补白）
- 输出到 ./out_step3/{id}_pair.jpg
"""

import argparse
import os
import re
import glob
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

# ==== 路径配置 ====
EXCEL_PATH     = os.path.join("out_step1", "step1_prompts.xlsx")
OUT_STEP2_DIR  = os.path.join(".", "out_step2")
OUT_DIR        = os.path.join(".", "out_step3")

# ==== 列名 ====
ID_COL                 = "id"
LEFT_IMAGE_COL         = "white_bg_image"           # out_step1 里的本地图片（通常 800x800）
RIGHT_BASENAME_COL     = "qwen_image_filenames"  # out_step2 输出基名（不含扩展）
PROMPT_COL             = "prompt"
ORI_TITLE_COL          = "ori_title"
FINAL_TITLE_COL        = "promo_title_final"
SUPER_COL              = "super_category"        # 新增：大类

# ==== 画面参数 ====
SIDE_SIZE          = 800   # 每侧目标尺寸（正方形）
GAP_BETWEEN_SIDES  = 0     # 左右图之间的间距（像素）
TEXT_TOP_PADDING   = 24
TEXT_BOTTOM_PADDING= 24
TEXT_SIDE_PADDING  = 40
FONT_SIZE          = 28
LINE_SPACING       = 10    # 行距（像素）
BG_COLOR           = (255, 255, 255)
TEXT_COLOR         = (0, 0, 0)
JPEG_QUALITY       = 92

WIN_FONT_CANDIDATES = [
    r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/msyh.ttc",   # 微软雅黑
    r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/SimHei.ttf", # 黑体
    r"/root/aicloud-data/yoyo_image_gen_mbti/fonts/SimSun.ttc", # 宋体
]
# 常见跨平台字体（如装了 Noto）
CROSS_FONT_CANDIDATES = [
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/System/Library/Fonts/PingFang.ttc",
]

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def load_font(size: int) -> ImageFont.FreeTypeFont:
    # 优先 Windows 常见中文字体
    for fp in WIN_FONT_CANDIDATES + CROSS_FONT_CANDIDATES:
        if os.path.exists(fp):
            try:
                return ImageFont.truetype(fp, size=size)
            except Exception:
                pass
    # 退回 PIL 默认（中文会方块，但不至于报错）
    return ImageFont.load_default()

def resize_to_square(img: Image.Image, size: int, fill=(255,255,255)) -> Image.Image:
    """保持比例缩放到不超过 size，再居中放到 size×size 的白底上。"""
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGB")
    w, h = img.size
    scale = min(size / w, size / h)
    nw, nh = int(w * scale), int(h * scale)
    img2 = img.resize((nw, nh), Image.LANCZOS)
    canvas = Image.new("RGB", (size, size), fill)
    x = (size - nw) // 2
    y = (size - nh) // 2
    canvas.paste(img2, (x, y))
    return canvas

def measure_text_wrapped(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont,
                         max_width: int):
    """
    中文/英文混排按“逐字符”换行，确保像素宽度不超过 max_width。
    返回：行列表, 行高像素
    """
    lines = []
    buf = ""
    for ch in (text or "").replace("\r\n", "\n").replace("\r", "\n"):
        if ch == "\n":
            lines.append(buf)
            buf = ""
            continue
        test = buf + ch
        if draw.textlength(test, font=font) <= max_width:
            buf = test
        else:
            lines.append(buf)
            buf = ch
    lines.append(buf)
    # 行高：用字体高度近似
    ascent, descent = font.getmetrics()
    line_height = ascent + descent
    return lines, line_height

def find_right_image(out_step2_dir: str, base: str):
    """
    在 out_step2_dir 按 base.*、base_*.*：
    - 优先精确：base.jpg/png/webp…
    - 否则找 base_2.* / base_3.* … (按名字排序取最前)
    找不到则返回 None
    """
    if not base:
        return None
    exact = sorted(glob.glob(os.path.join(out_step2_dir, base + ".*")))
    if exact:
        return exact[0]
    with_suffix = sorted(glob.glob(os.path.join(out_step2_dir, base + "_*.*")))
    if with_suffix:
        return with_suffix[0]
    return None

def _norm(x):
    s = str(x) if x is not None else ""
    return "" if s.lower() == "nan" else s

def compose_pair(left_img_path: str,
                 right_img_path: str,
                 super_category: str,
                 ori_title: str,
                 final_title: str,
                 prompt: str,
                 font: ImageFont.FreeTypeFont) -> Image.Image:
    # 读图 & 处理为 800x800
    left  = resize_to_square(Image.open(left_img_path),  SIDE_SIZE, BG_COLOR)
    right = resize_to_square(Image.open(right_img_path), SIDE_SIZE, BG_COLOR)

    # 先拼上半部分（左右图）
    pair_width = SIDE_SIZE * 2 + GAP_BETWEEN_SIDES
    pair_height = SIDE_SIZE
    top_canvas = Image.new("RGB", (pair_width, pair_height), BG_COLOR)
    top_canvas.paste(left,  (0, 0))
    top_canvas.paste(right, (SIDE_SIZE + GAP_BETWEEN_SIDES, 0))

    # 文字内容（带标签；新增 super_category）
    text_block = (
        f"super_category：{_norm(super_category)}\n"
        f"ori_title：{_norm(ori_title)}\n"
        f"promo_title_final：{_norm(final_title)}\n\n"
        f"prompt：{_norm(prompt)}"
    )

    # 计算文字区域高度
    draw = ImageDraw.Draw(top_canvas)
    usable_width = pair_width - 2 * TEXT_SIDE_PADDING
    lines, line_h = measure_text_wrapped(draw, text_block, font, usable_width)
    text_block_h = TEXT_TOP_PADDING + TEXT_BOTTOM_PADDING + max(1, len(lines)) * (line_h + LINE_SPACING) - LINE_SPACING

    # 合并到底部
    out_h = pair_height + text_block_h
    out_img = Image.new("RGB", (pair_width, out_h), BG_COLOR)
    out_img.paste(top_canvas, (0, 0))
    draw2 = ImageDraw.Draw(out_img)

    # 绘制文字
    x = TEXT_SIDE_PADDING
    y = pair_height + TEXT_TOP_PADDING
    for i, line in enumerate(lines):
        draw2.text((x, y + i * (line_h + LINE_SPACING)), line, fill=TEXT_COLOR, font=font)

    return out_img

def parse_args():
    parser = argparse.ArgumentParser(description="Merge white background and generated images into comparison pairs.")
    parser.add_argument(
        "--prompts-file",
        default=EXCEL_PATH,
        help="Excel file containing id, white_bg_image, qwen_image_filenames, etc.",
    )
    parser.add_argument(
        "--generated-dir",
        default=OUT_STEP2_DIR,
        help="Directory containing generated images (default: out_step2).",
    )
    parser.add_argument(
        "--output-dir",
        default=OUT_DIR,
        help="Directory to save merged pairs (default: out_step3).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    excel_path = args.prompts_file or EXCEL_PATH
    generated_dir = args.generated_dir or OUT_STEP2_DIR
    output_dir = args.output_dir or OUT_DIR
    ensure_dir(output_dir)
    if not os.path.exists(excel_path):
        print(f"× 找不到 Excel：{excel_path}")
        return

    df = pd.read_excel(excel_path)
    must_cols = [ID_COL, LEFT_IMAGE_COL, RIGHT_BASENAME_COL, ORI_TITLE_COL, FINAL_TITLE_COL, PROMPT_COL, SUPER_COL]
    for c in must_cols:
        if c not in df.columns:
            print(f"× Excel 缺少列：{c}")
            return

    font = load_font(FONT_SIZE)

    ok = fail = 0
    for _, row in df.iterrows():
        pid         = str(row.get(ID_COL))
        left_p      = str(row.get(LEFT_IMAGE_COL) or "").strip()
        base        = str(row.get(RIGHT_BASENAME_COL) or "").strip()
        super_cat   = row.get(SUPER_COL, "")
        ori_title   = row.get(ORI_TITLE_COL, "")
        final_title = row.get(FINAL_TITLE_COL, "")
        prompt      = row.get(PROMPT_COL, "")

        if not left_p or not os.path.exists(left_p):
            print(f"[{pid}] 跳过：左图不存在 -> {left_p}")
            fail += 1
            continue

        # 寻找右图
        right_p = find_right_image(generated_dir, base)
        temp_right = False
        if not right_p or not os.path.exists(right_p):
            print(f"[{pid}] 警告：未找到右图（{base}.*）于 {generated_dir}，将右侧留白。")
            # 右侧留白
            right_img = Image.new("RGB", (SIDE_SIZE, SIDE_SIZE), BG_COLOR)
            right_img_path = os.path.join(output_dir, f"__tmp_right_blank_{pid}.jpg")
            right_img.save(right_img_path, "JPEG", quality=95)
            right_p = right_img_path
            temp_right = True

        try:
            out_img = compose_pair(left_p, right_p, super_cat, ori_title, final_title, prompt, font)
            save_name = f"{pid}_pair.jpg"
            save_path = os.path.join(output_dir, save_name)
            out_img.save(save_path, "JPEG", quality=JPEG_QUALITY, optimize=True)
            print(f"[{pid}] ✓ 合成完成 -> {save_path}")
            ok += 1
        except Exception as e:
            print(f"[{pid}] × 合成失败：{e}")
            fail += 1
        finally:
            # 清理临时右图
            if temp_right and os.path.exists(right_p):
                try:
                    os.remove(right_p)
                except Exception:
                    pass

    print(f"\n完成：成功 {ok}，失败 {fail}。输出目录：{output_dir}")

if __name__ == "__main__":
    main()
