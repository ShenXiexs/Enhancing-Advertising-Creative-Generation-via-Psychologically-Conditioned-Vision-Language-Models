# -*- coding: utf-8 -*-
"""
Step 2: 读取 Step1 的 Excel -> 下载原图 -> 抠图 + 白底居中留白 -> 以 id_WxH.jpg 命名保存
并将文件名写回 Excel 的 white_bg_image 列（覆盖），同时把 qwen_image_filenames 也改为带尺寸后缀。
"""

import argparse
import os
import re
import cv2
import time
import json
import pandas as pd
import numpy as np
import requests
from io import BytesIO
from PIL import Image
from tqdm import tqdm
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# =============== 可配参数 ===============
EXCEL_PATH  = os.path.join("out_step1", "step1_prompts.xlsx")
OUT_DIR     = "out_step1"          # 与 Step1 同目录，保持“Excel 与图片同夹”

CANVAS_WH   = (960, 640)           # 使用3：2模板
CANVAS_WH = (800, 800)             # 使用其他模板

MARGIN      = 0.15                 # ≥12.5% 留白
WHITE_THR   = 250                  # 固定阈值参与联合
DILATE      = True                 # 掩码膨胀（用于边缘/线条补全）
DILATE_KSZ  = 5
DEBUG_PRINT = True

# —— 多线索抠图的额外参数（可按素材微调）——
BBOX_PAD_RATIO  = 0.015            # 对最终 bbox 外扩比例（避免裁太紧）
MIN_COMP_RATIO  = 0.0005           # 连通域最小占比（过滤小噪点），0.05%
SAT_THRESH      = 20               # HSV 饱和度阈值（>则认为可能是前景）
DARK_V_THRESH   = 245              # HSV 明度阈值（<则认为可能是前景）
CANNY_T1, CANNY_T2 = 50, 150       # Canny 边缘阈值
# ======================================

def read_excel_any(excel_path: str) -> pd.DataFrame:
    return pd.read_excel(excel_path)

def make_http_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    retry = Retry(total=3, backoff_factor=0.5,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

# ---------------- 多线索联合：稳健找 bbox（保留多商品） ----------------
def crop_bbox_robust(img: Image.Image,
                     thr=250,
                     dilate=False,
                     ksz=5,
                     bbox_pad=0.015,
                     min_comp_ratio=0.0005,
                     sat_thresh=20,
                     dark_v_thresh=245,
                     canny_t1=50,
                     canny_t2=150) -> Image.Image:
    W, H = img.size

    # 透明 PNG → 白底，并拿 alpha
    alpha_mask = None
    if img.mode in ("RGBA", "LA"):
        a = np.array(img.split()[-1])
        alpha_mask = (a > 0).astype(np.uint8) * 255
        bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
        bg.paste(img, (0, 0), img)
        rgb = bg.convert("RGB")
    else:
        rgb = img.convert("RGB")

    arr  = np.array(rgb)
    gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)

    # 固定阈值 + Otsu
    _, bin_fixed = cv2.threshold(gray, thr, 255, cv2.THRESH_BINARY_INV)
    try:
        _, bin_otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    except Exception:
        bin_otsu = np.zeros_like(bin_fixed)

    # HSV 线索（高饱和 or 较暗）
    hsv = cv2.cvtColor(arr, cv2.COLOR_RGB2HSV)
    S, V = hsv[:, :, 1], hsv[:, :, 2]
    sat_mask  = (S > SAT_THRESH).astype(np.uint8) * 255
    dark_mask = (V < DARK_V_THRESH).astype(np.uint8) * 255
    bin_hsv = cv2.bitwise_or(sat_mask, dark_mask)

    # 边缘（+可选膨胀）
    edges = cv2.Canny(gray, canny_t1, canny_t2)
    if dilate:
        kernel = np.ones((ksz, ksz), np.uint8)
        edges = cv2.dilate(edges, kernel, iterations=1)

    # 合并
    mask = bin_fixed | bin_otsu | bin_hsv | edges
    if alpha_mask is not None:
        mask = mask | alpha_mask

    # 闭运算
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, np.ones((3, 3), np.uint8), iterations=1)

    # 连通域过滤：保留“大块”
    total_pixels = W * H
    min_area = max(32, int(total_pixels * min_comp_ratio))
    num, labels, stats, _ = cv2.connectedComponentsWithStats((mask > 0).astype(np.uint8), connectivity=8)
    if num <= 1:
        return rgb

    keep = np.zeros_like(mask, dtype=np.uint8)
    for i in range(1, num):
        if stats[i, cv2.CC_STAT_AREA] >= min_area:
            keep[labels == i] = 255

    if keep.max() == 0:
        keep = mask

    ys, xs = np.where(keep > 0)
    if xs.size == 0 or ys.size == 0:
        return rgb

    x0, x1 = xs.min(), xs.max()
    y0, y1 = ys.min(), ys.max()

    # bbox 外扩
    pad_x = int((x1 - x0 + 1) * bbox_pad)
    pad_y = int((y1 - y0 + 1) * bbox_pad)
    x0 = max(0, x0 - pad_x); y0 = max(0, y0 - pad_y)
    x1 = min(W - 1, x1 + pad_x); y1 = min(H - 1, y1 + pad_y)

    return rgb.crop((x0, y0, x1 + 1, y1 + 1))

def place_on_canvas(fg: Image.Image, canvas_wh=(800, 800), margin=0.125) -> Image.Image:
    W, H = canvas_wh
    iw, ih = fg.size
    max_w, max_h = int(W * (1 - 2 * margin)), int(H * (1 - 2 * margin))
    r = min(max_w / iw, max_h / ih)
    nw, nh = max(1, int(iw * r)), max(1, int(ih * r))
    fg_s = fg.resize((nw, nh), Image.LANCZOS)

    bg = Image.new("RGB", (W, H), (255, 255, 255))
    pos = ((W - nw) // 2, (H - nh) // 2)
    bg.paste(fg_s, pos)
    return bg

def _normalize_id(v, fallback_idx: int) -> str:
    if pd.isna(v):
        return str(fallback_idx)
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return str(v).replace(".", "_")
    return str(v).strip()

# --------- 文件名尺寸后缀工具（对列 qwen_image_filenames 使用） ----------
def _add_size_suffix_to_path(path_str: str, wh_tag: str) -> str:
    if not path_str:
        return path_str
    d, b = os.path.dirname(path_str), os.path.basename(path_str)
    if not b:
        return path_str
    if "." in b:
        stem, ext = b.rsplit(".", 1)
        if re.search(r"_\d+x\d+$", stem):
            stem = re.sub(r"_\d+x\d+$", f"_{wh_tag}", stem)
        else:
            stem = f"{stem}_{wh_tag}"
        new_b = f"{stem}.{ext}"
    else:
        if re.search(r"_\d+x\d+$", b):
            new_b = re.sub(r"_\d+x\d+$", f"_{wh_tag}", b)
        else:
            new_b = f"{b}_{wh_tag}"
    return os.path.join(d, new_b) if d else new_b

def _transform_qwen_cell(val, wh_tag: str):
    if pd.isna(val):
        return val
    s = str(val).strip()
    if not s:
        return s
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            out_list = [_add_size_suffix_to_path(str(x).strip(), wh_tag) for x in obj if str(x).strip()]
            return json.dumps(out_list, ensure_ascii=False)
    except Exception:
        pass
    parts = [p.strip() for p in re.split(r"[,\|;]+", s) if p.strip()]
    if len(parts) > 1:
        return ";".join(_add_size_suffix_to_path(p, wh_tag) for p in parts)
    return _add_size_suffix_to_path(s, wh_tag)

def _format_path_for_excel(p: str) -> str:
    """
    将路径规范化，Windows 下使用反斜杠，其它系统使用正斜杠。
    方便在不同平台读取时都能定位到实际文件。
    """
    norm = os.path.normpath(p)
    if os.sep == "\\":
        return norm
    return norm.replace("\\", "/")

def parse_args():
    parser = argparse.ArgumentParser(description="Normalize white background images onto target canvas.")
    parser.add_argument(
        "--excel",
        default=EXCEL_PATH,
        help="Path to the Step1 prompts Excel (default: out_step1/step1_prompts.xlsx).",
    )
    parser.add_argument(
        "--out-dir",
        default=OUT_DIR,
        help="Directory to save normalized images (default: out_step1).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    excel_path = args.excel or EXCEL_PATH
    out_dir = args.out_dir or OUT_DIR
    os.makedirs(out_dir, exist_ok=True)
    df = read_excel_any(excel_path)

    # 若没有目标列则创建
    if "white_bg_image" not in df.columns:
        df["white_bg_image"] = ""
    if "qwen_image_filenames" not in df.columns:
        df["qwen_image_filenames"] = ""

    sess = make_http_session()
    durations = []
    updated_rows = 0

    W, H = CANVAS_WH
    wh_tag = f"{W}x{H}"  # 用于文件名后缀

    for i, row in tqdm(df.iterrows(), total=len(df), desc="Step2"):
        t0 = time.time()
        pid = _normalize_id(row.get("id", i + 1), i + 1)
        url = str(row.get("image_url", "")).strip()

        # 标准化 URL
        if url.startswith("//"):
            url = "https:" + url
        if not url:
            if DEBUG_PRINT:
                print(f"× 缺少 URL: id={pid}")
            continue

        # 下载
        try:
            r = sess.get(url, timeout=10)
            r.raise_for_status()
            img = Image.open(BytesIO(r.content))
        except Exception as e:
            print(f"× 下载失败 id={pid}: {e}")
            continue

        # 抠图 + 放置
        try:
            fg = crop_bbox_robust(
                img,
                thr=WHITE_THR,
                dilate=DILATE,
                ksz=DILATE_KSZ,
                bbox_pad=BBOX_PAD_RATIO,
                min_comp_ratio=MIN_COMP_RATIO,
                sat_thresh=SAT_THRESH,
                dark_v_thresh=DARK_V_THRESH,
                canny_t1=CANNY_T1, canny_t2=CANNY_T2
            )
            out = place_on_canvas(fg, canvas_wh=CANVAS_WH, margin=MARGIN)
        except Exception as e:
            print(f"× 处理失败 id={pid}: {e}")
            continue

        # 生成标准化文件名：id_WxH.jpg
        new_filename = f"{pid}_{wh_tag}.jpg"
        save_path = os.path.join(out_dir, new_filename)

        # 保存到 OUT_DIR/new_filename
        try:
            out.save(save_path, "JPEG", quality=95, subsampling=0, optimize=True)
        except Exception as e:
            print(f"× 保存失败 id={pid}: {e}")
            continue

        # —— 成功后：写回两列 —— #
        # 1) white_bg_image：写回标准化路径（跨平台使用）
        df.at[i, "white_bg_image"] = _format_path_for_excel(os.path.join(out_dir, new_filename))

        # 2) qwen_image_filenames（仅改文本，不改磁盘文件名）
        old_qwen = row.get("qwen_image_filenames", "")
        df.at[i, "qwen_image_filenames"] = _transform_qwen_cell(old_qwen, wh_tag)

        updated_rows += 1

        dt = time.time() - t0
        durations.append(dt)

    # 写回 Excel（只要有任何成功项）
    if updated_rows > 0:
        try:
            df.to_excel(excel_path, index=False)
            print(f"📝 已写回 Excel：{excel_path} | 更新 white_bg_image & qwen_image_filenames 共 {updated_rows} 条")
        except Exception as e:
            print(f"⚠️ 写回 Excel 失败：{e}")

    if durations:
        print(f"✅ 完成：{len(durations)} 张 | 平均每张 {sum(durations)/len(durations):.2f}s")
    else:
        print("⚠️ 没有成功保存的图片。")

if __name__ == "__main__":
    main()
