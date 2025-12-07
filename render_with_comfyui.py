# -*- coding: utf-8 -*-
"""
Batch render with ComfyUI API（根据实验标签输出到 out_step2/<exp_tag>/）
"""
import argparse
import json
import os
import time
import copy
import uuid
import shutil
import re
from datetime import datetime
from urllib.parse import quote
import pandas as pd
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ---------------- CONFIG ----------------
EXCEL_PATH       = os.path.join("out_step1", "step1_prompts.xlsx")
WORKFLOW_JSON    = "promo_banner_v3_api.json"
ID_COL           = "id"
FILENAME_COL     = "white_bg_image"
PROMPT_COL       = "prompt"
NEG_DEFAULT      = (
    "plain white background, dark background, empty minimal backdrop, overly simple scene, "
    "flat lighting, overexposed, underexposed, unrealistic CGI, plastic surface, "
    "cartoonish style, distorted perspective, cluttered props, watermark, logo, text, "
    "people, messy composition, incorrect shadows"
)

COMFY_HOST       = "http://localhost:8000"
CONNECT_TIMEOUT  = 5
READ_TIMEOUT     = 180
REQUEST_TIMEOUT  = 1200
POLL_INTERVAL    = 1.0
OUT_PREFIX_FMT   = "api_{id}"
SAMPLE_NUM       = None
FORCE_COPY_TO_INPUT = True
DRY_RUN_MINIMAL     = False

# 保存 API 输出到当前目录/out_step2/<exp_tag>
SAVE_ROOT = os.path.join(os.getcwd(), "out_step2")
SAVE_DIR = SAVE_ROOT

# 调试输出目录
DEBUG_DIR = "api_debug"
os.makedirs(DEBUG_DIR, exist_ok=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Render prompts with ComfyUI workflow.")
    parser.add_argument(
        "--prompts-file",
        default=EXCEL_PATH,
        help="Path to step1_prompts Excel to consume.",
    )
    parser.add_argument(
        "--exp-name",
        default="",
        help="Experiment/run tag for organizing outputs under out_step2/<exp>. "
             "If empty, infer from prompts filename or fallback to timestamp.",
    )
    parser.add_argument(
        "--output-root",
        default=SAVE_ROOT,
        help="Root directory for rendered images (default: out_step2). Subfolders per experiment are created automatically.",
    )
    return parser.parse_args()


def _sanitize_tag(tag: str) -> str:
    tag = (tag or "").strip()
    if not tag:
        return ""
    cleaned = re.sub(r"[^0-9A-Za-z_\-]+", "_", tag)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


# ---------------- HTTP / 基础 ----------------
def make_session():
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.trust_env = False
    retry = Retry(total=2, backoff_factor=0.4,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def comfy_get(sess, path, **kw):
    return sess.get(f"{COMFY_HOST}{path}", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), **kw)

def check_server(sess):
    print("[info] Checking ComfyUI service...", flush=True)
    r = comfy_get(sess, "/system_stats"); r.raise_for_status()
    stats = r.json()
    print(f"[ok] ComfyUI API: {COMFY_HOST}")
    return stats

def parse_io_dirs(stats):
    args = stats.get("system", {}).get("argv", []) or []
    base = input_dir = output_dir = None
    for i, t in enumerate(args):
        if t == "--base-directory"   and i + 1 < len(args): base = args[i + 1]
        if t == "--input-directory"  and i + 1 < len(args): input_dir = args[i + 1]
        if t == "--output-directory" and i + 1 < len(args): output_dir = args[i + 1]
    return base, input_dir, output_dir


# ---------------- 数据加载 ----------------
def read_prompts(path):
    df = pd.read_excel(path)
    if SAMPLE_NUM:
        df = df.head(SAMPLE_NUM)
    return df

def load_workflow(path):
    wf = json.load(open(path, encoding="utf-8"))
    if isinstance(wf, dict) and "nodes" in wf:
        wf = wf["nodes"]
    bad = [nid for nid, n in wf.items() if not isinstance(n, dict) or not n.get("class_type")]
    if bad:
        raise RuntimeError(f"Invalid workflow nodes (missing class_type): {bad}")
    return wf


# ---------------- 关键：图片放入 input 根目录 + 注入到 LoadImage ----------------
def ensure_in_input(fp, input_dir):
    """将源图复制到 ComfyUI 的 input 根目录（LoadImage 仅按文件名在 input 根目录找）"""
    fp = os.path.abspath(fp)
    if not FORCE_COPY_TO_INPUT or not input_dir:
        return fp
    dst = os.path.join(input_dir, os.path.basename(fp))
    if os.path.abspath(fp) != os.path.abspath(dst):
        if not os.path.exists(dst):
            os.makedirs(input_dir, exist_ok=True)
            shutil.copy2(fp, dst)
    return dst

def inject_params(wf, img_abs_path, prompt, neg, out_pref):
    """仅给 LoadImage/Load Image 写入文件名；其它占位符照旧。"""
    wf2 = copy.deepcopy(wf)
    for _, node in wf2.items():
        c   = node.get("class_type")
        ins = node.get("inputs", {}) or {}

        # {{IMAGE_PATH}}
        for k, v in list(ins.items()):
            if isinstance(v, str) and v == "{{IMAGE_PATH}}":
                ins[k] = img_abs_path

        if c == "PrimitiveString":
            if ins.get("value") == "{{PROMPT}}": ins["value"] = prompt
            if ins.get("value") == "{{NEG}}":    ins["value"] = neg

        if c == "SaveImage" and ins.get("filename_prefix") == "{{OUT}}":
            ins["filename_prefix"] = out_pref

        # LoadImage / Load Image → 只填文件名
        if c in ("LoadImage", "Load Image") and "image" in ins:
            ins["image"] = os.path.basename(img_abs_path)

        if c in ("TextEncodeQwenImageEdit","TextEncodeQwen","TextEncodeQwenImage",
                 "CLIPTextEncode","CLIPTextEncodeSDXL"):
            if ins.get("prompt") == "{{NEG}}": ins["prompt"] = neg
            if ins.get("text")   == "{{NEG}}": ins["text"]   = neg

        node["inputs"] = ins
    return wf2


# ---------------- 执行与轮询 ----------------
def print_error_messages(rec):
    try:
        msgs = (rec or {}).get("status", {}).get("messages", []) or []
        for m in msgs:
            if isinstance(m, dict):
                lvl = m.get('level', '')
                msg = m.get('message', '')
                if msg:
                    print(f"    [{lvl}] {msg}")
            else:
                print(f"    [msg] {m}")
    except Exception:
        pass

def wait_history(sess, pid, max_wait=3600):
    t0 = time.time()
    last = None
    while True:
        time.sleep(POLL_INTERVAL)
        r = sess.get(f"{COMFY_HOST}/history/{pid}", timeout=READ_TIMEOUT)
        if r.status_code != 200:
            continue
        data   = r.json().get(pid, {})
        status = (data.get("status", {}) or {}).get("status_str") or (data.get("status", {}) or {}).get("status")
        if status and status != last:
            print(f"  [status] {status} ({int(time.time()-t0)}s)")
            last = status
        if data.get("outputs"):
            return ("ok", time.time() - t0, data)
        if status == "error":
            return ("error", time.time() - t0, data)
        if time.time() - t0 > max_wait:
            return ("timeout", time.time() - t0, data)


# ---------------- 下载输出到 ./out_step2 ----------------
def _unique_path(dirpath, filename):
    """避免重名覆盖：存在则自动加 _1/_2..."""
    name, ext = os.path.splitext(filename)
    cand = os.path.join(dirpath, filename)
    idx = 1
    while os.path.exists(cand):
        cand = os.path.join(dirpath, f"{name}_{idx}{ext}")
        idx += 1
    return cand

def _download_view(sess, filename, subfolder, ftype):
    """通过 /view 下载输出文件（通常是 type=output）"""
    url = f"{COMFY_HOST}/view?filename={quote(filename)}&subfolder={quote(subfolder or '')}&type={quote(ftype or 'output')}"
    r = sess.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), stream=True)
    r.raise_for_status()
    return r.content


def save_outputs(sess, history_obj, save_dir, prefer_basename=None):
    """
    从 /history 里找到所有图片并下载到 save_dir。
    如果提供 prefer_basename（不含扩展名），则优先用它命名：
      - 第一张：{prefer_basename}{ext}
      - 其后：  {prefer_basename}_{n}{ext}
    否则使用 ComfyUI 原始文件名。
    """
    saved = []
    outputs = (history_obj or {}).get("outputs", {}) or {}
    counter = 1
    for _node_id, node_out in outputs.items():
        images = (node_out or {}).get("images") or []
        for im in images:
            fn   = im.get("filename")
            sub  = im.get("subfolder", "")
            ftyp = im.get("type", "output")
            if not fn:
                continue
            try:
                content = _download_view(sess, fn, sub, ftyp)
                ext = os.path.splitext(fn)[1] or ".png"
                if prefer_basename:
                    # 第一张不带序号，其后带 _2, _3...
                    if counter == 1:
                        target_name = f"{prefer_basename}{ext}"
                    else:
                        target_name = f"{prefer_basename}_{counter}{ext}"
                else:
                    target_name = fn
                local_path = _unique_path(save_dir, target_name)
                with open(local_path, "wb") as f:
                    f.write(content)
                saved.append(local_path)
                counter += 1
            except Exception as e:
                print(f"    [warn] 下载失败 {fn}: {e}")
    return saved


# ---------------- 主流程 ----------------
def main():
    args = parse_args()
    excel_path = args.prompts_file
    exp_tag = _sanitize_tag(args.exp_name)
    if not exp_tag:
        m = re.match(r"step1_prompts_(.+)\.xlsx$", os.path.basename(excel_path), re.I)
        if m:
            exp_tag = _sanitize_tag(m.group(1))
    if not exp_tag:
        exp_tag = datetime.now().strftime("run_%m%d%H%M")

    output_root = os.path.abspath(args.output_root or SAVE_ROOT)
    final_save_dir = os.path.join(output_root, exp_tag) if exp_tag else output_root
    os.makedirs(final_save_dir, exist_ok=True)

    global SAVE_DIR
    SAVE_DIR = final_save_dir

    print(f"[config] prompts_file = {excel_path}")
    print(f"[config] experiment   = {exp_tag}")
    print(f"[config] save_dir     = {SAVE_DIR}")

    sess  = make_session()
    stats = check_server(sess)
    base, inp_dir, out_dir = parse_io_dirs(stats)

    df = read_prompts(excel_path)
    print(f"[info] {len(df)} records to process.")

    wf_template = None if DRY_RUN_MINIMAL else load_workflow(WORKFLOW_JSON)
    client_id   = str(uuid.uuid4())

    durations = []
    ok = fail = 0

    for i, rec in enumerate(df.to_dict("records"), 1):
        pid    = str(rec.get(ID_COL))
        fn     = rec.get(FILENAME_COL)
        prompt = rec.get(PROMPT_COL)
        neg    = NEG_DEFAULT

        print(f"\n[task {i}/{len(df)}] id={pid}")
        if not fn or (not DRY_RUN_MINIMAL and not prompt):
            print(" skip (missing data)"); fail += 1; continue

        src_img = fn  # 绝对/相对路径皆可
        if not os.path.exists(src_img):
            print(" skip (file not found)"); fail += 1; continue

        # 复制到 input 根，并取复制后的绝对路径
        img_in_input = ensure_in_input(src_img, inp_dir)
        out_pref     = OUT_PREFIX_FMT.format(id=pid)

        # 注入：仅对 LoadImage/Load Image 写入 basename
        wf = (
            {"1": {"class_type": "LoadImage", "inputs": {"image": os.path.basename(img_in_input)}},
             "2": {"class_type": "SaveImage", "inputs": {"filename_prefix": out_pref, "images": ["1", 0]}}}
            if DRY_RUN_MINIMAL else
            inject_params(wf_template, img_in_input, prompt, neg, out_pref)
        )

        # === 唯一改动：从 Excel 读取 qwen_image_filenames 用作保存基名 ===
        qwen_name = str(rec.get("qwen_image_filenames") or "").strip()
        prefer_basename = qwen_name

        # 打印关键注入信息
        print("  [debug] inputs going into workflow:")
        print(f"    image_src : {src_img}")
        print(f"    copied_to : {img_in_input}")
        print(f"    used_name : {os.path.basename(img_in_input)}  # for LoadImage")
        print(f"    out_prefix: {out_pref}")
        print(f"    save_name : {prefer_basename}{{ext}}  # 输出落地到 out_step2 时采用")
        print(f"    prompt    : {str(prompt)[:160]}")
        print(f"    neg       : {str(neg)[:160]}")

        try:
            print(" queueing...")
            r = sess.post(
                f"{COMFY_HOST}/prompt",
                json={"prompt": wf, "client_id": client_id},
                timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT),
            )
            r.raise_for_status()
            job_id = r.json().get("prompt_id")
            print(f" queued id={job_id}")

            status, dur, hist = wait_history(sess, job_id)
            if status == "ok":
                durations.append(dur)
                # 下载输出到 ./out_step2，用 prefer_basename 命名
                saved_paths = save_outputs(sess, hist, SAVE_DIR, prefer_basename=prefer_basename)
                if saved_paths:
                    print(f" done ✅ ({dur:.1f}s) saved ->")
                    for p in saved_paths:
                        print(f"    {p}")
                else:
                    print(f" done ✅ ({dur:.1f}s) [无可下载的输出条目]")
                ok += 1
            else:
                print(f" error: status={status} ({dur:.1f}s)")
                print_error_messages(hist)
                # 落盘 prompt/history 便于复现
                with open(os.path.join(DEBUG_DIR, f"{pid}_prompt.json"), "w", encoding="utf-8") as f:
                    json.dump(wf, f, ensure_ascii=False, indent=2)
                with open(os.path.join(DEBUG_DIR, f"{pid}_history.json"), "w", encoding="utf-8") as f:
                    json.dump(hist, f, ensure_ascii=False, indent=2)
                fail += 1
        except Exception as e:
            print(f" exception: {e}")
            try:
                with open(os.path.join(DEBUG_DIR, f"{pid}_prompt.json"), "w", encoding="utf-8") as f:
                    json.dump(wf, f, ensure_ascii=False, indent=2)
            except Exception:
                pass
            fail += 1

    # 平均耗时
    if durations:
        avg = sum(durations) / len(durations)
        print(f"\n[info] 平均渲染时间：{avg:.2f} 秒/张")

    print(f"\nFinished. success={ok}, fail={fail}. 输出图片已保存到: {SAVE_DIR}")

if __name__ == '__main__':
    main()
