#!/usr/bin/env python3
import os
import requests
from tqdm import tqdm

# ---- 必须加在最顶部（防系统盘爆满） ----
os.environ["TMPDIR"] = "/root/aicloud-data/tmp"
os.environ["TEMP"] = "/root/aicloud-data/tmp"
os.environ["TMP"] = "/root/aicloud-data/tmp"
os.makedirs("/root/aicloud-data/tmp", exist_ok=True)

# -------------------------------
# 配置：你的 ComfyUI 路径
# -------------------------------
COMFYUI_DIR = "/workspace/ComfyUI"
MODEL_DIR = f"{COMFYUI_DIR}/models"
VAE_DIR = f"{MODEL_DIR}/vae"

os.makedirs(VAE_DIR, exist_ok=True)

# -------------------------------
# 你的全部模型 URL（你给的）
# -------------------------------
FILES = {
    "qwen_image_edit_fp8_e4m3fn.safetensors":
    "https://huggingface.co/Comfy-Org/Qwen-Image-Edit_ComfyUI/resolve/main/split_files/diffusion_models/qwen_image_edit_fp8_e4m3fn.safetensors",

    "qwen_2.5_vl_7b_fp8_scaled.safetensors":
    "https://huggingface.co/Comfy-Org/Qwen-Image_ComfyUI/resolve/main/split_files/text_encoders/qwen_2.5_vl_7b_fp8_scaled.safetensors",

    "Qwen-Image-Lightning-4steps-V2.0.safetensors":
    "https://huggingface.co/lightx2v/Qwen-Image-Lightning/resolve/main/Qwen-Image-Lightning-4steps-V2.0.safetensors",

    "qwen_image_vae.safetensors":
    "https://huggingface.co/Comfy-Org/Qwen-Image_ComfyUI/resolve/main/split_files/vae/qwen_image_vae.safetensors",
}

def download_with_resume(url, filepath):
    temp = filepath + ".part"
    headers = {}
    pos = 0

    if os.path.exists(temp):
        pos = os.path.getsize(temp)
        headers["Range"] = f"bytes={pos}-"

    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0)) + pos

        with open(temp, "ab") as f, tqdm(
            total=total,
            initial=pos,
            unit="B",
            unit_scale=True,
            desc=os.path.basename(filepath),
        ) as bar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    os.rename(temp, filepath)

def main():
    print("\n=== 开始下载模型文件 ===\n")
    for name, url in FILES.items():
        out = f"{VAE_DIR}/{name}"
        print(f"➡ 下载: {name}")
        download_with_resume(url, out)
        print(f"✔ 完成: {out}\n")

    print("\n🎉 全部模型下载完毕！\n")

if __name__ == "__main__":
    main()
