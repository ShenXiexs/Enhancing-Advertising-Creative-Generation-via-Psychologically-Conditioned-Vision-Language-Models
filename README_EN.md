# yoyo_image_gen_mbti

A set of experiment scripts for **background generation / style control** on e‑commerce product images. The pipeline builds background prompts from product categories (super category) and personas (MBTI / Big Five), calls ComfyUI for image editing/rendering, and outputs comparison images plus template-based final posters.

This repository is mainly a **runnable pipeline of scripts**, and by default relies on two local services:
- **Ollama**: generates short titles and background descriptions with `qwen2.5vl` (Step1/Step2)
- **ComfyUI**: renders images using the workflow `promo_banner_v3_api.json` (Step2 render stage)

---

## Pipeline Overview (from CSV to comparison images)

1. **Step1 Titles**: `create_promo_titles.py`  
   Reads product info and image URLs from `白底商品信息类目.csv`, generates short titles `promo_title_final`, saves white‑background/original images to `out_step1/`, and outputs `out_step1/step1_titles.xlsx`.
2. **Step2 Prompts**: `create_categorical_prompts.py`  
   Maps level‑one categories to `super_category` using `step_one_to_super_category_map.csv`. Optionally applies triad routing (`step_one_triad_prompts_22cats.csv` + `step_one_background_description.csv`) and appends MBTI/Big Five personas. Outputs `out_step1/step1_prompts_<exp>.xlsx`.
3. **White‑BG Normalization**: `normalize_scale_and_canvas.py`  
   Re‑downloads original images, extracts the product, centers on a fixed canvas (default `800x800`), writes back to the Excel `white_bg_image` column, and appends size suffixes to `qwen_image_filenames`.
4. **ComfyUI Rendering**: `render_with_comfyui.py`  
   Reads `white_bg_image` + `prompt`, calls the ComfyUI API, and saves outputs to `out_step2/<exp>/`.
5. **Comparison Pairs**: `merge_pairs.py`  
   Concatenates white‑background images with generated results side‑by‑side and adds labels (`super_category / ori_title / promo_title_final / prompt`) at the bottom. Outputs `out_step3/<exp>/{id}_pair.jpg`.

---

## One‑click Pipeline (recommended)

### MBTI (16 types)
Script: `run_mbti_pipeline.py`  
Behavior: iterates over the MBTI list and generates prompts / renders / comparison pairs for each type.

```bash
python run_mbti_pipeline.py \
  --use-experiment-csv \
  --prompt-model 32b \
  --mbti-plan A \
  --mbti-mode concat \
  --per-category 10 \
  --resume
```

Common parameters:
- `--categories`: comma/newline separated super categories (default: built‑in 14 categories)
- `--per-category`: samples per category (`<=0` means all)
- `--exp-prefix`: output experiment prefix (auto adds `_mbti` suffix)
- `--resume`: skip steps if intermediate outputs already exist
- `--skip-render / --skip-pairs`: do pre‑processing only or render only
- `--skip-kill-ollama`: by default it runs `pkill -9 ollama` before rendering to free VRAM
- `--use-experiment-csv`: Step1 reads `白底商品信息类目_experiment.csv` directly (use `--experiment-csv` to change path)
- `--dry-run`: print commands without executing

### Big Five (single‑dimension 10 groups / full combinations 32 groups)
Script: `run_big_five_pipeline.py`

```bash
python run_big_five_pipeline.py \
  --use-experiment-csv \
  --profile-set single \
  --prompt-model 32b \
  --big5-plan A \
  --big5-mode concat \
  --per-category 10 \
  --resume
```

---

## Run Step‑by‑Step (useful for debugging/customization)

1) Step1: generate short titles and white‑background image paths
```bash
python create_promo_titles.py --model 7b --csv-path "白底商品信息类目.csv" --out-dir out_step1
```

2) Step2: generate background prompts (optional MBTI / Big Five / triad)
```bash
python create_categorical_prompts.py \
  --model 32b \
  --persona-kind mbti \
  --mbti-plan A \
  --mbti-type ENFJ \
  --mbti-mode inline \
  --exp-name planA32b_enfj
```

3) White‑BG normalization (writes back into Excel)
```bash
python normalize_scale_and_canvas.py --excel out_step1/step1_prompts_planA32b_enfj.xlsx --out-dir out_step1
```

4) ComfyUI rendering (outputs to `out_step2/<exp>/`)
```bash
python render_with_comfyui.py \
  --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx \
  --exp-name planA32b_enfj \
  --output-root out_step2
```

5) Merge comparison pairs (outputs to `out_step3/<exp>/`)
```bash
python merge_pairs.py \
  --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx \
  --generated-dir out_step2/planA32b_enfj \
  --output-dir out_step3/planA32b_enfj
```

---

## Inputs & Config Files

Core input files (defaults are defined at the top of each script):
- `白底商品信息类目.csv`: raw product table (CSV/XLSX)
- `白底商品信息类目_experiment.csv`: filtered sample table (e.g. 140‑image subset for `--use-experiment-csv`)
  - Common columns: `id`, `ori_title`, `brand` (or `creative_id_brand`), `image_url`, `level_one_category_name`
  - Optional columns: `price`, `promotion` (used by `add_template_*.py`)
- `step_one_to_super_category_map.csv`: category mapping
  - Columns: `level_one_category_name` → `super_category`
- `step_one_triad_prompts_22cats.csv`: triad routing table (optional)
  - Columns: `Category`, `Style Priority 1/2/3`
- `step_one_background_description.csv`: background style descriptions (used by triad)
  - Columns: `background style`, `description`
- `mbti_profiles.csv` / `big_five_profiles.csv`: persona definitions
  - Used to merge persona text into Step1 outputs, or applied uniformly via `--mbti-type/--big5-types`

---

## Output Directories

- `out_step1/`
  - `step1_titles.xlsx`: Step1 title results
  - `step1_prompts_<exp>.xlsx`: Step2 prompts output (includes `prompt/super_category/qwen_image_filenames/white_bg_image` columns)
  - `*_800x800.jpg`: normalized white‑background images
- `out_step2/<exp>/`: ComfyUI render outputs (filenames usually come from `qwen_image_filenames`)
- `out_step3/<exp>/`: comparison pairs `{id}_pair.jpg`

Helper scripts:
- `rename_pairs_with_category.py`: rename `{id}_pair.jpg` to `{id}_{super_category}(_MBTI)_pair.jpg`
- `add_template_16_9.py` / `add_template_3_2.py` / `add_template_39_40.py`: place renders into templates and write copy, output to `output_*`

Example:
```bash
python rename_pairs_with_category.py \
  --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx \
  --pairs-dir out_step3/planA32b_enfj \
  --append-mbti
```

```bash
python add_template_16_9.py --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx --comfy-output out_step2/planA32b_enfj
```

---

## Dependencies & Services

### Python dependencies (main third‑party packages used)
This repo does **not** include a `requirements.txt`. Based on imports, common dependencies include:
- `pandas`, `openpyxl`
- `Pillow`
- `requests`, `urllib3`, `chardet`
- `tqdm`
- `numpy`
- `opencv-python` (required by `normalize_scale_and_canvas.py`)

### Ollama (Step1/Step2)
Default: `http://localhost:11434` (defined in `create_promo_titles.py` / `create_categorical_prompts.py`)  
Pull/start models ahead of time (example):
```bash
ollama pull qwen2.5vl:7b
ollama pull qwen2.5vl:32b
ollama serve
```

### ComfyUI (Rendering)
Default: `http://localhost:8000` (defined in `render_with_comfyui.py`)  
Ensure the workflow file `promo_banner_v3_api.json` is compatible with your ComfyUI setup/nodes.

`render_with_comfyui.py` will:
- call `/system_stats` to verify the service
- parse ComfyUI launch args for `--input-directory`, then copy `white_bg_image` into the ComfyUI input root (LoadImage resolves by filename only)

(Optional) `download_qwen_models.py` is a sample downloader for Qwen models into a ComfyUI install directory (paths are hardcoded; edit to match your ComfyUI location).

---

## FAQ

- **Render stage can’t find images**: ensure ComfyUI input directory is writable; `render_with_comfyui.py` copies source images into the input root and injects only basenames into `LoadImage`.
- **Chinese text renders as tofu**: `merge_pairs.py` / `add_template_*.py` rely on system fonts. Update font paths in scripts to an available Chinese font on your machine (macOS example: `PingFang.ttc`).
- **Pipeline interrupted and want to resume**: use `--resume` in `run_mbti_pipeline.py` / `run_big_five_pipeline.py`, or reuse existing `out_step1/*.xlsx` and `out_step2/<exp>/` manually.
