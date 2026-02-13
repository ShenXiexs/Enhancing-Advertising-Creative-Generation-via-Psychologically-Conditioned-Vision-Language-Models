# yoyo_image_gen_mbti

Research scripts for **background generation / style control** on e-commerce product images. The pipeline builds background prompts from product categories and personas (MBTI / Big Five / Schwartz), calls ComfyUI for image editing, and outputs comparison pairs and template posters.

This repo is a runnable script pipeline that relies on two local services by default:
- **Ollama**: generates Step1 short titles and Step2 background prompts with `qwen2.5vl`
- **ComfyUI**: renders images using `promo_banner_v3_api.json`

---

## Pipeline at a Glance

1) **Step1 Titles** (`create_promo_titles.py`)
   - Reads product CSV/XLSX (e.g., `白底商品信息类目.csv`)
   - Generates `promo_title_final`
   - Saves white-bg/original images to `out_step1/`
   - Outputs `out_step1/step1_titles.xlsx`

2) **Step2 Prompts** (`create_categorical_prompts.py`)
   - Maps level-one categories to `super_category` using `step_one_to_super_category_map.csv`
   - Optional triad routing: `step_one_triad_prompts_22cats.csv` + `step_one_background_description.csv`
   - Optional persona injection: MBTI / Big Five / Schwartz
   - Outputs `out_step1/step1_prompts_<exp>.xlsx`

3) **White-BG Normalization** (`normalize_scale_and_canvas.py`)
   - Re-downloads originals, centers product on fixed canvas (default `800x800`)
   - Writes back `white_bg_image` and updates `qwen_image_filenames`

4) **ComfyUI Rendering** (`render_with_comfyui.py`)
   - Reads `white_bg_image` + `prompt`
   - Calls ComfyUI API
   - Outputs to `out_step2/<exp>/`

5) **Comparison Pairs** (`merge_pairs.py`)
   - Concats white-bg and generated images
   - Adds labels (`super_category / ori_title / promo_title_final / prompt`)
   - Outputs `out_step3/<exp>/{id}_pair.jpg`

---

## Quickstart (One-Click Pipelines)

### MBTI (16 types)
```bash
python run_mbti_pipeline.py \
  --use-experiment-csv \
  --prompt-model 32b \
  --mbti-plan A \
  --mbti-mode concat \
  --per-category 10 \
  --style-constraints on \
  --end-with-4k on \
  --resume
```

### Big Five (10 single-dimension + 32 combos)
```bash
python run_big_five_pipeline.py \
  --use-experiment-csv \
  --profile-set single \
  --prompt-model 32b \
  --big5-plan A \
  --big5-mode concat \
  --per-category 10 \
  --style-constraints on \
  --end-with-4k on \
  --resume
```

### Schwartz Values
```bash
python run_schwartz_value_pipeline.py \
  --use-experiment-csv \
  --prompt-model 32b \
  --schwartz-persona-style target \
  --style-constraints on \
  --end-with-4k on \
  --resume
```

### Small benchmark (no persona + Big Five + Schwartz, 10 products)
```bash
python run_big5_schwartz_small_pipeline.py \
  --prompt-model 32b \
  --persona-mode both \
  --style-constraints on \
  --end-with-4k on
```

### No-persona baseline
```bash
python run_no_persona_pipeline.py \
  --use-experiment-csv \
  --prompt-model 32b \
  --style-constraints on \
  --end-with-4k on
```

Common flags:
- `--categories`: comma/newline separated super categories
- `--per-category`: samples per category (`<=0` keeps all)
- `--resume`: reuse existing outputs
- `--skip-render / --skip-pairs`: stop before render or pair stage
- Auto GPU release: pipeline runs `pkill -9 ollama` before ComfyUI rendering by default
- `--skip-kill-ollama`: skip that `pkill -9 ollama` step
- `--render-observe-prompt`: print prompt observability stats during rendering
- `--render-save-debug-artifacts off|error|all`: save debug files for render requests
- `--render-debug-dir`: debug artifact root (default: `api_debug`)
- `--style-constraints on|off`: include style constraint tail
- `--end-with-4k on|off`: require prompts to end with `4k`

---

## Step-by-Step (Manual)

1) Step1 titles
```bash
python create_promo_titles.py \
  --model 7b \
  --csv-path "白底商品信息类目.csv" \
  --out-dir out_step1
```

2) Step2 prompts (persona optional)
```bash
python create_categorical_prompts.py \
  --model 32b \
  --persona-kind mbti \
  --mbti-plan A \
  --mbti-type ENFJ \
  --mbti-mode inline \
  --style-constraints on \
  --end-with-4k on \
  --exp-name planA32b_enfj
```

3) Normalize white background
```bash
python normalize_scale_and_canvas.py \
  --excel out_step1/step1_prompts_planA32b_enfj.xlsx \
  --out-dir out_step1
```

4) Render via ComfyUI
```bash
python render_with_comfyui.py \
  --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx \
  --exp-name planA32b_enfj \
  --output-root out_step2 \
  --observe-prompt \
  --save-debug-artifacts all \
  --debug-dir api_debug
```

5) Merge comparison pairs
```bash
python merge_pairs.py \
  --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx \
  --generated-dir out_step2/planA32b_enfj \
  --output-dir out_step3/planA32b_enfj
```

---

## Prompt Controls (Step2)

`create_categorical_prompts.py` supports:
- `--persona-kind`: `none | mbti | big5 | schwartz | auto`
- Persona modes: `--mbti-mode/--big5-mode/--schwartz-mode` = `concat | inline`
  - `concat`: final prompt is rewritten to `prompt: <Target Audience ...> <scene description>`
    - For Big Five/Schwartz `target` style, audience text is prepended before scene text
    - This keeps persona cues in the front of the actual text sent to ComfyUI
  - `inline`: persona text inserted into system prompt
- Persona wording style:
  - Big Five: `--big5-persona-style legacy|target`
  - Schwartz: `--schwartz-persona-style legacy|target`
- Triad routing: `--disable-triad` to skip category-driven style hints
- Style tail controls:
  - `--style-constraints on|off` (cinematic lighting / props / no people, etc.)
  - `--end-with-4k on|off` (require final `4k` suffix)
- Seed: `--seed` sets Python/NumPy/Torch seeds (Ollama output still stochastic)

More details: `PROMPT_GENERATION_BIG5_SCHWARTZ.md`.

---

## Inputs & Config Files

Core inputs (defaults defined at top of scripts):
- `白底商品信息类目.csv`: raw product table (CSV/XLSX)
  - Required: `id`, `ori_title`, `brand` (or `creative_id_brand`), `image_url`, `level_one_category_name`
  - Optional: `price`, `promotion` (used by `add_template_*.py`)
- `白底商品信息类目_experiment.csv`: filtered sample table for quick runs
- `step_one_to_super_category_map.csv`: level-one → super category mapping
- `step_one_triad_prompts_22cats.csv`: triad routing table (`Category`, `Style Priority 1/2/3`)
- `step_one_background_description.csv`: style descriptions (`background style`, `description`)
- `mbti_profiles.csv` / `big_five_profiles.csv` / `schwartz_value_profiles.csv`: persona profiles

---

## Outputs

- `out_step1/`
  - `step1_titles.xlsx`
  - `step1_prompts_<exp>.xlsx`
  - `*_800x800.jpg` normalized white-bg images
- `out_step2/<exp>/`: ComfyUI renders
- `out_step3/<exp>/`: comparison pairs

---

## Utilities

- `rename_pairs_with_category.py`: rename `{id}_pair.jpg` → `{id}_{super_category}(_MBTI)_pair.jpg`
- `add_template_16_9.py` / `add_template_3_2.py` / `add_template_39_40.py`: place renders into templates
- `download_qwen_models.py`: sample downloader (paths are hardcoded; edit before use)

---

## Dependencies & Services

Python packages commonly used (no `requirements.txt` included):
- `pandas`, `openpyxl`
- `Pillow`
- `requests`, `urllib3`, `chardet`
- `tqdm`, `numpy`
- `opencv-python` (required by `normalize_scale_and_canvas.py`)

Ollama (Step1/Step2):
```bash
ollama pull qwen2.5vl:7b
ollama pull qwen2.5vl:32b
ollama serve
```

ComfyUI (rendering):
- Default API: `http://localhost:8000`
- Workflow: `promo_banner_v3_api.json`
- `render_with_comfyui.py` copies source images into ComfyUI input root; `LoadImage` resolves by filename

---

## Troubleshooting

- **Render can’t find images**: ensure ComfyUI input dir is writable. The script copies source images there and injects only basenames.
- **Chinese text renders as tofu**: update font paths in `merge_pairs.py` / `add_template_*.py` to a valid CJK font on your machine.
- **Resume after interruption**: use `--resume` in pipeline scripts or reuse existing `out_step1/*.xlsx` and `out_step2/<exp>/` manually.
