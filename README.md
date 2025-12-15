# yoyo_image_gen_mbti

面向电商商品图的“背景生成/风格控制”实验脚本集合：基于商品类目（super category）与人格设定（MBTI / Big Five）自动生成背景 prompt，调用 ComfyUI 进行图像编辑/渲染，并输出对比图与模板成品图。

本仓库主要是“一组可跑的流水线脚本”，默认依赖两类本地服务：
- **Ollama**：用于调用 `qwen2.5vl` 生成短标题与背景描述（Step1/Step2）
- **ComfyUI**：用于按工作流 `promo_banner_v3_api.json` 进行渲染（Step2 渲染阶段）

---

## 流程概览（从原始 CSV 到对比图）

1. **Step1 标题**：`create_promo_titles.py`  
   从 `白底商品信息类目.csv` 读取商品信息与图片 URL，生成短标题 `promo_title_final`，并保存“白底/原图”到 `out_step1/`，输出 `out_step1/step1_titles.xlsx`。
2. **Step2 Prompt**：`create_categorical_prompts.py`  
   依据 `step_one_to_super_category_map.csv` 把一级类目映射到大类 `super_category`，可选 triad 路由（`step_one_triad_prompts_22cats.csv` + `step_one_background_description.csv`），并附加 MBTI/Big Five persona，输出 `out_step1/step1_prompts_<exp>.xlsx`。
3. **白底归一化**：`normalize_scale_and_canvas.py`  
   重新下载原图并抠图居中到固定画布（默认 `800x800`），写回 Excel 的 `white_bg_image`，并给 `qwen_image_filenames` 加尺寸后缀。
4. **ComfyUI 渲染**：`render_with_comfyui.py`  
   读取 `white_bg_image` + `prompt`，调用 ComfyUI API 渲染，结果保存到 `out_step2/<exp>/`。
5. **对比图**：`merge_pairs.py`  
   将白底图与生成图左右拼接，底部附加 `super_category / ori_title / promo_title_final / prompt`，输出到 `out_step3/<exp>/{id}_pair.jpg`。

---

## 一键跑流水线（推荐）

### MBTI（16 型）
脚本：`run_mbti_pipeline.py`  
特点：会遍历 MBTI 列表，为每个 MBTI 生成一套 prompts / 渲染结果 / 对比图。

```bash
python run_mbti_pipeline.py \
  --use-experiment-csv \
  --prompt-model 32b \
  --mbti-plan A \
  --mbti-mode concat \
  --per-category 10 \
  --resume
```

重要参数（常用）：
- `--categories`：逗号/换行分隔的大类列表（默认内置 14 类）
- `--per-category`：每个大类抽样数（`<=0` 为全量）
- `--exp-prefix`：输出实验前缀（会自动加 `_mbti` 后缀）
- `--resume`：若中间产物已存在则跳过
- `--skip-render / --skip-pairs`：只做前处理或只做渲染
- `--skip-kill-ollama`：默认会在渲染前 `pkill -9 ollama` 释放显存
- `--use-experiment-csv`：Step1 直接读取已筛选样本 `白底商品信息类目_experiment.csv`（可用 `--experiment-csv` 改路径）
- `--dry-run`：只打印将执行的命令

### Big Five（单维 10 组 / 全组合 32 组）
脚本：`run_big_five_pipeline.py`

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

## 分步运行（调试/定制时有用）

1) Step1：生成短标题与白底图路径
```bash
python create_promo_titles.py --model 7b --csv-path "白底商品信息类目.csv" --out-dir out_step1
```

2) Step2：生成背景 prompts（可选 MBTI / Big Five / triad）
```bash
python create_categorical_prompts.py \
  --model 32b \
  --persona-kind mbti \
  --mbti-plan A \
  --mbti-type ENFJ \
  --mbti-mode inline \
  --exp-name planA32b_enfj
```

3) 白底归一化（会写回 Excel）
```bash
python normalize_scale_and_canvas.py --excel out_step1/step1_prompts_planA32b_enfj.xlsx --out-dir out_step1
```

4) ComfyUI 渲染（输出到 `out_step2/<exp>/`）
```bash
python render_with_comfyui.py \
  --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx \
  --exp-name planA32b_enfj \
  --output-root out_step2
```

5) 合成对比图（输出到 `out_step3/<exp>/`）
```bash
python merge_pairs.py \
  --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx \
  --generated-dir out_step2/planA32b_enfj \
  --output-dir out_step3/planA32b_enfj
```

---

## 输入数据与配置文件

核心输入文件（默认文件名见各脚本顶部常量）：
- `白底商品信息类目.csv`：原始商品表（CSV/XLSX 均可）
- `白底商品信息类目_experiment.csv`：已筛选样本的商品表（例如 140 张版本；供 `--use-experiment-csv` 使用）
  - 常用列：`id`, `ori_title`, `brand`（或 `creative_id_brand`）, `image_url`, `level_one_category_name`
  - 可选列：`price`, `promotion`（用于 `add_template_*.py` 写模板文案）
- `step_one_to_super_category_map.csv`：类目映射表
  - 列：`level_one_category_name` → `super_category`
- `step_one_triad_prompts_22cats.csv`：triad 路由表（可选）
  - 列：`Category`, `Style Priority 1/2/3`
- `step_one_background_description.csv`：背景风格描述（triad 会引用）
  - 列：`background style`, `description`
- `mbti_profiles.csv` / `big_five_profiles.csv`：persona 配置
  - 用于把 persona 信息并回 Step1 表，或在 `--mbti-type/--big5-types` 指定时整表使用同一 persona

---

## 输出目录说明

- `out_step1/`
  - `step1_titles.xlsx`：Step1 标题结果
  - `step1_prompts_<exp>.xlsx`：Step2 prompts 结果（含 `prompt/super_category/qwen_image_filenames/white_bg_image` 等列）
  - `*_800x800.jpg`：归一化后的白底图
- `out_step2/<exp>/`：ComfyUI 渲染输出（文件名通常来自 Excel 的 `qwen_image_filenames`）
- `out_step3/<exp>/`：对比图 `{id}_pair.jpg`

辅助脚本：
- `rename_pairs_with_category.py`：把 `{id}_pair.jpg` 重命名为 `{id}_{super_category}(_MBTI)_pair.jpg`，便于按类目管理
- `add_template_16_9.py` / `add_template_3_2.py` / `add_template_39_40.py`：把渲染图贴进对应模板并写入文案，输出到 `output_*`

示例：
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

## 依赖与服务（运行前准备）

### Python 依赖（脚本用到的主要三方库）
本仓库未提供 `requirements.txt`；按脚本 import，常用依赖包括：
- `pandas`, `openpyxl`
- `Pillow`
- `requests`, `urllib3`, `chardet`
- `tqdm`
- `numpy`
- `opencv-python`（`normalize_scale_and_canvas.py` 需要）

### Ollama（Step1/Step2）
默认地址：`http://localhost:11434`（写在 `create_promo_titles.py` / `create_categorical_prompts.py`）  
需要提前拉取/启动对应模型（示例）：
```bash
ollama pull qwen2.5vl:7b
ollama pull qwen2.5vl:32b
ollama serve
```

### ComfyUI（渲染）
默认地址：`http://localhost:8000`（写在 `render_with_comfyui.py`）  
并确保工作流文件 `promo_banner_v3_api.json` 与 ComfyUI 环境/节点兼容。

`render_with_comfyui.py` 会：
- 调用 `/system_stats` 检查服务
- 解析 ComfyUI 启动参数中的 `--input-directory`，并把 `white_bg_image` 复制到 ComfyUI 的 input 根目录（LoadImage 只按文件名找）

（可选）`download_qwen_models.py` 提供了一个示例下载脚本，用于把 Qwen 相关模型文件下载到某个 ComfyUI 安装目录下（脚本内路径是硬编码的，使用前请按你的 ComfyUI 目录修改）。

---

## 常见问题

- **渲染阶段报找不到图片**：确认 ComfyUI 的 input 目录可写；`render_with_comfyui.py` 会把源图复制到 input 根目录并仅把 basename 注入到 `LoadImage`。
- **中文字体显示为方块**：`merge_pairs.py`/`add_template_*.py` 依赖系统字体；可在脚本里把字体路径改成你机器上可用的中文字体（macOS 可用 `PingFang.ttc`）。
- **脚本中途停了想续跑**：优先用 `run_mbti_pipeline.py` / `run_big_five_pipeline.py` 的 `--resume`，或按步骤复用已有 `out_step1/*.xlsx` 与 `out_step2/<exp>/`。
