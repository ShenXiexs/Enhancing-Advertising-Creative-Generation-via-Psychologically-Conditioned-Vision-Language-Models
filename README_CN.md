# yoyo_image_gen_mbti

面向电商商品图的“背景生成/风格控制”实验脚本集合：基于商品类目（super category）与人格设定（MBTI / Big Five / Schwartz）自动生成背景 prompt，调用 ComfyUI 进行图像编辑/渲染，并输出对比图与模板成品图。

本仓库是一套可直接运行的脚本流水线，默认依赖两类本地服务：
- **Ollama**：用 `qwen2.5vl` 生成短标题与背景 prompt（Step1/Step2）
- **ComfyUI**：按工作流 `promo_banner_v3_api.json` 进行渲染（Step2 渲染阶段）

---

## 流程概览（从原始 CSV 到对比图）

1) **Step1 标题**（`create_promo_titles.py`）
   - 读取商品 CSV/XLSX（如 `白底商品信息类目.csv`）
   - 生成 `promo_title_final`
   - 保存白底/原图到 `out_step1/`
   - 输出 `out_step1/step1_titles.xlsx`

2) **Step2 Prompt**（`create_categorical_prompts.py`）
   - 依据 `step_one_to_super_category_map.csv` 映射 `super_category`
   - 可选 triad 路由：`step_one_triad_prompts_22cats.csv` + `step_one_background_description.csv`
   - 可选 persona：MBTI / Big Five / Schwartz
   - 输出 `out_step1/step1_prompts_<exp>.xlsx`

3) **白底归一化**（`normalize_scale_and_canvas.py`）
   - 重新下载原图，抠图并居中到固定画布（默认 `800x800`）
   - 写回 `white_bg_image` 并更新 `qwen_image_filenames`

4) **ComfyUI 渲染**（`render_with_comfyui.py`）
   - 读取 `white_bg_image` + `prompt`
   - 调用 ComfyUI API
   - 输出到 `out_step2/<exp>/`

5) **对比图合成**（`merge_pairs.py`）
   - 将白底图与生成图左右拼接
   - 底部附加 `super_category / ori_title / promo_title_final / prompt`
   - 输出 `out_step3/<exp>/{id}_pair.jpg`

---

## 一键流水线（推荐）

### MBTI（16 型）
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

### Big Five（单维 10 组 + 全组合 32 组）
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

### 小样本基线（无 persona + Big Five + Schwartz，各 10 商品）
```bash
python run_big5_schwartz_small_pipeline.py \
  --prompt-model 32b \
  --persona-mode both \
  --style-constraints on \
  --end-with-4k on
```

### 无 persona 基线
```bash
python run_no_persona_pipeline.py \
  --use-experiment-csv \
  --prompt-model 32b \
  --style-constraints on \
  --end-with-4k on
```

常用参数：
- `--categories`：逗号/换行分隔的大类列表
- `--per-category`：每个大类抽样数（`<=0` 为全量）
- `--resume`：复用已有中间产物
- `--skip-render / --skip-pairs`：只跑前处理或只做渲染
- 默认会在 ComfyUI 渲染前执行 `pkill -9 ollama` 释放显存
- `--skip-kill-ollama`：跳过该释放步骤
- `--style-constraints on|off`：是否保留风格约束尾巴
- `--end-with-4k on|off`：是否要求 prompt 以 `4k` 结尾

---

## 分步运行（调试/定制）

1) Step1 标题
```bash
python create_promo_titles.py \
  --model 7b \
  --csv-path "白底商品信息类目.csv" \
  --out-dir out_step1
```

2) Step2 Prompt（可选 persona）
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

3) 白底归一化
```bash
python normalize_scale_and_canvas.py \
  --excel out_step1/step1_prompts_planA32b_enfj.xlsx \
  --out-dir out_step1
```

4) ComfyUI 渲染
```bash
python render_with_comfyui.py \
  --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx \
  --exp-name planA32b_enfj \
  --output-root out_step2
```

5) 合成对比图
```bash
python merge_pairs.py \
  --prompts-file out_step1/step1_prompts_planA32b_enfj.xlsx \
  --generated-dir out_step2/planA32b_enfj \
  --output-dir out_step3/planA32b_enfj
```

---

## Step2 Prompt 控制项

`create_categorical_prompts.py` 支持：
- `--persona-kind`：`none | mbti | big5 | schwartz | auto`
- Persona 拼接模式：`--mbti-mode/--big5-mode/--schwartz-mode` = `concat | inline`
  - `concat`：最终文本改写为 `prompt: <Target Audience...> <scene>`
    - 对 Big Five / Schwartz 的 `target` 风格，target audience 会前置在场景描述之前
  - `inline`：persona 作为 system prompt 的一部分
- Persona 文案风格：
  - Big Five：`--big5-persona-style legacy|target`
  - Schwartz：`--schwartz-persona-style legacy|target`
- Triad 路由：`--disable-triad` 可跳过类目风格提示
- Tail 控制：
  - `--style-constraints on|off`（风格约束）
  - `--end-with-4k on|off`（结尾 4k）
- `--seed`：设置 Python/NumPy/Torch 种子（Ollama 仍有随机性）

详见 `PROMPT_GENERATION_BIG5_SCHWARTZ.md`。

---

## 输入数据与配置文件

核心输入（默认文件名见脚本顶部常量）：
- `白底商品信息类目.csv`：原始商品表（CSV/XLSX）
  - 必需列：`id`, `ori_title`, `brand`（或 `creative_id_brand`）, `image_url`, `level_one_category_name`
  - 可选列：`price`, `promotion`（用于 `add_template_*.py`）
- `白底商品信息类目_experiment.csv`：已筛选样本
- `step_one_to_super_category_map.csv`：一级类目 → 大类
- `step_one_triad_prompts_22cats.csv`：triad 路由表
- `step_one_background_description.csv`：背景风格说明
- `mbti_profiles.csv` / `big_five_profiles.csv` / `schwartz_value_profiles.csv`：persona 配置

---

## 输出目录

- `out_step1/`
  - `step1_titles.xlsx`
  - `step1_prompts_<exp>.xlsx`
  - `*_800x800.jpg` 白底归一化图片
- `out_step2/<exp>/`：ComfyUI 渲染结果
- `out_step3/<exp>/`：对比图

---

## 辅助脚本

- `rename_pairs_with_category.py`：重命名 `{id}_pair.jpg` → `{id}_{super_category}(_MBTI)_pair.jpg`
- `add_template_16_9.py` / `add_template_3_2.py` / `add_template_39_40.py`：模板合成
- `download_qwen_models.py`：示例模型下载脚本（路径需自行修改）

---

## 依赖与服务

Python 依赖（仓库未提供 `requirements.txt`）：
- `pandas`, `openpyxl`
- `Pillow`
- `requests`, `urllib3`, `chardet`
- `tqdm`, `numpy`
- `opencv-python`（`normalize_scale_and_canvas.py` 需要）

Ollama（Step1/Step2）：
```bash
ollama pull qwen2.5vl:7b
ollama pull qwen2.5vl:32b
ollama serve
```

ComfyUI（渲染）：
- 默认 API：`http://localhost:8000`
- 工作流：`promo_banner_v3_api.json`
- `render_with_comfyui.py` 会将源图复制到 ComfyUI input 根目录，LoadImage 仅按文件名解析

---

## 常见问题

- **渲染阶段找不到图片**：确认 ComfyUI input 目录可写；脚本会复制源图并仅注入 basename。
- **中文字体显示为方块**：在 `merge_pairs.py` / `add_template_*.py` 中替换为可用的中文字体路径。
- **中断后续跑**：优先使用 `--resume`，或复用已有 `out_step1/*.xlsx` 与 `out_step2/<exp>/`。
