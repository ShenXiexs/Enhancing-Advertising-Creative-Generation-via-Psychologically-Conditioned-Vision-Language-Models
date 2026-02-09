#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch pipeline for Schwartz Value personas:
1) Generate background prompts;
2) Sample by super category;
3) Normalize white background;
4) Render via ComfyUI;
5) Build paired comparisons.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Sequence

import pandas as pd

DRY_RUN = False


def set_dry_run(flag: bool) -> None:
    global DRY_RUN
    DRY_RUN = bool(flag)


REPO_ROOT = Path(__file__).resolve().parent
STEP1_FILENAME = "step1_titles.xlsx"
PROMPT_PREFIX = "step1_prompts"

DEFAULT_CATEGORIES = [
    "珠宝钟表奢品",
    "数码电子",
    "家电厨具",
    "食品饮料",
    "收纳日用",
    "服饰鞋包",
    "家纺软装",
    "家具",
    "户外运动装备交通",
    "母婴亲子",
    "医药保健计生",
    "美妆个护",
    "玩乐兴趣文创礼品",
    "宠物用品",
]


def read_table_auto(path: str) -> pd.DataFrame:
    fp = Path(path)
    ext = fp.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(fp)
    encodings = ("utf-8-sig", "gb18030", "utf-8", "latin1")
    for enc in encodings:
        try:
            return pd.read_csv(fp, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(fp, encoding="utf-8", errors="ignore")


def _find_col(df: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    cols = list(df.columns)
    lower_to_col = {}
    for c in cols:
        key = str(c).strip().lower()
        if key and key not in lower_to_col:
            lower_to_col[key] = c
    for cand in candidates:
        cand_key = str(cand).strip().lower()
        if cand_key and cand_key in lower_to_col:
            return lower_to_col[cand_key]
    for cand in candidates:
        cand_key = str(cand).strip().lower()
        if not cand_key:
            continue
        for c in cols:
            c_key = str(c).strip().lower()
            if cand_key in c_key:
                return c
    return None


def load_category_map(map_path: str, orig_col: str, target_col: str) -> dict:
    df = read_table_auto(map_path)
    if orig_col not in df.columns or target_col not in df.columns:
        def pick(name: str):
            name_l = str(name).strip().lower()
            exact = next((c for c in df.columns if str(c).strip().lower() == name_l), None)
            if exact:
                return exact
            partial = next((c for c in df.columns if name_l and name_l in str(c).strip().lower()), None)
            return partial

        resolved_orig = pick(orig_col)
        resolved_target = pick(target_col)
        if not resolved_orig or not resolved_target:
            cols = list(df.columns)
            if len(cols) >= 2:
                resolved_orig, resolved_target = cols[0], cols[1]
                print(
                    f"[WARN] 分类映射表缺少列名 {orig_col}/{target_col}，"
                    f"将使用前两列作为映射：{resolved_orig}/{resolved_target}",
                    flush=True,
                )
            else:
                raise ValueError(f"分类映射缺少列：{orig_col}/{target_col}")
        orig_col, target_col = resolved_orig, resolved_target

    def norm(x):
        return str(x).strip()

    mp = {}
    for _, row in df.iterrows():
        src = norm(row[orig_col])
        dst = norm(row[target_col])
        if src:
            mp[src] = dst or src
    return mp


def _split_by_commas(raw: str) -> List[str]:
    tokens = [tok.strip() for tok in re.split(r"[,\n]+", raw or "") if tok.strip()]
    return tokens


def _slugify_tag(text: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_\-]+", "_", (text or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "value"


def load_schwartz_types(path: str) -> List[str]:
    df = read_table_auto(path)
    df.columns = [str(c).strip() for c in df.columns]
    type_col = _find_col(df, ["schwartz_value_type", "value_type", "schwartz_type", "value", "type"])
    if not type_col:
        raise ValueError(
            "schwartz_value_profiles 缺少 schwartz_value_type 列，"
            f"当前列：{list(df.columns)}"
        )
    seen = set()
    values = []
    for v in df[type_col].tolist():
        s = str(v).strip()
        if not s or s.lower() == "nan":
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        values.append(s)
    return values


def _parse_schwartz_list(raw: str | None, profiles_path: str) -> List[str]:
    if raw and raw.strip():
        return _split_by_commas(raw)
    return load_schwartz_types(profiles_path)


def run_cmd(cmd: Sequence[str], desc: str) -> None:
    cmd_disp = " ".join(cmd)
    print(f"\n[RUN] {desc}\n  $ {cmd_disp}", flush=True)
    if DRY_RUN:
        print("  (dry-run) command skipped")
        return
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({desc}): {cmd_disp}")


def ensure_create_prompts_supports_schwartz(python_exe: str) -> None:
    cmd = [python_exe, "create_categorical_prompts.py", "-h"]
    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    help_text = (result.stdout or "") + "\n" + (result.stderr or "")
    if result.returncode != 0:
        raise RuntimeError(
            "无法执行 create_categorical_prompts.py 以检查参数支持，"
            f"请确认文件存在且可运行：{REPO_ROOT / 'create_categorical_prompts.py'}"
        )
    required_flags = ("--persona-kind", "--schwartz-type", "--schwartz-profiles", "--schwartz-mode",
                      "--style-constraints", "--end-with-4k")
    if not all(flag in help_text for flag in required_flags):
        raise RuntimeError(
            "当前目录下的 create_categorical_prompts.py 版本过旧，"
            "不支持 Schwartz Value 参数："
            f"{', '.join(required_flags)}。\n"
            "解决方式：在运行机器上同步最新代码（git pull / 重新拷贝仓库），"
            "或用本仓库中的 create_categorical_prompts.py 覆盖旧文件后再跑。"
        )


def prepare_step1_source(args, suffix_tag: str) -> str:
    if not args.step1_category_sample:
        return args.step1_csv
    per_cat = args.step1_category_per if args.step1_category_per and args.step1_category_per > 0 else args.per_category
    if per_cat <= 0:
        raise ValueError("step1_category_sample 需要 per-category > 0")
    subset_name = f"step1_source_{suffix_tag}.csv"
    subset_path = Path(args.prompts_dir) / subset_name
    if args.resume and subset_path.exists():
        print(f"[SKIP] 复用已有 Step1 源子集：{subset_path}")
        return str(subset_path)
    print(f"[info] Step1 源数据按大类抽样：{args.categories}，每类 {per_cat} 条")
    src_df = read_table_auto(args.step1_csv)
    cat_col = args.source_category_col
    if cat_col not in src_df.columns:
        raise ValueError(f"Step1 源数据缺少列：{cat_col}")
    cat_series = src_df[cat_col].astype(str).str.strip()
    mapping = load_category_map(args.category_map_xlsx, args.category_map_orig_col, args.category_map_target_col)
    src_df["_mapped_cat"] = cat_series.map(lambda x: mapping.get(x, x))
    filtered = src_df[src_df["_mapped_cat"].isin(args.categories)].copy()
    if filtered.empty:
        raise RuntimeError("映射后无可用数据，请检查分类映射或输入类别")
    sampled_frames = []
    missing = []
    insufficient = []
    for idx, cat in enumerate(args.categories):
        block = filtered[filtered["_mapped_cat"] == cat]
        if block.empty:
            missing.append(cat)
            continue
        if len(block) > per_cat:
            rs = args.seed + idx * 7919
            block = block.sample(n=per_cat, random_state=rs)
        elif len(block) < per_cat:
            insufficient.append((cat, len(block)))
        sampled_frames.append(block)
    if not sampled_frames:
        raise RuntimeError("指定大类在源数据中均缺失")
    subset = pd.concat(sampled_frames, ignore_index=True).drop(columns=["_mapped_cat"])
    subset.to_csv(subset_path, index=False)
    if missing:
        print(f"[WARN] 下列大类在源数据中缺失：{missing}")
    if insufficient:
        print("[WARN] 以下大类不足目标数量：" +
              ", ".join(f"{cat}({cnt}/{per_cat})" for cat, cnt in insufficient))
    print(f"[OK] Step1 源子集已写入：{subset_path} (共 {len(subset)} 条)")
    return str(subset_path)


def ensure_step1(args, source_csv: str) -> Path:
    out_dir = Path(args.prompts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    step1_excel = out_dir / STEP1_FILENAME
    if args.skip_step1 and step1_excel.exists():
        print(f"[SKIP] Step1 已存在：{step1_excel}")
        return step1_excel
    cmd = [
        sys.executable, "create_promo_titles.py",
        "--model", args.step1_model,
        "--csv-path", source_csv,
        "--out-dir", str(out_dir),
        "--sample-num", str(args.step1_sample_num),
        "--rand-seed", str(args.seed),
    ]
    run_cmd(cmd, "生成 Step1 标题 & 白底图")
    return step1_excel


def sample_categories(base_excel: Path,
                      categories: Sequence[str],
                      per_category: int,
                      seed: int,
                      suffix_tag: str) -> Path:
    df = pd.read_excel(base_excel)
    cat_col = next((c for c in ("super_category", "Category", "category", "level_one_category_name") if c in df.columns), None)
    if not cat_col:
        raise ValueError(f"{base_excel} 缺少 super_category/Category 列，无法按大类抽样")
    df["_cat_norm"] = df[cat_col].astype(str).str.strip()
    sampled_frames = []
    missing = []
    insufficient = []
    for idx, cat in enumerate(categories):
        block = df[df["_cat_norm"] == cat]
        if block.empty:
            missing.append(cat)
            continue
        if per_category > 0 and len(block) > per_category:
            rs = seed + idx * 9973
            block = block.sample(n=per_category, random_state=rs)
        elif per_category > 0 and len(block) < per_category:
            insufficient.append((cat, len(block)))
        sampled_frames.append(block)
    if not sampled_frames:
        raise RuntimeError(f"{base_excel} 在指定大类中没有可用数据")
    sampled = pd.concat(sampled_frames, ignore_index=True)
    sampled = sampled.drop(columns=["_cat_norm"], errors="ignore")
    subset_path = base_excel.with_name(f"{base_excel.stem}_{suffix_tag}.xlsx")
    sampled.to_excel(subset_path, index=False)
    if missing:
        print(f"[WARN] 以下大类在 {base_excel.name} 中找不到：{missing}")
    if insufficient:
        warn = ", ".join(f"{cat}({cnt}/{per_category})" for cat, cnt in insufficient)
        print(f"[WARN] 以下大类不足 {per_category} 条，已全部保留：{warn}")
    print(f"[OK] 已输出抽样文件：{subset_path}")
    return subset_path


def process_schwartz_value(value_type: str,
                            args,
                            categories: Sequence[str],
                            suffix_tag: str,
                            seed: int) -> dict:
    value_tag = _slugify_tag(value_type)
    exp_tag = f"{args.exp_prefix}_{value_tag}"
    prompts_path = Path(args.prompts_dir) / f"{PROMPT_PREFIX}_{exp_tag}.xlsx"

    if args.skip_prompts:
        if not prompts_path.exists():
            raise FileNotFoundError(f"[ERR] 找不到 prompts 文件，无法 --skip-prompts：{prompts_path}")
        print(f"[SKIP] --skip-prompts 生效，直接使用：{prompts_path}")
    elif args.resume and prompts_path.exists():
        print(f"[SKIP] 复用已有 Prompt：{prompts_path}")
    else:
        cmd = [
            sys.executable, "create_categorical_prompts.py",
            "--model", args.prompt_model,
            "--persona-kind", "schwartz",
            "--schwartz-profiles", args.schwartz_profiles,
            "--schwartz-type", value_type,
            "--schwartz-mode", args.schwartz_mode,
            "--schwartz-persona-style", args.schwartz_persona_style,
            "--style-constraints", args.style_constraints,
            "--end-with-4k", args.end_with_4k,
            "--exp-name", exp_tag,
            "--seed", str(seed),
        ]
        if args.disable_triad:
            cmd.append("--disable-triad")
        run_cmd(cmd, f"生成 Prompt（Schwartz={value_type})")

    subset_path: Path
    resampled = False
    if args.skip_sampling:
        subset_path = prompts_path
        subset_exists = subset_path.exists()
        if not subset_exists:
            raise FileNotFoundError(f"[ERR] --skip-sampling 需要已有文件：{subset_path}")
        print(f"[SKIP] --skip-sampling 生效，直接使用 prompts 文件：{subset_path}")
    else:
        subset_path = prompts_path.with_name(f"{prompts_path.stem}_{suffix_tag}.xlsx")
        subset_exists = subset_path.exists()
        if args.resume and subset_exists:
            print(f"[SKIP] 复用已有抽样文件：{subset_path}")
        else:
            subset_path = sample_categories(prompts_path, categories, args.per_category, seed, suffix_tag)
            resampled = True
        subset_exists = subset_path.exists()

    if args.skip_normalize:
        print(f"[SKIP] --skip-normalize 生效，未进行白底归一化：{subset_path}")
    else:
        need_norm = args.force_normalize or resampled or args.skip_sampling or not subset_exists or not args.resume
        if not need_norm and args.resume:
            print(f"[SKIP] resume 模式下沿用已有白底：{subset_path}")
        else:
            norm_cmd = [
                sys.executable, "normalize_scale_and_canvas.py",
                "--excel", str(subset_path),
                "--out-dir", args.prompts_dir,
            ]
            run_cmd(norm_cmd, f"归一化白底图（Schwartz={value_type})")

    return {"value": value_type, "exp_tag": exp_tag, "subset": subset_path}


def pkill_ollama():
    print("\n[info] 开始 pkill -9 ollama，释放显存 ...")
    if DRY_RUN:
        print("  (dry-run) 跳过 pkill ollama")
        return
    subprocess.run(["pkill", "-9", "ollama"], cwd=REPO_ROOT, check=False)


def run_render(job: dict, args) -> None:
    render_cmd = [
        sys.executable, "render_with_comfyui.py",
        "--prompts-file", str(job["subset"]),
        "--exp-name", job["exp_tag"],
        "--output-root", args.render_root,
    ]
    run_cmd(render_cmd, f"ComfyUI 渲染（Schwartz={job['value']})")


def run_pairs(job: dict, args) -> None:
    gen_dir = Path(args.render_root) / job["exp_tag"]
    pairs_dir = Path(args.pairs_root) / job["exp_tag"]
    if not DRY_RUN:
        pairs_dir.mkdir(parents=True, exist_ok=True)
    merge_cmd = [
        sys.executable, "merge_pairs.py",
        "--prompts-file", str(job["subset"]),
        "--generated-dir", str(gen_dir),
        "--output-dir", str(pairs_dir),
    ]
    run_cmd(merge_cmd, f"生成对比图（Schwartz={job['value']})")


def main():
    parser = argparse.ArgumentParser(description="Schwartz Value pipeline (Step1->Step2->Normalize->Render->Pairs)")
    parser.add_argument("--schwartz-types", default="",
                        help="Comma/newline separated Schwartz value types; empty means all from profiles.")
    parser.add_argument("--limit-schwartz", type=int, default=0,
                        help="Only process first N values (order from profiles or input list).")
    parser.add_argument("--schwartz-mode", choices=["concat", "inline"], default="inline",
                        help="Persona mode for Schwartz value prompts.")
    parser.add_argument("--schwartz-persona-style", choices=["legacy", "target"], default="legacy",
                        help="Schwartz persona wording: legacy=You prioritize; target=Target audience.")
    parser.add_argument("--schwartz-profiles", default="schwartz_value_profiles.csv",
                        help="Schwartz value profiles CSV path.")
    parser.add_argument("--categories", default=",".join(DEFAULT_CATEGORIES),
                        help="Comma or newline separated super-category list.")
    parser.add_argument("--per-category", type=int, default=10,
                        help="Samples per category; <=0 keeps all.")
    parser.add_argument("--subset-suffix", default="",
                        help="Extra suffix for sampled Excel (e.g. 14cats_10).")
    parser.add_argument("--seed", type=int, default=2026,
                        help="Global random seed.")
    parser.add_argument("--prompt-model", choices=["7b", "32b"], default="32b",
                        help="Model size for prompt generation.")
    parser.add_argument("--style-constraints", choices=["on", "off"], default="on",
                        help="Include style constraint tail in prompts (default: on).")
    parser.add_argument("--end-with-4k", choices=["on", "off"], default="on",
                        help="Require prompts to end with \"4k\" (default: on).")
    parser.add_argument("--disable-triad", dest="disable_triad", action="store_true",
                        default=True, help="Disable triad routing (default on).")
    parser.add_argument("--enable-triad", dest="disable_triad", action="store_false",
                        help="Enable triad routing.")
    parser.add_argument("--exp-prefix", default="schwartz_inline",
                        help="Output prefix; will append _<value>.")
    parser.add_argument("--prompts-dir", default="out_step1",
                        help="Step1/Step2 Excel output dir.")
    parser.add_argument("--render-root", default="out_step2",
                        help="Render output root dir.")
    parser.add_argument("--pairs-root", default="out_step3",
                        help="Pairs output root dir.")
    parser.add_argument("--step1-csv", default="白底商品信息类目.csv",
                        help="Step1 input CSV/XLSX.")
    parser.add_argument("--experiment-csv", default="白底商品信息类目_experiment.csv",
                        help="Experiment CSV/XLSX.")
    parser.add_argument("--use-experiment-csv", action="store_true",
                        help="Use experiment CSV instead of step1-csv.")
    parser.add_argument("--step1-model", choices=["7b", "32b"], default="7b",
                        help="Model size for step1 title generation.")
    parser.add_argument("--step1-sample-num", type=int, default=0,
                        help="Step1 sample rows; <=0 means all.")
    parser.add_argument("--skip-step1", action="store_true",
                        help="Skip step1 if step1_titles.xlsx exists.")
    parser.add_argument("--resume", action="store_true",
                        help="Reuse existing prompts/sampled/render outputs.")
    parser.add_argument("--skip-kill-ollama", action="store_true",
                        help="Do not pkill ollama before rendering.")
    parser.add_argument("--skip-prompts", action="store_true",
                        help="Skip step2 prompt generation (requires existing Excel).")
    parser.add_argument("--skip-sampling", action="store_true",
                        help="Skip category sampling; use full prompts Excel.")
    parser.add_argument("--skip-normalize", action="store_true",
                        help="Skip background normalization.")
    parser.add_argument("--force-normalize", action="store_true",
                        help="Force normalization even in resume mode.")
    parser.add_argument("--skip-render", action="store_true",
                        help="Skip rendering stage.")
    parser.add_argument("--skip-pairs", action="store_true",
                        help="Skip pairing stage.")
    parser.add_argument("--force-render", action="store_true",
                        help="Force render even if output exists.")
    parser.add_argument("--force-pairs", action="store_true",
                        help="Force pairs even if output exists.")
    parser.add_argument("--source-category-col", default="level_one_category_name",
                        help="Column name for level-one category in step1 csv.")
    parser.add_argument("--category-map-xlsx", default="step_one_to_super_category_map.csv",
                        help="Mapping table from level-one to super-category.")
    parser.add_argument("--category-map-orig-col", default="level_one_category_name",
                        help="Source column name in mapping table.")
    parser.add_argument("--category-map-target-col", default="super_category",
                        help="Target column name in mapping table.")
    parser.add_argument("--step1-category-sample", dest="step1_category_sample", action="store_true",
                        default=True, help="Enable category sampling for step1 source.")
    parser.add_argument("--no-step1-category-sample", dest="step1_category_sample", action="store_false",
                        help="Disable category sampling for step1 source.")
    parser.add_argument("--step1-category-per", type=int, default=0,
                        help="Step1 samples per category; <=0 means use --per-category.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing.")
    args = parser.parse_args()

    set_dry_run(args.dry_run)
    ensure_create_prompts_supports_schwartz(sys.executable)

    if args.use_experiment_csv:
        exp_path = Path(args.experiment_csv)
        if not exp_path.exists():
            raise FileNotFoundError(f"找不到 experiment CSV：{exp_path}（可用 --experiment-csv 指定路径）")
        args.step1_csv = str(exp_path)
        if args.skip_step1:
            print("[WARN] --use-experiment-csv 与 --skip-step1 同时使用：将复用现有 step1_titles.xlsx，"
                  "可能不是由 experiment CSV 生成。")

    args.schwartz_types = _parse_schwartz_list(args.schwartz_types, args.schwartz_profiles)
    if args.limit_schwartz and args.limit_schwartz > 0:
        args.schwartz_types = args.schwartz_types[:args.limit_schwartz]
    args.categories = _split_by_commas(args.categories) or DEFAULT_CATEGORIES
    suffix_tag = args.subset_suffix.strip() or f"{len(args.categories)}cats_{args.per_category if args.per_category > 0 else 'full'}"

    source_csv = prepare_step1_source(args, suffix_tag)
    ensure_step1(args, source_csv)

    jobs = []
    for idx, value_type in enumerate(args.schwartz_types):
        job = process_schwartz_value(value_type, args, args.categories, suffix_tag, seed=args.seed + idx * 101)
        jobs.append(job)

    if not jobs:
        print("[WARN] 没有需要处理的 Schwartz Value，流程结束。")
        return

    if not args.skip_render and not args.skip_kill_ollama:
        pkill_ollama()

    if args.skip_render:
        print("\n=== 跳过渲染阶段 (--skip-render) ===")
    else:
        print("\n=== 开始渲染阶段（请确保 ComfyUI API 可用） ===")

    for job in jobs:
        render_dir = Path(args.render_root) / job["exp_tag"]
        pairs_dir = Path(args.pairs_root) / job["exp_tag"]

        if args.skip_render:
            print(f"[SKIP] 渲染已跳过，期望已有输出：{render_dir}")
        else:
            if args.resume and render_dir.exists() and not args.force_render:
                print(f"[SKIP] 复用已有渲染输出：{render_dir}")
            else:
                run_render(job, args)

        if args.skip_pairs:
            print(f"[SKIP] 对比图阶段已跳过（Schwartz={job['value']})")
            continue

        if args.resume and pairs_dir.exists() and not args.force_pairs:
            print(f"[SKIP] 复用已有对比图：{pairs_dir}")
            continue

        run_pairs(job, args)


if __name__ == "__main__":
    main()
