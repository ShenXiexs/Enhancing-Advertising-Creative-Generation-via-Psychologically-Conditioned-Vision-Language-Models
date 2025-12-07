#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量遍历 16 种 MBTI，并针对指定大类自动完成：
1) 生成背景 prompt（可选禁用 triad）；
2) 依据大类抽样固定数量的素材；
3) 白底归一化；
4) 调用 ComfyUI 渲染；
5) 生成对比图。

使用前请先手动启动 Ollama（供步骤 1 & 2 使用）以及 ComfyUI（供渲染使用），
脚本会在进入渲染阶段前自动 pkill -9 ollama 释放显存。
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import os
from pathlib import Path
from typing import List, Sequence

import pandas as pd


DRY_RUN = False


def set_dry_run(flag: bool) -> None:
    global DRY_RUN
    DRY_RUN = bool(flag)


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


REPO_ROOT = Path(__file__).resolve().parent
STEP1_FILENAME = "step1_titles.xlsx"
PROMPT_PREFIX = "step1_prompts"

DEFAULT_MBTI_TYPES = [
    "ESTJ", "ESTP", "ESFJ", "ESFP",
    "ENTJ", "ENTP", "ENFJ", "ENFP",
    "ISTJ", "ISTP", "ISFJ", "ISFP",
    "INTJ", "INTP", "INFJ", "INFP",
]

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


def load_category_map(map_path: str, orig_col: str, target_col: str) -> dict:
    df = read_table_auto(map_path)
    if orig_col not in df.columns or target_col not in df.columns:
        raise ValueError(f"分类映射缺少列：{orig_col}/{target_col}")
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


def _parse_mbti_list(raw: str | None) -> List[str]:
    if not raw:
        return DEFAULT_MBTI_TYPES
    tokens = [tok.strip().upper() for tok in re.split(r"[,\s]+", raw) if tok.strip()]
    return tokens or DEFAULT_MBTI_TYPES


def run_cmd(cmd: Sequence[str], desc: str) -> None:
    cmd_disp = " ".join(cmd)
    print(f"\n[RUN] {desc}\n  $ {cmd_disp}", flush=True)
    if DRY_RUN:
        print("  (dry-run) command skipped")
        return
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({desc}): {cmd_disp}")


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


def process_mbti_type(mbti: str,
                      args,
                      categories: Sequence[str],
                      suffix_tag: str,
                      seed: int) -> dict:
    exp_tag = f"{args.exp_prefix}_{mbti.lower()}"
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
            "--mbti-plan", args.mbti_plan,
            "--mbti-profiles", args.mbti_profiles,
            "--mbti-type", mbti,
            "--mbti-mode", args.mbti_mode,
            "--exp-name", exp_tag,
            "--seed", str(seed),
        ]
        if args.disable_triad:
            cmd.append("--disable-triad")
        run_cmd(cmd, f"生成 Prompt（MBTI={mbti})")

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
            run_cmd(norm_cmd, f"归一化白底图（MBTI={mbti})")

    return {"mbti": mbti, "exp_tag": exp_tag, "subset": subset_path}


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
    run_cmd(render_cmd, f"ComfyUI 渲染（MBTI={job['mbti']})")


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
    run_cmd(merge_cmd, f"生成对比图（MBTI={job['mbti']})")


def main():
    parser = argparse.ArgumentParser(description="MBTI 批量自动化（Step1→Step2→Normalize→Render→对比图）")
    parser.add_argument("--mbti-types", default=",".join(DEFAULT_MBTI_TYPES),
                        help="以逗号/空格分隔的 MBTI 列表，默认 16 型全量。")
    parser.add_argument("--limit-mbti", type=int, default=0,
                        help="仅处理前 N 个 MBTI（按传入顺序），0 表示无限制。")
    parser.add_argument("--categories", default=",".join(DEFAULT_CATEGORIES),
                        help="以逗号或换行分隔的大类名称列表。")
    parser.add_argument("--per-category", type=int, default=10,
                        help="每个大类抽样的数量，<=0 表示保留该大类全部。")
    parser.add_argument("--subset-suffix", default="",
                        help="抽样 Excel 额外后缀（默认自动生成，如 14cats_10）。")
    parser.add_argument("--seed", type=int, default=125,
                        help="全局随机种子，影响 pandas.sample 以及 Step1/Step2。")
    parser.add_argument("--mbti-plan", choices=["A", "B"], default="A",
                        help="MBTI persona 使用的计划，默认 A。")
    parser.add_argument("--mbti-mode", choices=["concat", "inline"], default="inline",
                        help="persona 拼接模式（create_categorical_prompts.py 对应参数）。")
    parser.add_argument("--prompt-model", choices=["7b", "32b"], default="32b",
                        help="用于生成背景 prompt 的模型规格。")
    parser.add_argument("--disable-triad", dest="disable_triad", action="store_true",
                        default=True, help="禁用 triad（默认开启）。")
    parser.add_argument("--enable-triad", dest="disable_triad", action="store_false",
                        help="启用 triad 路由。")
    parser.add_argument("--exp-prefix", default="planA32b_notraid",
                        help="输出 Excel/渲染目录的统一前缀，将自动加上 _<mbti>。")
    parser.add_argument("--prompts-dir", default="out_step1",
                        help="Step1/Step2 Excel 输出目录，默认 out_step1。")
    parser.add_argument("--render-root", default="out_step2",
                        help="渲染图片落地目录，默认 out_step2。")
    parser.add_argument("--pairs-root", default="out_step3",
                        help="对比图落地目录，默认 out_step3。")
    parser.add_argument("--mbti-profiles", default="mbti_profiles.csv",
                        help="MBTI personas CSV 路径。")
    parser.add_argument("--step1-csv", default="白底商品信息类目.csv",
                        help="Step1 输入 CSV/XLSX。")
    parser.add_argument("--step1-model", choices=["7b", "32b"], default="7b",
                        help="Step1 生成标题所用的模型规格。")
    parser.add_argument("--step1-sample-num", type=int, default=0,
                        help="Step1 抽样行数，<=0 表示全量。")
    parser.add_argument("--skip-step1", action="store_true",
                        help="若 step1_titles.xlsx 已存在且无需重跑，可加此参数跳过 Step1。")
    parser.add_argument("--resume", action="store_true",
                        help="已存在 step1_prompts_xxx.xlsx / 渲染目录时自动跳过对应步骤。")
    parser.add_argument("--skip-kill-ollama", action="store_true",
                        help="若不希望脚本自动 pkill -9 ollama，可加此参数。")
    parser.add_argument("--skip-prompts", action="store_true",
                        help="跳过 Step2 prompt 生成（需确保 Excel 已存在）。")
    parser.add_argument("--skip-sampling", action="store_true",
                        help="跳过大类抽样，直接使用全量 prompts Excel。")
    parser.add_argument("--skip-normalize", action="store_true",
                        help="跳过白底归一化。")
    parser.add_argument("--force-normalize", action="store_true",
                        help="即使处于 resume 模式也强制重新归一化。")
    parser.add_argument("--skip-render", action="store_true",
                        help="跳过 ComfyUI 渲染阶段（方便只做前处理）。")
    parser.add_argument("--skip-pairs", action="store_true",
                        help="跳过对比图合成阶段。")
    parser.add_argument("--force-render", action="store_true",
                        help="即便渲染目录存在也强制重新渲染。")
    parser.add_argument("--force-pairs", action="store_true",
                        help="即便对比图目录存在也强制重新生成。")
    parser.add_argument("--source-category-col", default="level_one_category_name",
                        help="Step1 原始 CSV 中的一级类目列名。")
    parser.add_argument("--category-map-xlsx", default="step_one_to_super_category_map.csv",
                        help="原一级类目 -> 归入大类 的映射表（CSV/XLSX）。")
    parser.add_argument("--category-map-orig-col", default="level_one_category_name",
                        help="映射表中的原始类目列名。")
    parser.add_argument("--category-map-target-col", default="super_category",
                        help="映射表中的目标大类列名。")
    parser.add_argument("--step1-category-sample", dest="step1_category_sample", action="store_true",
                        default=True, help="按大类抽样限定 Step1 输入（默认开启）。")
    parser.add_argument("--no-step1-category-sample", dest="step1_category_sample", action="store_false",
                        help="禁用 Step1 大类抽样。")
    parser.add_argument("--step1-category-per", type=int, default=0,
                        help="Step1 每个大类抽样数量，<=0 则沿用 --per-category。")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅打印将执行的命令，不真正运行。")
    args = parser.parse_args()
    set_dry_run(args.dry_run)
    args.mbti_types = _parse_mbti_list(args.mbti_types)
    if args.limit_mbti and args.limit_mbti > 0:
        args.mbti_types = args.mbti_types[:args.limit_mbti]
    args.categories = _split_by_commas(args.categories) or DEFAULT_CATEGORIES
    suffix_tag = args.subset_suffix.strip() or f"{len(args.categories)}cats_{args.per_category if args.per_category > 0 else 'full'}"

    source_csv = prepare_step1_source(args, suffix_tag)
    ensure_step1(args, source_csv)

    jobs = []
    for idx, mbti in enumerate(args.mbti_types):
        job = process_mbti_type(mbti, args, args.categories, suffix_tag, seed=args.seed + idx * 101)
        jobs.append(job)

    if not jobs:
        print("[WARN] 没有需要处理的 MBTI，流程结束。")
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
            print(f"[SKIP] 对比图阶段已跳过（MBTI={job['mbti']})")
            continue

        if args.resume and pairs_dir.exists() and not args.force_pairs:
            print(f"[SKIP] 复用已有对比图：{pairs_dir}")
            continue

        run_pairs(job, args)


if __name__ == "__main__":
    main()
