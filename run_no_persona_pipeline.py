#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Baseline pipeline without persona injection:
1) Generate prompts (no persona);
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
                    f"[WARN] category map missing columns {orig_col}/{target_col}; "
                    f"using first two columns: {resolved_orig}/{resolved_target}",
                    flush=True,
                )
            else:
                raise ValueError(f"category map missing columns: {orig_col}/{target_col}")
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
    return [tok.strip() for tok in re.split(r"[,\n]+", raw or "") if tok.strip()]


def run_cmd(cmd: Sequence[str], desc: str) -> None:
    cmd_disp = " ".join(cmd)
    print(f"\n[RUN] {desc}\n  $ {cmd_disp}", flush=True)
    if DRY_RUN:
        print("  (dry-run) command skipped")
        return
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({desc}): {cmd_disp}")


def prepare_step1_source(args, suffix_tag: str) -> str:
    if not args.step1_category_sample:
        return args.step1_csv
    per_cat = args.step1_category_per if args.step1_category_per and args.step1_category_per > 0 else args.per_category
    if per_cat <= 0:
        raise ValueError("step1_category_sample requires per-category > 0")
    subset_name = f"step1_source_{suffix_tag}.csv"
    subset_path = Path(args.prompts_dir) / subset_name
    if args.resume and subset_path.exists():
        print(f"[SKIP] reuse Step1 source subset: {subset_path}")
        return str(subset_path)
    print(f"[info] Step1 source sampling by categories: {args.categories}, per category={per_cat}")
    src_df = read_table_auto(args.step1_csv)
    cat_col = args.source_category_col
    if cat_col not in src_df.columns:
        raise ValueError(f"Step1 source missing column: {cat_col}")
    cat_series = src_df[cat_col].astype(str).str.strip()
    mapping = load_category_map(args.category_map_xlsx, args.category_map_orig_col, args.category_map_target_col)
    src_df["_mapped_cat"] = cat_series.map(lambda x: mapping.get(x, x))
    filtered = src_df[src_df["_mapped_cat"].isin(args.categories)].copy()
    if filtered.empty:
        raise RuntimeError("no data after category mapping; check input categories")
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
        raise RuntimeError("no categories available in source data")
    subset = pd.concat(sampled_frames, ignore_index=True).drop(columns=["_mapped_cat"])
    subset.to_csv(subset_path, index=False)
    if missing:
        print(f"[WARN] missing categories in source: {missing}")
    if insufficient:
        print("[WARN] insufficient categories: " +
              ", ".join(f"{cat}({cnt}/{per_cat})" for cat, cnt in insufficient))
    print(f"[OK] Step1 subset written: {subset_path} (rows={len(subset)})")
    return str(subset_path)


def ensure_step1(args, source_csv: str) -> Path:
    out_dir = Path(args.prompts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    step1_excel = out_dir / STEP1_FILENAME
    if args.skip_step1 and step1_excel.exists():
        print(f"[SKIP] Step1 exists: {step1_excel}")
        return step1_excel
    cmd = [
        sys.executable, "create_promo_titles.py",
        "--model", args.step1_model,
        "--csv-path", source_csv,
        "--out-dir", str(out_dir),
        "--sample-num", str(args.step1_sample_num),
        "--rand-seed", str(args.seed),
    ]
    run_cmd(cmd, "generate Step1 titles & white background")
    return step1_excel


def sample_categories(base_excel: Path,
                      categories: Sequence[str],
                      per_category: int,
                      seed: int,
                      suffix_tag: str) -> Path:
    df = pd.read_excel(base_excel)
    cat_col = next((c for c in ("super_category", "Category", "category", "level_one_category_name") if c in df.columns), None)
    if not cat_col:
        raise ValueError(f"{base_excel} missing super_category/Category column")
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
        raise RuntimeError(f"{base_excel} has no data for requested categories")
    sampled = pd.concat(sampled_frames, ignore_index=True)
    sampled = sampled.drop(columns=["_cat_norm"], errors="ignore")
    subset_path = base_excel.with_name(f"{base_excel.stem}_{suffix_tag}.xlsx")
    sampled.to_excel(subset_path, index=False)
    if missing:
        print(f"[WARN] missing categories in {base_excel.name}: {missing}")
    if insufficient:
        warn = ", ".join(f"{cat}({cnt}/{per_category})" for cat, cnt in insufficient)
        print(f"[WARN] insufficient categories (<{per_category}) kept as-is: {warn}")
    print(f"[OK] sampled Excel output: {subset_path}")
    return subset_path


def pkill_ollama():
    print("\n[info] pkill -9 ollama to release GPU memory ...")
    if DRY_RUN:
        print("  (dry-run) skip pkill ollama")
        return
    subprocess.run(["pkill", "-9", "ollama"], cwd=REPO_ROOT, check=False)


def run_render(job: dict, args) -> None:
    render_cmd = [
        sys.executable, "render_with_comfyui.py",
        "--prompts-file", str(job["subset"]),
        "--exp-name", job["exp_tag"],
        "--output-root", args.render_root,
    ]
    run_cmd(render_cmd, "ComfyUI render (no persona)")


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
    run_cmd(merge_cmd, "build paired comparisons (no persona)")


def main():
    parser = argparse.ArgumentParser(description="Baseline pipeline (no persona injection)")
    parser.add_argument("--exp-name", default="nopersona",
                        help="Experiment tag for outputs (e.g. nopersona_0320).")
    parser.add_argument("--categories", default=",".join(DEFAULT_CATEGORIES),
                        help="Comma/newline separated super-category list.")
    parser.add_argument("--per-category", type=int, default=10,
                        help="Samples per category; <=0 keeps all.")
    parser.add_argument("--subset-suffix", default="",
                        help="Extra suffix for sampled Excel (e.g. 14cats_10).")
    parser.add_argument("--seed", type=int, default=125,
                        help="Global random seed.")
    parser.add_argument("--prompt-model", choices=["7b", "32b"], default="32b",
                        help="Model size for prompt generation.")
    parser.add_argument("--disable-triad", dest="disable_triad", action="store_true",
                        default=True, help="Disable triad routing (default on).")
    parser.add_argument("--enable-triad", dest="disable_triad", action="store_false",
                        help="Enable triad routing.")
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

    if args.use_experiment_csv:
        exp_path = Path(args.experiment_csv)
        if not exp_path.exists():
            raise FileNotFoundError(f"experiment CSV not found: {exp_path}")
        args.step1_csv = str(exp_path)
        if args.skip_step1:
            print("[WARN] --use-experiment-csv with --skip-step1: step1_titles.xlsx may not match experiment CSV.")

    args.categories = _split_by_commas(args.categories) or DEFAULT_CATEGORIES
    suffix_tag = args.subset_suffix.strip() or f"{len(args.categories)}cats_{args.per_category if args.per_category > 0 else 'full'}"

    source_csv = prepare_step1_source(args, suffix_tag)
    ensure_step1(args, source_csv)

    exp_tag = (args.exp_name or "").strip() or "nopersona"
    prompts_path = Path(args.prompts_dir) / (f"{PROMPT_PREFIX}_{exp_tag}.xlsx" if exp_tag else f"{PROMPT_PREFIX}.xlsx")

    if args.skip_prompts:
        if not prompts_path.exists():
            raise FileNotFoundError(f"[ERR] prompts file not found for --skip-prompts: {prompts_path}")
        print(f"[SKIP] --skip-prompts active, using: {prompts_path}")
    elif args.resume and prompts_path.exists():
        print(f"[SKIP] reuse existing prompts: {prompts_path}")
    else:
        cmd = [
            sys.executable, "create_categorical_prompts.py",
            "--model", args.prompt_model,
            "--persona-kind", "none",
            "--exp-name", exp_tag,
            "--seed", str(args.seed),
        ]
        if args.disable_triad:
            cmd.append("--disable-triad")
        run_cmd(cmd, "generate prompts (no persona)")

    subset_path: Path
    resampled = False
    if args.skip_sampling:
        subset_path = prompts_path
        subset_exists = subset_path.exists()
        if not subset_exists:
            raise FileNotFoundError(f"[ERR] --skip-sampling requires existing file: {subset_path}")
        print(f"[SKIP] --skip-sampling active, using prompts file: {subset_path}")
    else:
        subset_path = prompts_path.with_name(f"{prompts_path.stem}_{suffix_tag}.xlsx")
        subset_exists = subset_path.exists()
        if args.resume and subset_exists:
            print(f"[SKIP] reuse sampled file: {subset_path}")
        else:
            subset_path = sample_categories(prompts_path, args.categories, args.per_category, args.seed, suffix_tag)
            resampled = True
        subset_exists = subset_path.exists()

    if args.skip_normalize:
        print(f"[SKIP] --skip-normalize active: {subset_path}")
    else:
        need_norm = args.force_normalize or resampled or args.skip_sampling or not subset_exists or not args.resume
        if not need_norm and args.resume:
            print(f"[SKIP] resume mode, reuse normalized images: {subset_path}")
        else:
            norm_cmd = [
                sys.executable, "normalize_scale_and_canvas.py",
                "--excel", str(subset_path),
                "--out-dir", args.prompts_dir,
            ]
            run_cmd(norm_cmd, "normalize white background")

    job = {"exp_tag": exp_tag, "subset": subset_path}

    if not args.skip_render and not args.skip_kill_ollama:
        pkill_ollama()

    if args.skip_render:
        print("\n=== Render skipped (--skip-render) ===")
    else:
        print("\n=== Start rendering (ensure ComfyUI API is available) ===")

    render_dir = Path(args.render_root) / job["exp_tag"]
    pairs_dir = Path(args.pairs_root) / job["exp_tag"]

    if args.skip_render:
        print(f"[SKIP] render skipped, expect output at: {render_dir}")
    else:
        if args.resume and render_dir.exists() and not args.force_render:
            print(f"[SKIP] reuse render output: {render_dir}")
        else:
            run_render(job, args)

    if args.skip_pairs:
        print("[SKIP] pairing skipped")
        return

    if args.resume and pairs_dir.exists() and not args.force_pairs:
        print(f"[SKIP] reuse pairs output: {pairs_dir}")
        return

    run_pairs(job, args)


if __name__ == "__main__":
    main()
