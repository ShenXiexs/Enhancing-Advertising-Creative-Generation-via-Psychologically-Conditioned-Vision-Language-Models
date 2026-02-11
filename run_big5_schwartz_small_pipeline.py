#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Small experiment pipeline (target-audience personas + style variants):
- 10 products from the experiment-small CSV (see DEFAULT_STEP1_CSV)
- Big Five: 5 traits x (High/Low) = 10 profiles
- Schwartz values: 10 values
- Style variants: with style constraints, and no-style + 4k
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

DEFAULT_STEP1_CSV = "白底商品信息类目_experiment_small.csv"
DEFAULT_BIG5_PROFILES = "big_five_profiles.csv"
DEFAULT_SCHWARTZ_PROFILES = "schwartz_value_profiles.csv"

TRAITS = ["Openness", "Conscientiousness", "Extraversion", "Agreeableness", "Neuroticism"]
TRAIT_ABBR = {
    "Openness": "o",
    "Conscientiousness": "c",
    "Extraversion": "e",
    "Agreeableness": "a",
    "Neuroticism": "n",
}


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


def load_schwartz_types(path: str) -> List[str]:
    df = read_table_auto(path)
    df.columns = [str(c).strip() for c in df.columns]
    type_col = _find_col(df, ["schwartz_value_type", "value_type", "schwartz_type", "value", "type"])
    if not type_col:
        raise ValueError(
            "schwartz_value_profiles is missing schwartz_value_type column; "
            f"columns={list(df.columns)}"
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


def ensure_create_prompts_supports(python_exe: str, required_flags: Sequence[str]) -> None:
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
            "Unable to execute create_categorical_prompts.py; "
            f"check file exists at {REPO_ROOT / 'create_categorical_prompts.py'}"
        )
    if not all(flag in help_text for flag in required_flags):
        raise RuntimeError(
            "create_categorical_prompts.py is missing required flags: "
            f"{', '.join(required_flags)}"
        )


def ensure_step1(args) -> Path:
    out_dir = Path(args.prompts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    step1_excel = out_dir / STEP1_FILENAME
    if args.skip_step1 and step1_excel.exists():
        print(f"[SKIP] Step1 exists: {step1_excel}")
        return step1_excel
    cmd = [
        sys.executable, "create_promo_titles.py",
        "--model", args.step1_model,
        "--csv-path", args.step1_csv,
        "--out-dir", str(out_dir),
        "--sample-num", str(args.step1_sample_num),
        "--rand-seed", str(args.seed),
    ]
    run_cmd(cmd, "generate Step1 titles & white background")
    return step1_excel


def pkill_ollama():
    print("\n[info] pkill -9 ollama to release GPU memory ...")
    if DRY_RUN:
        print("  (dry-run) skip pkill ollama")
        return
    subprocess.run(["pkill", "-9", "ollama"], cwd=REPO_ROOT, check=False)


def run_render(job: dict, args) -> None:
    if not args.skip_kill_ollama:
        pkill_ollama()
    render_cmd = [
        sys.executable, "render_with_comfyui.py",
        "--prompts-file", str(job["subset"]),
        "--exp-name", job["exp_tag"],
        "--output-root", args.render_root,
    ]
    if getattr(args, "_comfy_checked", False) and not args.always_check_comfyui:
        render_cmd.append("--skip-comfyui-check")
    if job.get("seed") is not None:
        render_cmd.extend(["--seed", str(job["seed"])])
    run_cmd(render_cmd, f"ComfyUI render ({job['label']})")
    args._comfy_checked = True


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
    run_cmd(merge_cmd, f"build paired comparisons ({job['label']})")


def build_big5_jobs() -> List[dict]:
    jobs = []
    for trait in TRAITS:
        for level in ("High", "Low"):
            label = f"{TRAIT_ABBR[trait]}{'h' if level == 'High' else 'l'}"
            tokens = f"{trait}:{level}"
            jobs.append({"label": label, "tokens": tokens, "description": f"{trait} {level}"})
    return jobs


def process_big5_profile(profile: dict, args, seed: int,
                         persona_mode: str,
                         style_constraints: str, end_with_4k: str,
                         style_tag: str = "", disable_triad: bool = False) -> dict:
    exp_tag = f"{args.big5_exp_prefix}_{profile['label']}"
    if style_tag:
        exp_tag = f"{exp_tag}_{style_tag}"
    prompts_path = Path(args.prompts_dir) / f"{PROMPT_PREFIX}_{exp_tag}.xlsx"

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
            "--persona-kind", "big5",
            "--big5-plan", args.big5_plan,
            "--big5-profiles", args.big5_profiles,
            "--big5-types", profile["tokens"],
            "--big5-mode", persona_mode,
            "--big5-persona-style", args.big5_persona_style,
            "--style-constraints", style_constraints,
            "--end-with-4k", end_with_4k,
            "--exp-name", exp_tag,
            "--seed", str(seed),
        ]
        if args.no_background or disable_triad:
            cmd.append("--disable-triad")
        run_cmd(cmd, f"generate prompts (Big5={profile['label']})")

    subset_path = prompts_path
    if not subset_path.exists():
        raise FileNotFoundError(f"[ERR] prompts file missing: {subset_path}")

    if args.skip_normalize:
        print(f"[SKIP] --skip-normalize active: {subset_path}")
    else:
        if args.resume and not args.force_normalize:
            print(f"[SKIP] resume mode, reuse normalized images: {subset_path}")
        else:
            norm_cmd = [
                sys.executable, "normalize_scale_and_canvas.py",
                "--excel", str(subset_path),
                "--out-dir", args.prompts_dir,
            ]
            run_cmd(norm_cmd, f"normalize white background (Big5={profile['label']})")

    return {
        "kind": "big5",
        "label": f"big5_{profile['label']}_{persona_mode}",
        "exp_tag": exp_tag,
        "subset": subset_path,
        "seed": seed,
    }


def process_schwartz_value(value_type: str, args, seed: int,
                           persona_mode: str,
                           style_constraints: str, end_with_4k: str,
                           style_tag: str = "", disable_triad: bool = False) -> dict:
    value_tag = re.sub(r"[^0-9A-Za-z_\-]+", "_", value_type.strip().lower())
    value_tag = re.sub(r"_+", "_", value_tag).strip("_") or "value"
    exp_tag = f"{args.schwartz_exp_prefix}_{value_tag}"
    if style_tag:
        exp_tag = f"{exp_tag}_{style_tag}"
    prompts_path = Path(args.prompts_dir) / f"{PROMPT_PREFIX}_{exp_tag}.xlsx"

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
            "--persona-kind", "schwartz",
            "--schwartz-profiles", args.schwartz_profiles,
            "--schwartz-type", value_type,
            "--schwartz-mode", persona_mode,
            "--schwartz-persona-style", args.schwartz_persona_style,
            "--style-constraints", style_constraints,
            "--end-with-4k", end_with_4k,
            "--exp-name", exp_tag,
            "--seed", str(seed),
        ]
        if args.no_background or disable_triad:
            cmd.append("--disable-triad")
        run_cmd(cmd, f"generate prompts (Schwartz={value_type})")

    subset_path = prompts_path
    if not subset_path.exists():
        raise FileNotFoundError(f"[ERR] prompts file missing: {subset_path}")

    if args.skip_normalize:
        print(f"[SKIP] --skip-normalize active: {subset_path}")
    else:
        if args.resume and not args.force_normalize:
            print(f"[SKIP] resume mode, reuse normalized images: {subset_path}")
        else:
            norm_cmd = [
                sys.executable, "normalize_scale_and_canvas.py",
                "--excel", str(subset_path),
                "--out-dir", args.prompts_dir,
            ]
            run_cmd(norm_cmd, f"normalize white background (Schwartz={value_type})")

    return {
        "kind": "schwartz",
        "label": f"schwartz_{value_type}_{persona_mode}",
        "exp_tag": exp_tag,
        "subset": subset_path,
        "seed": seed,
    }


def process_no_persona(args, seed: int,
                       style_constraints: str, end_with_4k: str,
                       style_tag: str = "", disable_triad: bool = False) -> dict:
    exp_tag = args.nopersona_exp_prefix
    if style_tag:
        exp_tag = f"{exp_tag}_{style_tag}"
    prompts_path = Path(args.prompts_dir) / f"{PROMPT_PREFIX}_{exp_tag}.xlsx"

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
            "--style-constraints", style_constraints,
            "--end-with-4k", end_with_4k,
            "--exp-name", exp_tag,
            "--seed", str(seed),
        ]
        if args.no_background or disable_triad:
            cmd.append("--disable-triad")
        run_cmd(cmd, "generate prompts (no persona)")

    subset_path = prompts_path
    if not subset_path.exists():
        raise FileNotFoundError(f"[ERR] prompts file missing: {subset_path}")

    if args.skip_normalize:
        print(f"[SKIP] --skip-normalize active: {subset_path}")
    else:
        if args.resume and not args.force_normalize:
            print(f"[SKIP] resume mode, reuse normalized images: {subset_path}")
        else:
            norm_cmd = [
                sys.executable, "normalize_scale_and_canvas.py",
                "--excel", str(subset_path),
                "--out-dir", args.prompts_dir,
            ]
            run_cmd(norm_cmd, "normalize white background (no persona)")

    return {
        "kind": "none",
        "label": "nopersona",
        "exp_tag": exp_tag,
        "subset": subset_path,
        "seed": seed,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Small pipeline: target-audience Big Five + Schwartz with two style variants."
    )
    parser.add_argument("--step1-csv", default=DEFAULT_STEP1_CSV,
                        help="Step1 input CSV/XLSX (default: experiment_small).")
    parser.add_argument("--step1-sample-num", type=int, default=10,
                        help="Number of products to sample for Step1 (default: 10).")
    parser.add_argument("--step1-model", choices=["7b", "32b"], default="7b",
                        help="Model size for Step1 title generation.")
    parser.add_argument("--prompt-model", choices=["7b", "32b"], default="32b",
                        help="Model size for prompt generation.")
    parser.add_argument("--style-constraints", choices=["on", "off"], default="on",
                        help="Style constraints for custom variant only (default: on).")
    parser.add_argument("--end-with-4k", choices=["on", "off"], default="on",
                        help="4k suffix for custom variant only (default: on).")
    parser.add_argument("--style-variants", choices=["both", "style", "no-style", "custom"], default="both",
                        help="Style variants to run: both=style+no-style+4k, style=style only, "
                             "no-style=no-style+4k only (also disables triad), custom=use --style-constraints/--end-with-4k.")
    parser.add_argument("--seed", type=int, default=2026,
                        help="Global random seed.")
    parser.add_argument("--persona-mode", choices=["concat", "inline", "both"], default="inline",
                        help="Persona mode for Big5/Schwartz: concat, inline, or both (default: inline).")
    parser.add_argument("--no-background", action="store_true",
                        help="Disable background style descriptions (triad).")
    parser.add_argument("--big5-plan", choices=["A", "B"], default="A",
                        help="Big Five persona plan.")
    parser.add_argument("--big5-persona-style", choices=["legacy", "target"], default="target",
                        help="Big Five persona wording: legacy=As a picture; target=Target audience.")
    parser.add_argument("--big5-profiles", default=DEFAULT_BIG5_PROFILES,
                        help="Big Five profiles CSV path.")
    parser.add_argument("--big5-exp-prefix", default="big5_small",
                        help="Prefix for Big Five outputs.")
    parser.add_argument("--schwartz-profiles", default=DEFAULT_SCHWARTZ_PROFILES,
                        help="Schwartz profiles CSV path.")
    parser.add_argument("--schwartz-types", default="",
                        help="Comma/newline separated Schwartz value types; empty means all.")
    parser.add_argument("--schwartz-persona-style", choices=["legacy", "target"], default="target",
                        help="Schwartz persona wording: legacy=You prioritize; target=Target audience.")
    parser.add_argument("--limit-schwartz", type=int, default=0,
                        help="Limit to first N Schwartz values (0 = all).")
    parser.add_argument("--schwartz-exp-prefix", default="schwartz_small",
                        help="Prefix for Schwartz outputs.")
    parser.add_argument("--skip-nopersona", action="store_true",
                        help="Skip no-persona baseline outputs.")
    parser.add_argument("--nopersona-exp-prefix", default="nopersona_small",
                        help="Prefix for no-persona outputs.")
    parser.add_argument("--prompts-dir", default="out_step1",
                        help="Step1/Step2 Excel output dir.")
    parser.add_argument("--render-root", default="out_step2",
                        help="Render output root dir.")
    parser.add_argument("--pairs-root", default="out_step3",
                        help="Pairs output root dir.")
    parser.add_argument("--skip-step1", action="store_true",
                        help="Skip step1 if step1_titles.xlsx exists.")
    parser.add_argument("--resume", action="store_true",
                        help="Reuse existing prompts/normalized/render outputs.")
    parser.add_argument("--skip-kill-ollama", action="store_true",
                        help="Do not pkill ollama before rendering.")
    parser.add_argument("--always-check-comfyui", action="store_true",
                        help="Always check ComfyUI service before every render job.")
    parser.add_argument("--skip-prompts", action="store_true",
                        help="Skip prompt generation (requires existing Excel).")
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
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing.")
    args = parser.parse_args()
    args._comfy_checked = False

    set_dry_run(args.dry_run)

    ensure_create_prompts_supports(
        sys.executable,
        ("--persona-kind", "--big5-plan", "--big5-types", "--big5-mode", "--schwartz-type", "--schwartz-mode",
         "--style-constraints", "--end-with-4k"),
    )

    step1_path = Path(args.step1_csv)
    if not step1_path.exists():
        raise FileNotFoundError(f"step1 CSV not found: {step1_path}")

    ensure_step1(args)

    big5_jobs = build_big5_jobs()

    if args.schwartz_types.strip():
        schwartz_values = _split_by_commas(args.schwartz_types)
    else:
        schwartz_values = load_schwartz_types(args.schwartz_profiles)
    if args.limit_schwartz and args.limit_schwartz > 0:
        schwartz_values = schwartz_values[:args.limit_schwartz]

    if args.style_variants == "both":
        variants = [
            ("style", "on", "on", False),
            ("nostyle4k", "off", "on", True),
        ]
    elif args.style_variants == "style":
        variants = [("style", "on", "on", False)]
    elif args.style_variants == "no-style":
        variants = [("nostyle4k", "off", "on", True)]
    else:
        tag = (
            f"custom_{'style' if args.style_constraints == 'on' else 'nostyle'}_"
            f"{'4k' if args.end_with_4k == 'on' else 'no4k'}"
        )
        variants = [(tag, args.style_constraints, args.end_with_4k, False)]

    persona_modes = ["concat", "inline"] if args.persona_mode == "both" else [args.persona_mode]

    jobs = []
    for v_idx, (tag, style_constraints, end_with_4k, disable_triad) in enumerate(variants):
        base_seed = args.seed
        if not args.skip_nopersona:
            jobs.append(process_no_persona(args, seed=base_seed,
                                           style_constraints=style_constraints,
                                           end_with_4k=end_with_4k,
                                           style_tag=tag,
                                           disable_triad=disable_triad))
        for mode in persona_modes:
            mode_tag = tag if len(persona_modes) == 1 else f"{tag}_{mode}"
            for idx, value_type in enumerate(schwartz_values):
                job = process_schwartz_value(value_type, args, seed=base_seed + idx * 101,
                                             persona_mode=mode,
                                             style_constraints=style_constraints,
                                             end_with_4k=end_with_4k,
                                             style_tag=mode_tag,
                                             disable_triad=disable_triad)
                jobs.append(job)

            offset = len(schwartz_values) + 1
            for idx, profile in enumerate(big5_jobs):
                job = process_big5_profile(profile, args, seed=base_seed + (offset + idx) * 101,
                                           persona_mode=mode,
                                           style_constraints=style_constraints,
                                           end_with_4k=end_with_4k,
                                           style_tag=mode_tag,
                                           disable_triad=disable_triad)
                jobs.append(job)

    if not jobs:
        print("[WARN] no jobs created, exit.")
        return

    if args.skip_render:
        print("\n=== Render skipped (--skip-render) ===")
    else:
        print("\n=== Start rendering (ensure ComfyUI API is available) ===")

    for job in jobs:
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
            print(f"[SKIP] pairing skipped ({job['label']})")
            continue

        if args.resume and pairs_dir.exists() and not args.force_pairs:
            print(f"[SKIP] reuse pairs output: {pairs_dir}")
            continue

        run_pairs(job, args)


if __name__ == "__main__":
    main()
