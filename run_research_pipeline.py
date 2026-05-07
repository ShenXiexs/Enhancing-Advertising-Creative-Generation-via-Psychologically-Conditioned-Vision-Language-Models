#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Research pipeline for custom image + notes CSV/XLSX input.

Flow:
1) Prepare a step1_titles-compatible Excel from custom input.
2) Split rows by effective persona condition.
3) Generate prompts with the existing persona flow.
4) Normalize images (URL or local path).
5) Render with ComfyUI.
6) Merge comparison pairs.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd


DRY_RUN = False
REPO_ROOT = Path(__file__).resolve().parent


def set_dry_run(flag: bool) -> None:
    global DRY_RUN
    DRY_RUN = bool(flag)


def parse_args():
    parser = argparse.ArgumentParser(description="Research pipeline for image + notes CSV/XLSX input.")
    parser.add_argument("--input-csv", required=True, help="Research CSV/XLSX containing images and optional notes.")
    parser.add_argument("--exp-prefix", default="research", help="Experiment prefix for research batches.")
    parser.add_argument("--prompts-dir", default="out_step1/research", help="Root directory for prepared inputs and prompt Excels.")
    parser.add_argument("--render-root", default="out_step2", help="Render output root.")
    parser.add_argument("--pairs-root", default="out_step3", help="Pairs output root.")

    parser.add_argument("--id-col", default="id", help="ID column name in the input file.")
    parser.add_argument("--image-col", default="image_path", help="Image path / URL column name in the input file.")
    parser.add_argument("--title-col", default="ori_title", help="Original title column name in the input file.")
    parser.add_argument("--promo-title-col", default="", help="Optional promo title column name.")
    parser.add_argument("--brand-col", default="brand", help="Brand column name in the input file.")
    parser.add_argument("--category-col", default="level_one_category_name", help="Level-one category column name.")
    parser.add_argument("--super-category-col", default="", help="Optional super-category column name.")
    parser.add_argument("--note-col", default="", help="Optional research note column name.")
    parser.add_argument("--condition-col", default="", help="Optional condition column name.")
    parser.add_argument("--persona-kind-col", default="", help="Optional persona kind column name.")
    parser.add_argument("--mbti-type-col", default="", help="Optional MBTI type column name.")
    parser.add_argument("--big5-types-col", default="", help="Optional Big Five token column name.")
    parser.add_argument("--schwartz-type-col", default="", help="Optional Schwartz value type column name.")

    parser.add_argument("--persona-kind", choices=["auto", "none", "mbti", "big5", "schwartz"], default="auto",
                        help="Default persona family when rows do not specify one.")
    parser.add_argument("--run-baseline", action="store_true", help="Also run a no-persona baseline over the prepared dataset.")

    parser.add_argument("--prompt-model", choices=["7b", "32b"], default="32b", help="Model size for prompt generation.")
    parser.add_argument("--style-constraints", choices=["on", "off"], default="on", help="Include style constraint tail.")
    parser.add_argument("--end-with-4k", choices=["on", "off"], default="on", help="Require prompts to end with 4k.")
    parser.add_argument("--disable-triad", action="store_true", help="Disable triad routing.")
    parser.add_argument("--note-mode", choices=["soft", "hard", "off"], default="soft", help="How research_note affects prompt generation.")
    parser.add_argument("--seed", type=int, default=2026, help="Global random seed.")

    parser.add_argument("--mbti-plan", choices=["none", "A", "B"], default="none", help="MBTI persona plan for research batches.")
    parser.add_argument("--mbti-profiles", default="mbti_profiles.csv", help="MBTI profiles CSV.")
    parser.add_argument("--mbti-key", default="id", help="Join key for MBTI profiles.")
    parser.add_argument("--mbti-type", choices=["", "ESTJ", "ESTP", "ESFJ", "ESFP", "ENTJ", "ENTP", "ENFJ", "ENFP",
                                                "ISTJ", "ISTP", "ISFJ", "ISFP", "INTJ", "INTP", "INFJ", "INFP"],
                        default="", help="Default MBTI type when rows do not specify one.")
    parser.add_argument("--mbti-mode", choices=["concat", "inline"], default="concat", help="How MBTI persona is applied.")

    parser.add_argument("--big5-plan", choices=["none", "A", "B"], default="none", help="Big Five persona plan.")
    parser.add_argument("--big5-profiles", default="big_five_profiles.csv", help="Big Five profiles CSV.")
    parser.add_argument("--big5-key", default="id", help="Join key for Big Five profiles.")
    parser.add_argument("--big5-types", default="", help="Default Big Five token set when rows do not specify one.")
    parser.add_argument("--big5-mode", choices=["concat", "inline"], default="concat", help="How Big Five persona is applied.")
    parser.add_argument("--big5-persona-style", choices=["legacy", "target"], default="legacy", help="Big Five wording style.")

    parser.add_argument("--schwartz-profiles", default="schwartz_value_profiles.csv", help="Schwartz profiles CSV.")
    parser.add_argument("--schwartz-key", default="id", help="Join key for Schwartz profiles.")
    parser.add_argument("--schwartz-type", default="", help="Default Schwartz value type when rows do not specify one.")
    parser.add_argument("--schwartz-mode", choices=["concat", "inline"], default="concat", help="How Schwartz persona is applied.")
    parser.add_argument("--schwartz-persona-style", choices=["legacy", "target"], default="legacy", help="Schwartz wording style.")

    parser.add_argument("--concat-persona-format", choices=["lead", "full"], default="lead",
                        help="When persona mode is concat: prepend a concise lead or the full persona instruction.")

    parser.add_argument("--skip-prompts", action="store_true", help="Skip prompt generation and reuse existing prompt Excels.")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip normalization.")
    parser.add_argument("--skip-render", action="store_true", help="Skip rendering.")
    parser.add_argument("--skip-pairs", action="store_true", help="Skip pair image generation.")
    parser.add_argument("--skip-kill-ollama", action="store_true", help="Do not pkill ollama before rendering.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing.")
    return parser.parse_args()


def read_table_auto(path: str) -> pd.DataFrame:
    fp = Path(path)
    ext = fp.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(fp)
    for enc in ("utf-8-sig", "gb18030", "utf-8", "latin1"):
        try:
            return pd.read_csv(fp, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(fp, encoding="utf-8", errors="ignore")


def text_or_empty(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    s = str(val).strip()
    return "" if not s or s.lower() == "nan" else s


def sanitize_tag(tag: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_\-]+", "_", str(tag or "").strip())
    return re.sub(r"_+", "_", cleaned).strip("_") or "research"


def normalize_persona_kind(val: str) -> str:
    s = text_or_empty(val).lower()
    if s == "schwartz_value":
        s = "schwartz"
    return s if s in ("none", "mbti", "big5", "schwartz") else ""


def run_cmd(cmd, desc: str) -> None:
    cmd_disp = " ".join(cmd)
    print(f"\n[RUN] {desc}\n  $ {cmd_disp}", flush=True)
    if DRY_RUN:
        print("  (dry-run) command skipped")
        return
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({desc}): {cmd_disp}")


def default_persona_kind(args) -> str:
    if args.persona_kind != "auto":
        return args.persona_kind
    active = []
    if args.mbti_plan != "none" or text_or_empty(args.mbti_type):
        active.append("mbti")
    if args.big5_plan != "none" or text_or_empty(args.big5_types):
        active.append("big5")
    if text_or_empty(args.schwartz_type):
        active.append("schwartz")
    if len(active) > 1:
        raise ValueError("Multiple default persona families implied; set --persona-kind explicitly.")
    return active[0] if active else "none"


def ensure_row_value(kind: str, row: pd.Series, args):
    if kind == "none":
        return ""
    if kind == "mbti":
        value = text_or_empty(row.get("mbti_type")) or text_or_empty(args.mbti_type)
        if value:
            return value.upper()
        if args.mbti_plan == "none":
            raise ValueError("MBTI batch requested but --mbti-plan is none and no row/default mbti_type provided.")
        return "__profile_join__"
    if kind == "big5":
        value = text_or_empty(row.get("big5_types")) or text_or_empty(args.big5_types)
        if not value:
            raise ValueError("Big Five batch requested but no row/default big5_types provided.")
        return value
    if kind == "schwartz":
        value = text_or_empty(row.get("schwartz_type")) or text_or_empty(args.schwartz_type)
        if not value:
            raise ValueError("Schwartz batch requested but no row/default schwartz_type provided.")
        return value
    raise ValueError(f"Unsupported persona kind: {kind}")


def build_batches(df: pd.DataFrame, args):
    default_kind = default_persona_kind(args)
    grouped_rows = {}
    for idx, row in df.iterrows():
        row_kind = normalize_persona_kind(row.get("persona_kind"))
        kind = row_kind or default_kind
        value = ensure_row_value(kind, row, args)
        grouped_rows.setdefault((kind, value), []).append(idx)

    batches = []
    for (kind, value), indices in grouped_rows.items():
        if kind == "none":
            label = "nopersona"
        elif kind == "mbti":
            label = f"mbti_{value.lower()}" if value != "__profile_join__" else "mbti_profiles"
        elif kind == "big5":
            label = "big5_" + sanitize_tag(value.lower())
        else:
            label = "schwartz_" + sanitize_tag(value.lower())
        batches.append({
            "kind": kind,
            "value": value,
            "label": label,
            "indices": indices,
            "is_baseline": False,
        })

    if args.run_baseline and any(batch["kind"] != "none" for batch in batches):
        batches.append({
            "kind": "none",
            "value": "",
            "label": "baseline",
            "indices": list(df.index),
            "is_baseline": True,
        })
    return batches


def build_prepare_cmd(args, out_xlsx: Path):
    cmd = [
        sys.executable, "prepare_research_inputs.py",
        "--input-csv", args.input_csv,
        "--out-xlsx", str(out_xlsx),
        "--id-col", args.id_col,
        "--image-col", args.image_col,
        "--title-col", args.title_col,
        "--brand-col", args.brand_col,
        "--category-col", args.category_col,
    ]
    if args.promo_title_col:
        cmd.extend(["--promo-title-col", args.promo_title_col])
    if args.super_category_col:
        cmd.extend(["--super-category-col", args.super_category_col])
    if args.note_col:
        cmd.extend(["--note-col", args.note_col])
    if args.condition_col:
        cmd.extend(["--condition-col", args.condition_col])
    if args.persona_kind_col:
        cmd.extend(["--persona-kind-col", args.persona_kind_col])
    if args.mbti_type_col:
        cmd.extend(["--mbti-type-col", args.mbti_type_col])
    if args.big5_types_col:
        cmd.extend(["--big5-types-col", args.big5_types_col])
    if args.schwartz_type_col:
        cmd.extend(["--schwartz-type-col", args.schwartz_type_col])
    return cmd


def build_prompt_cmd(args, subset_xlsx: Path, out_dir: Path, exp_tag: str,
                     use_existing_super: bool, has_note: bool, batch: dict):
    cmd = [
        sys.executable, "create_categorical_prompts.py",
        "--titles-file", str(subset_xlsx),
        "--out-dir", str(out_dir),
        "--model", args.prompt_model,
        "--persona-kind", batch["kind"],
        "--style-constraints", args.style_constraints,
        "--end-with-4k", args.end_with_4k,
        "--exp-name", exp_tag,
        "--seed", str(args.seed),
        "--concat-persona-format", args.concat_persona_format,
    ]
    if args.disable_triad:
        cmd.append("--disable-triad")
    if has_note:
        cmd.extend(["--note-col", "research_note", "--note-mode", args.note_mode])
    if use_existing_super:
        cmd.extend(["--use-existing-super-category", "--super-category-col", "super_category"])
    if batch["kind"] == "mbti":
        mbti_plan = args.mbti_plan if args.mbti_plan != "none" else "A"
        cmd.extend([
            "--mbti-plan", mbti_plan,
            "--mbti-profiles", args.mbti_profiles,
            "--mbti-key", args.mbti_key,
            "--mbti-mode", args.mbti_mode,
        ])
        if batch["value"] != "__profile_join__":
            cmd.extend(["--mbti-type", batch["value"]])
    elif batch["kind"] == "big5":
        cmd.extend([
            "--big5-plan", args.big5_plan,
            "--big5-profiles", args.big5_profiles,
            "--big5-key", args.big5_key,
            "--big5-types", batch["value"],
            "--big5-mode", args.big5_mode,
            "--big5-persona-style", args.big5_persona_style,
        ])
    elif batch["kind"] == "schwartz":
        cmd.extend([
            "--schwartz-profiles", args.schwartz_profiles,
            "--schwartz-key", args.schwartz_key,
            "--schwartz-type", batch["value"],
            "--schwartz-mode", args.schwartz_mode,
            "--schwartz-persona-style", args.schwartz_persona_style,
        ])
    return cmd


def main():
    args = parse_args()
    set_dry_run(args.dry_run)

    exp_prefix = sanitize_tag(args.exp_prefix)
    work_dir = Path(args.prompts_dir) / exp_prefix
    work_dir.mkdir(parents=True, exist_ok=True)

    prepared_xlsx = work_dir / "step1_titles_prepared.xlsx"
    run_cmd(build_prepare_cmd(args, prepared_xlsx), "prepare research inputs")

    prepared_df = read_table_auto(str(prepared_xlsx))
    if prepared_df.empty:
        raise RuntimeError("Prepared research input is empty.")
    has_existing_super = (
        "super_category" in prepared_df.columns
        and prepared_df["super_category"].astype(str).str.strip().replace("nan", "").ne("").any()
    )

    batches = build_batches(prepared_df, args)
    print(f"[Info] Prepared rows={len(prepared_df)} | batches={len(batches)}")

    for batch in batches:
        exp_tag = sanitize_tag(f"{exp_prefix}_{batch['label']}")
        subset_xlsx = work_dir / f"step1_titles_{exp_tag}.xlsx"
        prompt_xlsx = work_dir / f"step1_prompts_{exp_tag}.xlsx"

        subset_df = prepared_df.loc[batch["indices"]].copy().reset_index(drop=True)
        has_note = "research_note" in subset_df.columns
        if not DRY_RUN:
            subset_df.to_excel(subset_xlsx, index=False)
        print(
            f"[Batch] exp={exp_tag} kind={batch['kind']} value={batch['value'] or 'n/a'} "
            f"rows={len(subset_df)} baseline={batch['is_baseline']}",
            flush=True,
        )

        if args.skip_prompts:
            if not prompt_xlsx.exists():
                raise FileNotFoundError(f"--skip-prompts requires existing file: {prompt_xlsx}")
            print(f"[SKIP] reuse prompts: {prompt_xlsx}")
        else:
            run_cmd(
                build_prompt_cmd(args, subset_xlsx, work_dir, exp_tag, has_existing_super, has_note, batch),
                f"generate prompts ({exp_tag})",
            )

        if args.skip_normalize:
            print(f"[SKIP] normalize: {prompt_xlsx}")
        else:
            run_cmd(
                [
                    sys.executable, "normalize_scale_and_canvas.py",
                    "--excel", str(prompt_xlsx),
                    "--out-dir", str(work_dir),
                    "--image-col", "image_url",
                ],
                f"normalize images ({exp_tag})",
            )

        if not args.skip_render and not args.skip_kill_ollama:
            print(f"[info] pkill -9 ollama before render ({exp_tag})")
            if not DRY_RUN:
                subprocess.run(["pkill", "-9", "ollama"], cwd=REPO_ROOT, check=False)

        if args.skip_render:
            print(f"[SKIP] render: {exp_tag}")
        else:
            run_cmd(
                [
                    sys.executable, "render_with_comfyui.py",
                    "--prompts-file", str(prompt_xlsx),
                    "--exp-name", exp_tag,
                    "--output-root", args.render_root,
                ],
                f"ComfyUI render ({exp_tag})",
            )

        if args.skip_pairs:
            print(f"[SKIP] pairs: {exp_tag}")
        else:
            run_cmd(
                [
                    sys.executable, "merge_pairs.py",
                    "--prompts-file", str(prompt_xlsx),
                    "--generated-dir", str(Path(args.render_root) / exp_tag),
                    "--output-dir", str(Path(args.pairs_root) / exp_tag),
                ],
                f"build pairs ({exp_tag})",
            )


if __name__ == "__main__":
    main()
