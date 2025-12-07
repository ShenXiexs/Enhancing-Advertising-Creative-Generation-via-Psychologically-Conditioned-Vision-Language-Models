#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据 Step1/Step2 的 Excel，将 out_step3/... 下的 {id}_pair.jpg
重命名为 {id}_{super_category}_pair.jpg，方便按大类管理。
"""
import argparse
import os
import re
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

MBTI_REGEX = r"(ESTJ|ESTP|ESFJ|ESFP|ENTJ|ENTP|ENFJ|ENFP|ISTJ|ISTP|ISFJ|ISFP|INTJ|INTP|INFJ|INFP)"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Rename pair images to include category suffix."
    )
    parser.add_argument(
        "--prompts-file",
        required=True,
        help="Excel file containing id + category columns（通常是 step1_prompts_xxx.xlsx）",
    )
    parser.add_argument(
        "--pairs-dir",
        required=True,
        help="Directory storing pair images（例如 out_step3/<exp_tag>）。",
    )
    parser.add_argument(
        "--id-col",
        default="id",
        help="ID column name in prompts Excel (default: id).",
    )
    parser.add_argument(
        "--category-col",
        default="super_category",
        help="Category column name to append (default: super_category).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without renaming files.",
    )
    parser.add_argument(
        "--keep-original",
        action="store_true",
        help="If set, keep old files and only copy to new names.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="When set, search for pair images in all subdirectories under pairs-dir.",
    )
    parser.add_argument(
        "--append-mbti",
        action="store_true",
        help="Append MBTI type (inferred from directory name) to the filename suffix.",
    )
    return parser.parse_args()


def normalize_id(val) -> str:
    if val is None:
        return ""
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    text = str(val).strip()
    return text


def sanitize_category(cat: str) -> str:
    cat = (cat or "").strip()
    if not cat:
        return "unknown"
    # 替换非法文件字符
    cat = re.sub(r"[\\/:*?\"<>|]", "_", cat)
    cat = re.sub(r"\s+", "_", cat)
    cat = cat.strip("_")
    return cat or "unknown"


def read_mapping(path: str, id_col: str, cat_col: str) -> Dict[str, str]:
    df = pd.read_excel(path)
    if id_col not in df.columns:
        raise ValueError(f"Excel 缺少 id 列：{id_col}")
    if cat_col not in df.columns:
        raise ValueError(f"Excel 缺少类别列：{cat_col}")
    mapping = {}
    for _, row in df.iterrows():
        key = normalize_id(row[id_col])
        if not key:
            continue
        mapping[key] = sanitize_category(row[cat_col])
    if not mapping:
        raise RuntimeError("Excel 中没有有效的 id/category 对")
    return mapping


def extract_mbti_from_path(file_path: Path, root: Path) -> Optional[str]:
    rel_parts = file_path.relative_to(root).parts
    pattern = re.compile(MBTI_REGEX, re.I)
    for part in rel_parts:
        match = pattern.search(part)
        if match:
            return match.group(1).upper()
    return None


def find_existing_target(pid: str, cat: str, root: Path, recursive: bool) -> Optional[Path]:
    pattern = re.compile(
        rf"^{re.escape(pid)}_{re.escape(cat)}(?:_{MBTI_REGEX})?_pair\.jpg$",
        re.I,
    )
    iterator = (
        root.rglob(f"{pid}_*_pair.jpg") if recursive else root.glob(f"{pid}_*_pair.jpg")
    )
    for path in iterator:
        if pattern.match(path.name):
            return path
    return None


def main():
    args = parse_args()
    mapping = read_mapping(args.prompts_file, args.id_col, args.category_col)
    pairs_dir = Path(args.pairs_dir)
    if not pairs_dir.exists():
        raise FileNotFoundError(f"pair 目录不存在：{pairs_dir}")

    renamed = 0
    skipped = 0
    for pid, cat in mapping.items():
        existing = find_existing_target(pid, cat, pairs_dir, args.recursive)
        if existing:
            print(f"[SKIP] 已存在目标命名：{existing.relative_to(pairs_dir)}")
            skipped += 1
            continue
        old_name = f"{pid}_pair.jpg"
        if args.recursive:
            candidates = list(pairs_dir.rglob(old_name))
            if not candidates:
                skipped += 1
                continue
            old_path = candidates[0]
        else:
            old_path = pairs_dir / old_name
            if not old_path.exists():
                skipped += 1
                continue
        suffix_parts = [cat]
        if args.append_mbti:
            mbti = extract_mbti_from_path(old_path, pairs_dir)
            if mbti:
                suffix_parts.append(mbti)
        suffix = "_".join([p for p in suffix_parts if p])
        new_name = f"{pid}_{suffix}_pair.jpg"
        new_path = old_path.with_name(new_name)
        if new_path.exists():
            skipped += 1
            continue
        action = "COPY" if args.keep_original else "RENAME"
        rel_root = old_path.relative_to(pairs_dir)
        print(f"[{action}] {rel_root} -> {new_path.relative_to(pairs_dir)}")
        if args.dry_run:
            continue
        if args.keep_original:
            from shutil import copy2
            copy2(old_path, new_path)
        else:
            os.rename(old_path, new_path)
        renamed += 1

    print(f"\n完成：重命名 {renamed} 个，跳过 {skipped} 个。目标目录：{pairs_dir}")
    if args.dry_run:
        print("（dry-run 模式，仅打印计划，不做实际改动）")


if __name__ == "__main__":
    main()
