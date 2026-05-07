#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prepare a research-friendly CSV/XLSX into a step1_titles.xlsx-compatible table.

This adapter does not call any model or ComfyUI service. It only normalizes
columns so downstream prompt / normalize / render scripts can reuse the
existing pipeline unchanged.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Prepare research image+notes input into a step1_titles.xlsx-compatible file."
    )
    parser.add_argument("--input-csv", required=True, help="Input CSV/XLSX containing images and optional notes.")
    parser.add_argument("--out-xlsx", required=True, help="Output Excel path compatible with step1_titles.xlsx.")
    parser.add_argument("--id-col", default="id", help="ID column name in the input file.")
    parser.add_argument("--image-col", default="image_path", help="Image path / image URL column name.")
    parser.add_argument("--title-col", default="ori_title", help="Original title column name.")
    parser.add_argument("--promo-title-col", default="", help="Optional promo title column name.")
    parser.add_argument("--brand-col", default="brand", help="Brand column name.")
    parser.add_argument("--category-col", default="level_one_category_name", help="Level-one category column name.")
    parser.add_argument("--super-category-col", default="", help="Optional super category column name.")
    parser.add_argument("--note-col", default="", help="Optional research note / prompt hint column name.")
    parser.add_argument("--condition-col", default="", help="Optional condition column name.")
    parser.add_argument("--persona-kind-col", default="", help="Optional persona kind column name.")
    parser.add_argument("--mbti-type-col", default="", help="Optional MBTI type column name.")
    parser.add_argument("--big5-types-col", default="", help="Optional Big Five token column name.")
    parser.add_argument("--schwartz-type-col", default="", help="Optional Schwartz value type column name.")
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


def resolve_col(df: pd.DataFrame, name: str) -> str | None:
    if not name:
        return None
    if name in df.columns:
        return name
    target = str(name).strip().lower()
    for col in df.columns:
        if str(col).strip().lower() == target:
            return col
    return None


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


def resolve_image_source(raw: str, base_dir: Path) -> str:
    src = text_or_empty(raw)
    if not src:
        return ""
    if src.startswith("//"):
        return "https:" + src
    if src.startswith("http://") or src.startswith("https://"):
        return src
    p = Path(src)
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return str(p)


def require_col(df: pd.DataFrame, name: str, label: str) -> str:
    col = resolve_col(df, name)
    if not col:
        raise ValueError(f"Missing required column for {label}: {name}")
    return col


def main():
    args = parse_args()
    input_path = Path(args.input_csv)
    out_path = Path(args.out_xlsx)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = read_table_auto(str(input_path))
    df.columns = [str(c).strip() for c in df.columns]
    base_dir = input_path.resolve().parent

    id_col = require_col(df, args.id_col, "id")
    image_col = require_col(df, args.image_col, "image")

    title_col = resolve_col(df, args.title_col)
    promo_title_col = resolve_col(df, args.promo_title_col)
    brand_col = resolve_col(df, args.brand_col)
    category_col = resolve_col(df, args.category_col)
    super_category_col = resolve_col(df, args.super_category_col)
    note_col = resolve_col(df, args.note_col)
    condition_col = resolve_col(df, args.condition_col)
    persona_kind_col = resolve_col(df, args.persona_kind_col)
    mbti_type_col = resolve_col(df, args.mbti_type_col)
    big5_types_col = resolve_col(df, args.big5_types_col)
    schwartz_type_col = resolve_col(df, args.schwartz_type_col)

    if not category_col and not super_category_col:
        raise ValueError("Need at least one of --category-col or --super-category-col")

    records = []
    for idx, row in df.iterrows():
        item_id = text_or_empty(row.get(id_col)) or str(idx + 1)
        ori_title = text_or_empty(row.get(title_col)) if title_col else ""
        if not ori_title:
            ori_title = f"research_item_{item_id}"
        promo_title = text_or_empty(row.get(promo_title_col)) if promo_title_col else ""
        if not promo_title:
            promo_title = ori_title
        level_one = text_or_empty(row.get(category_col)) if category_col else ""
        super_category = text_or_empty(row.get(super_category_col)) if super_category_col else ""

        records.append({
            "id": item_id,
            "ori_title": ori_title,
            "brand": text_or_empty(row.get(brand_col)) if brand_col else "",
            "image_url": resolve_image_source(row.get(image_col), base_dir),
            "level_one_category_name": level_one,
            "super_category": super_category,
            "promo_title_final": promo_title,
            "white_bg_image": "",
            "research_note": text_or_empty(row.get(note_col)) if note_col else "",
            "condition": text_or_empty(row.get(condition_col)) if condition_col else "",
            "persona_kind": text_or_empty(row.get(persona_kind_col)) if persona_kind_col else "",
            "mbti_type": text_or_empty(row.get(mbti_type_col)) if mbti_type_col else "",
            "big5_types": text_or_empty(row.get(big5_types_col)) if big5_types_col else "",
            "schwartz_type": text_or_empty(row.get(schwartz_type_col)) if schwartz_type_col else "",
        })

    out_df = pd.DataFrame(records)
    out_df.to_excel(out_path, index=False)

    print("===> [DONE] Research input preparation")
    print(f"Input : {input_path}")
    print(f"Output: {out_path}")
    print(f"Rows  : {len(out_df)}")


if __name__ == "__main__":
    main()
