#!/usr/bin/env python3
"""
批量重命名图片：以 CSV 中的 ori_title 替换文件名。

用法（默认路径已经写好，如需调整可通过参数传入）:
    python scripts/rename_images_from_csv.py
    python scripts/rename_images_from_csv.py --dry-run
    python scripts/rename_images_from_csv.py \
        --csv /path/to/白底商品信息类目_experiment.csv \
        --base /path/to/big_five_test1214 /path/to/mbti_test1216
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Tuple

DEFAULT_BASE_DIRS = [
    Path("/Users/samxie/Research/YoYoRecomSys_GenPic/Research_Proj/MBTI/Exper_output/big_five_test1214"),
    Path("/Users/samxie/Research/YoYoRecomSys_GenPic/Research_Proj/MBTI/Exper_output/mbti_test1216"),
]

DEFAULT_CSV_PATH = Path(
    "/Users/samxie/Research/YoYoRecomSys_GenPic/Research_Proj/MBTI/"
    "yoyo_image_gen_mbti/白底商品信息类目_experiment.csv"
)

INVALID_CHAR_PATTERN = re.compile(r'[\\/:*?"<>|\r\n]+')
WHITESPACE_PATTERN = re.compile(r"\s+")
FILENAME_ID_PATTERN = re.compile(r"^(\d+)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="根据 CSV 的 ori_title 批量重命名图片")
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help="包含 id 与 ori_title 的 CSV 路径（默认即需求中的路径）",
    )
    parser.add_argument(
        "--base",
        type=Path,
        nargs="+",
        default=DEFAULT_BASE_DIRS,
        help="需要处理的顶层目录，脚本只会进入其下一层目录扫描图片",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印重命名计划，不真正移动文件",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="CSV 文件编码（默认 utf-8）",
    )
    return parser.parse_args()


def sanitize_title(title: str) -> str:
    """去掉非法字符，并把连续空白压缩为单个下划线。"""
    cleaned = INVALID_CHAR_PATTERN.sub("", title)
    cleaned = cleaned.strip()
    if not cleaned:
        return ""
    cleaned = WHITESPACE_PATTERN.sub("_", cleaned)
    return cleaned


def build_title_mapping(csv_path: Path, encoding: str = "utf-8") -> Dict[str, str]:
    """读取 CSV，返回 id -> 清洗后的 ori_title 映射。"""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 文件不存在: {csv_path}")

    with csv_path.open("r", encoding=encoding, newline="") as fp:
        reader = csv.DictReader(fp)
        required_columns = {"id", "ori_title"}
        missing = required_columns - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV 缺少列: {', '.join(sorted(missing))}")

        mapping: Dict[str, str] = {}
        duplicates: Dict[str, int] = defaultdict(int)
        for row in reader:
            raw_id = (row.get("id") or "").strip()
            if not raw_id:
                continue
            ori_title = (row.get("ori_title") or "").strip()
            sanitized = sanitize_title(ori_title)
            if not sanitized:
                logging.warning("ID %s 的标题为空或全是非法字符，跳过", raw_id)
                continue
            duplicates[raw_id] += 1
            if raw_id not in mapping:
                mapping[raw_id] = sanitized
        duplicate_ids = [key for key, count in duplicates.items() if count > 1]
        if duplicate_ids:
            logging.info("CSV 中存在重复 ID（以首个出现的标题为准）：%s", ", ".join(duplicate_ids[:10]))
        logging.info("共加载 %s 条标题", len(mapping))
        return mapping


def iter_target_files(base_dirs: Iterable[Path]) -> Iterable[Tuple[Path, Path]]:
    """
    遍历 base_dir/子目录/*.* 文件，返回 (目录, 文件)。
    仅处理“第二层”文件（即 base_dir 的直接子目录里的文件）。
    """
    for base_dir in base_dirs:
        if not base_dir.exists():
            logging.warning("目录不存在，跳过: %s", base_dir)
            continue
        for child in sorted(base_dir.iterdir()):
            if not child.is_dir():
                continue
            for file_path in sorted(child.iterdir()):
                if file_path.is_file():
                    yield child, file_path


def rename_files(
    mapping: Dict[str, str],
    base_dirs: Iterable[Path],
    dry_run: bool = False,
) -> None:
    total = 0
    renamed = 0
    skipped_no_id = 0
    skipped_no_title = 0

    for parent_dir, file_path in iter_target_files(base_dirs):
        total += 1
        match = FILENAME_ID_PATTERN.match(file_path.stem)
        if not match:
            skipped_no_id += 1
            logging.debug("文件名不含数字前缀，跳过: %s", file_path)
            continue

        file_id = match.group(1)
        title = mapping.get(file_id)
        if not title:
            skipped_no_title += 1
            logging.debug("CSV 中找不到 ID=%s 的标题，跳过 %s", file_id, file_path)
            continue

        new_name = f"{file_id}_{title}{file_path.suffix.lower()}"
        new_path = parent_dir / new_name
        if new_path == file_path:
            continue

        counter = 1
        unique_path = new_path
        while unique_path.exists() and unique_path != file_path:
            unique_path = parent_dir / f"{file_id}_{title}_{counter}{file_path.suffix.lower()}"
            counter += 1

        logging.info("重命名：%s -> %s", file_path, unique_path)
        if not dry_run:
            file_path.rename(unique_path)
        renamed += 1

    logging.info(
        "处理完成：总计 %s 个文件，成功 %s，未匹配 ID %s，未匹配标题 %s",
        total,
        renamed,
        skipped_no_id,
        skipped_no_title,
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    mapping = build_title_mapping(args.csv, encoding=args.encoding)
    rename_files(mapping, args.base, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
