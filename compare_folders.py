#!/usr/bin/env python3
"""
check_ids_presence.py

Purpose:
  For each file in Folder A, extract 12-digit customer IDs and check whether each ID
  is present in any file inside Folder B. Produce per-source detailed presence file,
  missing-only file, per-source missing count file, and an overall summary file.

How to use:
  1) Edit the three variables in the "COPY YOUR PATHS / MODE HERE" section:
       - folderA, folderB, output_dir
       - MODE: 'A_to_B', 'B_to_A', or 'BOTH' (default is 'A_to_B')
  2) Run:
       python check_ids_presence.py

Notes:
  - ID pattern used: exactly 12 consecutive digits (regex r'\b\d{12}\b').
  - Matching is exact string match on the digits.
  - Empty lines are ignored only insofar as they have no IDs.
  - This script is optimized to scan the target folder files (B) once per pass
    and use a set for O(1) membership checks.
"""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time
import sys

# -------------------------------------------------------------------
# >>>> COPY YOUR PATHS / SET MODE HERE <<<<
# Put absolute or relative paths to your folders here:
folderA = Path("/absolute/path/to/folderA")       # <-- CHANGE THIS (Folder A)
folderB = Path("/absolute/path/to/folderB")       # <-- CHANGE THIS (Folder B)
output_dir = Path("/absolute/path/to/output_dir") # <-- CHANGE THIS (Output folder)

# MODE options:
#   'A_to_B'  -> For each file in folderA, check presence in union of folderB
#   'B_to_A'  -> For each file in folderB, check presence in union of folderA
#   'BOTH'    -> Do both passes
MODE = "A_to_B"  # <-- set to 'A_to_B', 'B_to_A' or 'BOTH' (case-insensitive)

# Number of worker threads to process multiple source files concurrently:
NUM_WORKER_THREADS = 4
# -------------------------------------------------------------------

ID_PATTERN = re.compile(r'\b\d{12}\b')  # matches exactly 12 consecutive digits

def extract_ids_with_locations(path: Path):
    """
    Scan file and return a dict: id_str -> list of occurrences [(line_no, line_text), ...]
    Only IDs matching the 12-digit pattern are considered.
    """
    ids = {}
    try:
        with path.open('r', encoding='utf-8', errors='replace') as fh:
            for i, raw in enumerate(fh, start=1):
                line = raw.rstrip('\n\r')
                # find all 12-digit ids in this line
                for m in ID_PATTERN.findall(line):
                    ids.setdefault(m, []).append((i, line))
    except Exception as e:
        raise RuntimeError(f"Error reading file {path}: {e}")
    return ids

def build_union_id_set_for_folder(folder: Path):
    """
    Build and return a set of all 12-digit IDs found across all .txt files in folder.
    This scans each file once (streaming).
    """
    id_set = set()
    for p in sorted(folder.iterdir()):
        if not p.is_file() or p.suffix.lower() != '.txt':
            continue
        try:
            with p.open('r', encoding='utf-8', errors='replace') as fh:
                for raw in fh:
                    # findall returns list of id strings in the line
                    for m in ID_PATTERN.findall(raw):
                        id_set.add(m)
        except Exception as e:
            # If a single file fails, we log and continue (do not abort whole pass)
            print(f"Warning: cannot read {p}: {e}", file=sys.stderr)
    return id_set

def process_source_file_ids(source_path: Path, target_union_set: set, output_dir: Path, source_label: str, target_label: str):
    """
    For a given source file, extract IDs and check presence in target_union_set.
    Writes three files to output_dir:
      - <source>.ids_presence.txt   : ID, FOUND/NOT_FOUND, occurrence lines (line numbers)
      - <source>.ids_missing.txt    : missing IDs only (one per line; optionally with occurrence lines)
      - <source>.ids_missing.count.txt : single integer (number of unique missing IDs)
    Returns dict: { 'source': source_name, 'missing_count': int, 'missing_ids': set(...) }
    """
    source_name = source_path.name
    out_presence = output_dir / f"{source_name}.ids_presence.txt"
    out_missing = output_dir / f"{source_name}.ids_missing.txt"
    out_count = output_dir / f"{source_name}.ids_missing.count.txt"

    # Extract IDs and their occurrences from source
    try:
        ids_map = extract_ids_with_locations(source_path)  # dict: id -> [(ln, line), ...]
    except Exception as e:
        # write an error file and return zero result
        err_path = output_dir / f"{source_name}.ids_error.txt"
        err_path.write_text(f"ERROR reading source file {source_path}: {e}\n", encoding='utf-8')
        return {'source': source_name, 'missing_count': 0, 'missing_ids': set()}

    unique_ids = sorted(ids_map.keys())

    # Prepare results
    found_ids = []
    missing_ids = []

    for idv in unique_ids:
        if idv in target_union_set:
            found_ids.append(idv)
        else:
            missing_ids.append(idv)

    # Write presence file
    with out_presence.open('w', encoding='utf-8') as f:
        f.write("# ID presence report\n")
        f.write(f"# Source file: {source_path}\n")
        f.write(f"# Target folder (union): {target_label}\n")
        f.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        if not unique_ids:
            f.write("# NOTE: No 12-digit IDs found in source file.\n")
        else:
            f.write("ID,STATUS,occurrences\n")
            for idv in unique_ids:
                status = "FOUND" if idv in target_union_set else "NOT_FOUND"
                occs = ids_map.get(idv, [])
                # format occurrences as semicolon-separated "ln:summary" (truncate long lines)
                occ_strs = []
                for ln, line in occs:
                    # keep up to first 120 chars of line for readability
                    snippet = (line[:120] + '...') if len(line) > 120 else line
                    # escape commas in snippet to keep CSV-like structure simple (replace with space)
                    snippet = snippet.replace(',', ' ')
                    occ_strs.append(f"{ln}:{snippet}")
                f.write(f"{idv},{status},\"{' | '.join(occ_strs)}\"\n")

    # Write missing-only file
    with out_missing.open('w', encoding='utf-8') as f:
        f.write(f"# Missing IDs from source: {source_name} (not present in {target_label})\n")
        f.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        if not missing_ids:
            f.write("# (none) - all IDs present in target folder union.\n")
        else:
            for idv in missing_ids:
                # optionally write occurrences as well
                occs = ids_map.get(idv, [])
                occ_lines = "; ".join(f"{ln}" for ln, _ in occs)
                f.write(f"{idv}")
                # include occurrence line numbers in parentheses to help locate source
                if occ_lines:
                    f.write(f"  # src_lines: {occ_lines}")
                f.write("\n")

    # Write count file (single integer)
    missing_count = len(missing_ids)
    with out_count.open('w', encoding='utf-8') as f:
        f.write(str(missing_count) + "\n")

    return {'source': source_name, 'missing_count': missing_count, 'missing_ids': set(missing_ids)}

def gather_text_files(folder: Path):
    """Return sorted list of .txt files (regular files) in folder."""
    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Folder does not exist or is not a directory: {folder}")
    files = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == '.txt'])
    return files

def run_pass_ids(source_folder: Path, target_folder: Path, output_dir: Path, threads: int):
    """
    Build union ID set from target_folder, then for each source file in source_folder
    check presence against that union set in parallel (threads). Returns list of result dicts.
    """
    source_files = gather_text_files(source_folder)
    target_files = gather_text_files(target_folder)

    if not source_files:
        print(f"No .txt files found in source folder: {source_folder}", file=sys.stderr)
        return []
    if not target_files:
        print(f"No .txt files found in target folder: {target_folder}", file=sys.stderr)
        return []

    # Build union set of IDs from all target files (scan once)
    print(f"Building union of IDs from target folder: {target_folder} (this may take some time for large folders)...")
    target_union = build_union_id_set_for_folder(target_folder)
    print(f"Done. Found {len(target_union)} unique 12-digit IDs in target folder.")

    results = []
    source_folder_name = source_folder.name or "source"
    target_folder_name = target_folder.name or "target"

    # Process each source file in parallel; each worker will write its own files
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = []
        for src in source_files:
            futures.append(ex.submit(process_source_file_ids, src, target_union, output_dir, source_folder_name, target_folder_name))
        for f in as_completed(futures):
            try:
                res = f.result()
                if res:
                    results.append(res)
            except Exception as e:
                print(f"Error processing a source file: {e}", file=sys.stderr)
    return results

def write_ids_summary(results: list, output_dir: Path, pass_label: str):
    """
    Write a summary file listing per-source missing counts, sum of counts, and
    unique missing IDs across all sources.
    """
    summary_path = output_dir / f"ids_presence_summary.{pass_label}.summary.txt"
    grand_sum = 0
    union_missing_ids = set()

    with summary_path.open('w', encoding='utf-8') as s:
        s.write(f"# IDs presence summary - pass: {pass_label}\n")
        s.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        if not results:
            s.write("# No results (no source or target files?)\n")
            return summary_path

        s.write("SOURCE_FILE,TOTAL_MISSING_UNIQUE\n")
        for r in sorted(results, key=lambda x: x['source']):
            s.write(f"{r['source']},{r['missing_count']}\n")
            grand_sum += int(r['missing_count'] or 0)
            union_missing_ids.update(r.get('missing_ids', set()))

        s.write("\n")
        s.write(f"SUM_OF_PER_SOURCE_MISSING_COUNTS,{grand_sum}\n")
        s.write(f"UNIQUE_MISSING_IDS_ACROSS_ALL_SOURCES,{len(union_missing_ids)}\n")
        s.write("\n")
        if union_missing_ids:
            s.write("# List of unique missing IDs across all sources (one per line):\n")
            for idv in sorted(union_missing_ids):
                s.write(idv + "\n")

    return summary_path

def main():
    mode_val = (MODE or "A_to_B").strip().upper()
    if mode_val not in ("A_TO_B", "B_TO_A", "BOTH"):
        print("MODE must be one of 'A_to_B', 'B_to_A', or 'BOTH'.", file=sys.stderr)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    summary_paths = []

    if mode_val in ("A_TO_B", "BOTH"):
        print(f"Running A ->
