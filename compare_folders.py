#!/usr/bin/env python3
"""
compare_folders_missing_only.py

Purpose:
  For each source file (in folder A or folder B depending on MODE), compare it against
  every file in the target folder and write ONLY the lines that are present in the
  source but missing in the target. Empty lines are ignored.

How to use:
  1) Edit the three variables in the "COPY YOUR PATHS / MODE HERE" section:
       - folderA, folderB, output_dir
       - MODE: 'A_to_B', 'B_to_A', or 'BOTH'
  2) Run:
       python compare_folders_missing_only.py

Notes on behavior:
  - For each source file (e.g., a1.txt), the script writes a file:
        a1.txt.vs_all_from_<target_folder>.missing.txt
    which contains, per target file, only the missing lines (with original line numbers).
  - If a source line appears multiple times and it is missing in the target, every occurrence
    is reported with its original line number.
  - Threading is used per-source-file; each thread writes to its own dedicated output file
    so there are no race conditions.
"""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys

# -------------------------------------------------------------------
# >>>> COPY YOUR PATHS / SET MODE HERE <<<<
# Put absolute or relative paths to your folders here:
folderA = Path("/absolute/path/to/folderA")       # <-- CHANGE THIS (Folder A)
folderB = Path("/absolute/path/to/folderB")       # <-- CHANGE THIS (Folder B)
output_dir = Path("/absolute/path/to/output_dir") # <-- CHANGE THIS (Output folder)

# MODE options:
#   'A_to_B'  -> Compare each file in folderA against all files in folderB
#   'B_to_A'  -> Compare each file in folderB against all files in folderA
#   'BOTH'    -> Do both passes
MODE = "BOTH"  # <-- set to 'A_to_B', 'B_to_A' or 'BOTH' (case-insensitive)

# Number of worker threads to compare multiple source files concurrently:
NUM_WORKER_THREADS = 4
# -------------------------------------------------------------------

def read_source_non_empty_lines_with_lineno(path: Path):
    """
    Return a list of tuples (orig_line_number, line_text) for non-empty lines in the file.
    Preserves original line numbers. Skips lines where line.strip() == ''.
    """
    result = []
    with path.open('r', encoding='utf-8', errors='replace') as f:
        for i, raw in enumerate(f, start=1):
            line = raw.rstrip('\n\r')
            if line.strip() == '':
                continue
            result.append((i, line))
    return result

def build_target_line_set(path: Path):
    """
    Return a set of non-empty lines present in the target file.
    Using a set gives O(1) membership tests for missing-line detection.
    """
    s = set()
    with path.open('r', encoding='utf-8', errors='replace') as f:
        for raw in f:
            line = raw.rstrip('\n\r')
            if line.strip() == '':
                continue
            s.add(line)
    return s

def compare_source_to_target_missing(source_lines_with_ln, target_line_set):
    """
    Given source_lines_with_ln: list[(orig_ln, line)]
          target_line_set: set(line)
    Return a list of strings describing missing source lines (preserving orig ln).
    """
    missing_entries = []
    for orig_ln, line in source_lines_with_ln:
        if line not in target_line_set:
            # Record missing occurrence; include the source original line number.
            missing_entries.append((orig_ln, line))
    return missing_entries

def compare_source_to_targets_and_write(source_path: Path, target_paths: list, output_dir: Path, source_folder_name: str, target_folder_name: str):
    """
    Compare one source file to each target file and write ONLY missing lines
    into a dedicated output file.
    """
    out_filename = f"{source_path.name}.vs_all_from_{target_folder_name}.missing.txt"
    out_path = output_dir / out_filename

    # Read source lines once
    source_lines = read_source_non_empty_lines_with_lineno(source_path)
    if not source_lines:
        # quickly write a small output noting source had no non-empty lines
        with out_path.open('w', encoding='utf-8') as out:
            out.write(f"# Source file: {source_path}\n")
            out.write(f"# Target folder: {target_folder_name} ({len(target_paths)} files)\n")
            out.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            out.write("# NOTE: Source file has no non-empty lines (after trimming). No missing lines.\n")
        return

    with out_path.open('w', encoding='utf-8') as out:
        out.write("# Missing-lines report\n")
        out.write(f"# Source file: {source_path}\n")
        out.write(f"# Target folder: {target_folder_name} ({len(target_paths)} files)\n")
        out.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        overall_any_missing = False

        # For each target file, build its set and test membership
        for tgt in target_paths:
            out.write("=" * 80 + "\n")
            out.write(f"Target file: {tgt.name}\n")
            out.write("-" * 80 + "\n")

            try:
                tgt_set = build_target_line_set(tgt)
            except Exception as e:
                out.write(f"# ERROR reading target file {tgt}: {e}\n\n")
                continue

            missing = compare_source_to_target_missing(source_lines, tgt_set)
            if not missing:
                out.write("(no missing lines — every non-empty line of source is present in this target)\n\n")
            else:
                overall_any_missing = True
                # Write each missing occurrence with original source line number
                for orig_ln, line in missing:
                    # Present content as-is (not quoted) and include line number
                    out.write(f"src_ln={orig_ln}: {line}\n")
                out.write("\n")

        if not overall_any_missing:
            out.write("# Overall: No missing lines found for this source file against all targets.\n")

def gather_text_files(folder: Path):
    """Return sorted list of .txt files (regular files) in folder."""
    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Folder does not exist or is not a directory: {folder}")
    files = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == '.txt'])
    return files

def run_pass(source_folder: Path, target_folder: Path, output_dir: Path, threads: int):
    """
    For each file in source_folder, compare it with all files in target_folder and
    write missing-only outputs.
    """
    source_files = gather_text_files(source_folder)
    target_files = gather_text_files(target_folder)

    if not source_files:
        print(f"No .txt files found in source folder: {source_folder}", file=sys.stderr)
        return
    if not target_files:
        print(f"No .txt files found in target folder: {target_folder}", file=sys.stderr)
        return

    source_folder_name = source_folder.name or "source"
    target_folder_name = target_folder.name or "target"

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = []
        for src in source_files:
            futures.append(ex.submit(compare_source_to_targets_and_write, src, target_files, output_dir, source_folder_name, target_folder_name))
        for f in as_completed(futures):
            # will re-raise exceptions from worker if any
            f.result()

def main():
    mode_val = (MODE or "BOTH").strip().upper()
    if mode_val not in ("A_TO_B", "B_TO_A", "BOTH"):
        print("MODE must be one of 'A_to_B', 'B_to_A', or 'BOTH'.", file=sys.stderr)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    if mode_val in ("A_TO_B", "BOTH"):
        print(f"Running A -> all B missing-lines pass: {folderA} -> {folderB}")
        try:
            run_pass(folderA, folderB, output_dir, NUM_WORKER_THREADS)
        except Exception as e:
            print("Error during A->B pass:", e, file=sys.stderr)

    if mode_val in ("B_TO_A", "BOTH"):
        print(f"Running B -> all A missing-lines pass: {folderB} -> {folderA}")
        try:
            run_pass(folderB, folderA, output_dir, NUM_WORKER_THREADS)
        except Exception as e:
            print("Error during B->A pass:", e, file=sys.stderr)

    print(f"\nDone. Missing-lines outputs are in: {output_dir}")
    try:
        for p in sorted(output_dir.iterdir()):
            if p.is_file():
                print(" -", p.name)
    except Exception:
        pass

if __name__ == "__main__":
    main()
