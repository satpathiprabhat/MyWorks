#!/usr/bin/env python3
"""
compare_folders_missing_with_counts.py

Enhancements over previous version:
- Reports only missing lines (source lines absent in each target).
- Produces per-source output files like:
      <source>.vs_all_from_<target_folder>.missing.txt
- After each pass (A->B or B->A) writes:
      summary_<PASS>.counts.txt   -> per-source counts + total (human-readable)
      total_<PASS>.count.txt      -> single integer (overall total) only

How to use:
  1) Edit the three variables in the "COPY YOUR PATHS / MODE HERE" section:
       - folderA, folderB, output_dir
       - MODE: 'A_to_B', 'B_to_A', or 'BOTH'
  2) Run:
       python compare_folders_missing_with_counts.py
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
    Return a list of tuples (orig_ln, line) describing missing source lines (preserving orig ln).
    """
    missing_entries = []
    for orig_ln, line in source_lines_with_ln:
        if line not in target_line_set:
            missing_entries.append((orig_ln, line))
    return missing_entries

def compare_source_to_targets_and_write(source_path: Path, target_paths: list, output_dir: Path, source_folder_name: str, target_folder_name: str):
    """
    Compare one source file to each target file and write ONLY missing lines
    into a dedicated output file. Return the total missing count (sum across targets).
    """
    out_filename = f"{source_path.name}.vs_all_from_{target_folder_name}.missing.txt"
    out_path = output_dir / out_filename

    # Read source lines once
    source_lines = read_source_non_empty_lines_with_lineno(source_path)
    if not source_lines:
        with out_path.open('w', encoding='utf-8') as out:
            out.write(f"# Source file: {source_path}\n")
            out.write(f"# Target folder: {target_folder_name} ({len(target_paths)} files)\n")
            out.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            out.write("# NOTE: Source file has no non-empty lines (after trimming). No missing lines.\n")
        return 0  # zero missing entries

    total_missing_for_source = 0

    with out_path.open('w', encoding='utf-8') as out:
        out.write("# Missing-lines report\n")
        out.write(f"# Source file: {source_path}\n")
        out.write(f"# Target folder: {target_folder_name} ({len(target_paths)} files)\n")
        out.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

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
                # Write each missing occurrence with original source line number
                for orig_ln, line in missing:
                    out.write(f"src_ln={orig_ln}: {line}\n")
                out.write("\n")
                total_missing_for_source += len(missing)

        if total_missing_for_source == 0:
            out.write("# Overall: No missing lines found for this source file against all targets.\n")

    return total_missing_for_source

def gather_text_files(folder: Path):
    """Return sorted list of .txt files (regular files) in folder."""
    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Folder does not exist or is not a directory: {folder}")
    files = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == '.txt'])
    return files

def run_pass_and_collect_counts(source_folder: Path, target_folder: Path, output_dir: Path, threads: int):
    """
    For each file in source_folder, compare it with all files in target_folder and
    write missing-only outputs. Returns a dict: { source_filename: missing_count }.
    """
    source_files = gather_text_files(source_folder)
    target_files = gather_text_files(target_folder)

    if not source_files:
        print(f"No .txt files found in source folder: {source_folder}", file=sys.stderr)
        return {}
    if not target_files:
        print(f"No .txt files found in target folder: {target_folder}", file=sys.stderr)
        return {}

    source_folder_name = source_folder.name or "source"
    target_folder_name = target_folder.name or "target"

    # Submit tasks and map futures to source file names
    future_to_source = {}
    results = {}

    with ThreadPoolExecutor(max_workers=threads) as ex:
        for src in source_files:
            fut = ex.submit(compare_source_to_targets_and_write, src, target_files, output_dir, source_folder_name, target_folder_name)
            future_to_source[fut] = src.name

        for fut in as_completed(future_to_source):
            src_name = future_to_source[fut]
            try:
                missing_count = fut.result()
            except Exception as e:
                print(f"Error while processing source {src_name}: {e}", file=sys.stderr)
                missing_count = 0
            results[src_name] = missing_count

    return results  # map of source filename -> missing count

def write_summary_files(output_dir: Path, pass_tag: str, counts_map: dict):
    """
    Write two files:
      - summary_{pass_tag}.counts.txt  (human-readable list + total)
      - total_{pass_tag}.count.txt     (single integer = overall total)
    pass_tag: short tag like "A_to_B" or "B_to_A"
    counts_map: { source_filename: missing_count }
    """
    summary_path = output_dir / f"summary_{pass_tag}.counts.txt"
    total_path = output_dir / f"total_{pass_tag}.count.txt"

    total_all = sum(counts_map.values())

    # Write human-readable summary
    with summary_path.open('w', encoding='utf-8') as s:
        s.write(f"# Summary of missing-line counts for pass: {pass_tag}\n")
        s.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        if not counts_map:
            s.write("# No processed source files (empty or error encountered)\n")
        else:
            for src in sorted(counts_map.keys()):
                s.write(f"{src}: {counts_map[src]}\n")
            s.write("\n")
            s.write(f"TOTAL: {total_all}\n")

    # Write the single-integer total file (only the count)
    with total_path.open('w', encoding='utf-8') as t:
        t.write(str(total_all))

def main():
    mode_val = (MODE or "BOTH").strip().upper()
    if mode_val not in ("A_TO_B", "B_TO_A", "BOTH"):
        print("MODE must be one of 'A_to_B', 'B_to_A', or 'BOTH'.", file=sys.stderr)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    if mode_val in ("A_TO_B", "BOTH"):
        print(f"Running A -> all B missing-lines pass: {folderA} -> {folderB}")
        try:
            counts_A_to_B = run_pass_and_collect_counts(folderA, folderB, output_dir, NUM_WORKER_THREADS)
            write_summary_files(output_dir, "A_to_B", counts_A_to_B)
            print(f"Summary files for A->B written to {output_dir} (summary_A_to_B.counts.txt and total_A_to_B.count.txt)")
        except Exception as e:
            print("Error during A->B pass:", e, file=sys.stderr)

    if mode_val in ("B_TO_A", "BOTH"):
        print(f"Running B -> all A missing-lines pass: {folderB} -> {folderA}")
        try:
            counts_B_to_A = run_pass_and_collect_counts(folderB, folderA, output_dir, NUM_WORKER_THREADS)
            write_summary_files(output_dir, "B_to_A", counts_B_to_A)
            print(f"Summary files for B->A written to {output_dir} (summary_B_to_A.counts.txt and total_B_to_A.count.txt)")
        except Exception as e:
            print("Error during B->A pass:", e, file=sys.stderr)

    print(f"\nDone. Outputs (missing-lines per source) and summaries are in: {output_dir}")
    try:
        for p in sorted(output_dir.iterdir()):
            if p.is_file():
                print(" -", p.name)
    except Exception:
        pass

if __name__ == "__main__":
    main()
