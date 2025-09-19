#!/usr/bin/env python3
"""
compare_folders_missing_count.py

Purpose:
  For each source file (in folder A or folder B depending on MODE), compare it against
  every file in the target folder and write ONLY the lines that are present in the
  source but missing in the target. Empty lines are ignored.

  Additionally, after finishing comparisons:
    - write a per-source total-count file that contains only the total missing count
      (a single integer).
    - write an overall summary file listing total missing counts per source and a grand total.

How to use:
  1) Edit the three variables in the "COPY YOUR PATHS / MODE HERE" section:
       - folderA, folderB, output_dir
       - MODE: 'A_to_B', 'B_to_A', or 'BOTH'
  2) Run:
       python compare_folders_missing_count.py

Notes:
  - Each source produces:
      * <source>.vs_all_from_<target_folder>.missing.txt          (detailed per-target missing lines)
      * <source>.vs_all_from_<target_folder>.missing.totalcount.txt  (single integer: total missing occurrences)
  - At the end, a summary file is created:
      * all_sources_missing_counts.summary.txt
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
    """Return a list of tuples (orig_line_number, line_text) for non-empty lines in the file."""
    result = []
    with path.open('r', encoding='utf-8', errors='replace') as f:
        for i, raw in enumerate(f, start=1):
            line = raw.rstrip('\n\r')
            if line.strip() == '':
                continue
            result.append((i, line))
    return result

def build_target_line_set(path: Path):
    """Return a set of non-empty lines present in the target file."""
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
    Return a list of tuples (orig_ln, line) describing missing source lines.
    """
    missing_entries = []
    for orig_ln, line in source_lines_with_ln:
        if line not in target_line_set:
            missing_entries.append((orig_ln, line))
    return missing_entries

def compare_source_to_targets_and_write(source_path: Path, target_paths: list, output_dir: Path, source_folder_name: str, target_folder_name: str):
    """
    Compare one source file to each target file and write:
      - detailed missing-lines file
      - per-source total-count file (single integer)
    Returns a dict with counts for aggregation:
      { 'source': source_name, 'target_counts': {tgt_name: count}, 'total_missing': total }
    """
    out_filename = f"{source_path.name}.vs_all_from_{target_folder_name}.missing.txt"
    out_path = output_dir / out_filename
    total_missing = 0
    per_target_counts = {}

    # Read source lines once
    source_lines = read_source_non_empty_lines_with_lineno(source_path)

    # Write detailed missing-lines file
    with out_path.open('w', encoding='utf-8') as out:
        out.write("# Missing-lines report\n")
        out.write(f"# Source file: {source_path}\n")
        out.write(f"# Target folder: {target_folder_name} ({len(target_paths)} files)\n")
        out.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        if not source_lines:
            out.write("# NOTE: Source file has no non-empty lines (after trimming). No missing lines.\n")
            # Write a per-source zero count below (after closing this file)
            per_target_counts = {t.name: 0 for t in target_paths}
            total_missing = 0
        else:
            overall_any_missing = False
            for tgt in target_paths:
                out.write("=" * 80 + "\n")
                out.write(f"Target file: {tgt.name}\n")
                out.write("-" * 80 + "\n")
                try:
                    tgt_set = build_target_line_set(tgt)
                except Exception as e:
                    out.write(f"# ERROR reading target file {tgt}: {e}\n\n")
                    per_target_counts[tgt.name] = 0
                    continue

                missing = compare_source_to_target_missing(source_lines, tgt_set)
                per_target_counts[tgt.name] = len(missing)
                total_missing += len(missing)

                if not missing:
                    out.write("(no missing lines — every non-empty line of source is present in this target)\n\n")
                else:
                    overall_any_missing = True
                    for orig_ln, line in missing:
                        out.write(f"src_ln={orig_ln}: {line}\n")
                    out.write("\n")

            if not overall_any_missing:
                out.write("# Overall: No missing lines found for this source file against all targets.\n")

    # Write per-source total-count file (single integer, only the count)
    totalcount_filename = f"{source_path.name}.vs_all_from_{target_folder_name}.missing.totalcount.txt"
    totalcount_path = output_dir / totalcount_filename
    with totalcount_path.open('w', encoding='utf-8') as tc:
        tc.write(str(total_missing) + "\n")

    # Also write a small per-source-per-target counts file for convenience
    counts_detail_filename = f"{source_path.name}.vs_all_from_{target_folder_name}.missing.counts.txt"
    counts_detail_path = output_dir / counts_detail_filename
    with counts_detail_path.open('w', encoding='utf-8') as cd:
        cd.write(f"# Counts for source: {source_path.name}\n")
        cd.write(f"# Target folder: {target_folder_name}\n")
        cd.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        for tgt_name in sorted(per_target_counts.keys()):
            cd.write(f"{tgt_name}: {per_target_counts[tgt_name]}\n")
        cd.write("\n")
        cd.write(f"TOTAL: {total_missing}\n")

    return {
        'source': source_path.name,
        'target_counts': per_target_counts,
        'total_missing': total_missing
    }

def gather_text_files(folder: Path):
    """Return sorted list of .txt files (regular files) in folder."""
    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Folder does not exist or is not a directory: {folder}")
    files = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == '.txt'])
    return files

def run_pass(source_folder: Path, target_folder: Path, output_dir: Path, threads: int):
    """
    For each file in source_folder, compare it with all files in target_folder and
    write missing-only outputs and count files.

    Returns list of result-dicts (one per source) for aggregation.
    """
    source_files = gather_text_files(source_folder)
    target_files = gather_text_files(target_folder)

    if not source_files:
        print(f"No .txt files found in source folder: {source_folder}", file=sys.stderr)
        return []
    if not target_files:
        print(f"No .txt files found in target folder: {target_folder}", file=sys.stderr)
        return []

    source_folder_name = source_folder.name or "source"
    target_folder_name = target_folder.name or "target"

    results = []
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = []
        for src in source_files:
            futures.append(ex.submit(compare_source_to_targets_and_write, src, target_files, output_dir, source_folder_name, target_folder_name))
        for f in as_completed(futures):
            try:
                res = f.result()
                if res:
                    results.append(res)
            except Exception as e:
                # Log and continue
                print(f"Error comparing a source file: {e}", file=sys.stderr)
    return results

def write_overall_summary(all_results: list, output_dir: Path, pass_label: str):
    """
    Write an overall summary file that lists per-source total missing counts and a grand total.
    pass_label helps distinguish A->B vs B->A passes in filenames.
    """
    summary_filename = f"all_sources_missing_counts.{pass_label}.summary.txt"
    summary_path = output_dir / summary_filename

    grand_total = 0
    with summary_path.open('w', encoding='utf-8') as s:
        s.write(f"# Missing counts summary - pass: {pass_label}\n")
        s.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        if not all_results:
            s.write("# No results (no source or target files?)\n")
            return
        s.write("SOURCE_FILE,TOTAL_MISSING\n")
        for res in sorted(all_results, key=lambda r: r['source']):
            s.write(f"{res['source']},{res['total_missing']}\n")
            grand_total += int(res['total_missing'] or 0)
        s.write("\n")
        s.write(f"GRAND_TOTAL,{grand_total}\n")

    return summary_path

def main():
    mode_val = (MODE or "BOTH").strip().upper()
    if mode_val not in ("A_TO_B", "B_TO_A", "BOTH"):
        print("MODE must be one of 'A_to_B', 'B_to_A', or 'BOTH'.", file=sys.stderr)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    all_summary_paths = []

    if mode_val in ("A_TO_B", "BOTH"):
        print(f"Running A -> all B missing-lines pass: {folderA} -> {folderB}")
        try:
            results_A_to_B = run_pass(folderA, folderB, output_dir, NUM_WORKER_THREADS)
            summary_path = write_overall_summary(results_A_to_B, output_dir, pass_label="A_to_B")
            if summary_path:
                all_summary_paths.append(summary_path)
        except Exception as e:
            print("Error during A->B pass:", e, file=sys.stderr)

    if mode_val in ("B_TO_A", "BOTH"):
        print(f"Running B -> all A missing-lines pass: {folderB} -> {folderA}")
        try:
            results_B_to_A = run_pass(folderB, folderA, output_dir, NUM_WORKER_THREADS)
            summary_path = write_overall_summary(results_B_to_A, output_dir, pass_label="B_to_A")
            if summary_path:
                all_summary_paths.append(summary_path)
        except Exception as e:
            print("Error during B->A pass:", e, file=sys.stderr)

    # Combined master summary (if multiple passes)
    if len(all_summary_paths) > 1:
        combined_filename = "all_passes_missing_counts.master_summary.txt"
        combined_path = output_dir / combined_filename
        with combined_path.open('w', encoding='utf-8') as master:
            master.write(f"# Master summary across passes\n")
            master.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for sp in all_summary_paths:
                master.write(f"--- Summary file: {sp.name} ---\n")
                master.write(sp.read_text())
                master.write("\n\n")
        print(f"\nMaster summary written: {combined_path}")

    print(f"\nDone. Missing-lines outputs and count files are in: {output_dir}")
    try:
        for p in sorted(output_dir.iterdir()):
            if p.is_file():
                print(" -", p.name)
    except Exception:
        pass

if __name__ == "__main__":
    main()
