#!/usr/bin/env python3
"""
compare_folders.py

How to use (quick):
  1) Edit the three variables below in the "COPY YOUR PATHS / MODE HERE" section:
       - folderA: Path to folder A
       - folderB: Path to folder B
       - output_dir: Where diff output files will be written
       - MODE: 'A_to_B', 'B_to_A', or 'BOTH' (case-insensitive)

  2) Run:
       python compare_folders.py

Behavior:
  - Streams files; does NOT load entire files into memory.
  - Compares non-empty lines only (lines that are empty after .strip() are ignored).
  - Sequence-wise comparison: compares non-empty line #1 vs non-empty line #1, etc.
  - Writes one output file per source file. Output filename contains the direction and
    the "target folder name" for clarity.
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

def non_empty_lines_with_orig_lineno(file_path: Path):
    """
    Generator yielding (original_line_number, line_text) for non-empty lines.
    Lines that are empty after stripping whitespace are skipped.
    """
    with file_path.open('r', encoding='utf-8', errors='replace') as f:
        for i, raw in enumerate(f, start=1):
            line = raw.rstrip('\n\r')
            if line.strip() == '':
                continue
            yield (i, line)

def compare_two_streams(a_path: Path, b_path: Path):
    """
    Compare non-empty lines from a_path and b_path in sequence.
    Yields human-readable diff strings (one per mismatch/extra line).
    """
    gen_a = non_empty_lines_with_orig_lineno(a_path)
    gen_b = non_empty_lines_with_orig_lineno(b_path)

    a_iter = iter(gen_a)
    b_iter = iter(gen_b)
    a_next = b_next = None
    seq_idx = 1

    while True:
        if a_next is None:
            try:
                a_next = next(a_iter)
            except StopIteration:
                a_next = None
        if b_next is None:
            try:
                b_next = next(b_iter)
            except StopIteration:
                b_next = None

        if a_next is None and b_next is None:
            break  # both done

        if a_next is not None and b_next is not None:
            a_ln, a_line = a_next
            b_ln, b_line = b_next
            if a_line != b_line:
                yield (f"SEQ#{seq_idx}: A({a_path.name}) [orig_ln={a_ln}]: {a_line!r}\n"
                       f"          vs B({b_path.name}) [orig_ln={b_ln}]: {b_line!r}")
        elif a_next is not None and b_next is None:
            a_ln, a_line = a_next
            yield (f"SEQ#{seq_idx}: A({a_path.name}) [orig_ln={a_ln}] EXTRA LINE: {a_line!r}\n"
                   f"          B({b_path.name}) has no corresponding non-empty line (exhausted).")
        elif a_next is None and b_next is not None:
            b_ln, b_line = b_next
            yield (f"SEQ#{seq_idx}: B({b_path.name}) [orig_ln={b_ln}] EXTRA LINE: {b_line!r}\n"
                   f"          A({a_path.name}) has no corresponding non-empty line (exhausted).")

        a_next = None
        b_next = None
        seq_idx += 1

def compare_source_to_targets(source_path: Path, target_files: list, output_dir: Path, source_folder_name: str, target_folder_name: str):
    """
    Compare one source_path to every file in target_files.
    Writes results to output_dir with a descriptive filename.
    """
    # Output filename indicates direction and target folder name for clarity.
    safe_source_name = source_path.name
    out_filename = f"{safe_source_name}.vs_all_from_{target_folder_name}.diff.txt"
    out_path = output_dir / out_filename

    with out_path.open('w', encoding='utf-8') as out:
        out.write("# Comparison results\n")
        out.write(f"# Source file: {source_path}\n")
        out.write(f"# Target folder: {target_folder_name} - {len(target_files)} files\n")
        out.write(f"# Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        any_diffs = False
        for tgt in target_files:
            out.write("=" * 80 + "\n")
            out.write(f"Comparing SOURCE: {source_path.name}  <--->  TARGET: {tgt.name}\n")
            out.write("-" * 80 + "\n")
            diffs_found = False
            for diff_line in compare_two_streams(source_path, tgt):
                out.write(diff_line + "\n")
                diffs_found = True
                any_diffs = True
            if not diffs_found:
                out.write("(no differences found — non-empty lines match in sequence)\n")
            out.write("\n")

        if not any_diffs:
            out.write("# Overall: No differences found for this source file against all targets.\n")

def gather_text_files(folder: Path):
    """Return sorted list of .txt files (regular files) in folder."""
    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Folder does not exist or is not a directory: {folder}")
    files = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == '.txt'])
    return files

def run_pass(source_folder: Path, target_folder: Path, output_dir: Path, threads: int):
    """
    Run: for each file in source_folder, compare it with all files in target_folder.
    Uses threads to parallelize per-source-file comparisons.
    """
    source_files = gather_text_files(source_folder)
    target_files = gather_text_files(target_folder)

    if not source_files:
        print(f"No .txt files found in source folder: {source_folder}", file=sys.stderr)
        return
    if not target_files:
        print(f"No .txt files found in target folder: {target_folder}", file=sys.stderr)
        return

    # Use folder name (or fallback to 'folder') in output filenames for clarity
    source_folder_name = source_folder.name or "source"
    target_folder_name = target_folder.name or "target"

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = []
        for src in source_files:
            futures.append(ex.submit(compare_source_to_targets, src, target_files, output_dir, source_folder_name, target_folder_name))
        for f in as_completed(futures):
            # raise if exception occurred in any worker
            f.result()

def main():
    # Validate MODE value
    mode_val = (MODE or "BOTH").strip().upper()
    if mode_val not in ("A_TO_B", "B_TO_A", "BOTH"):
        print("MODE must be one of 'A_to_B', 'B_to_A', or 'BOTH'.", file=sys.stderr)
        return

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run passes according to mode
    if mode_val in ("A_TO_B", "BOTH"):
        print(f"Running A -> all B comparisons: {folderA} -> {folderB}")
        try:
            run_pass(folderA, folderB, output_dir, NUM_WORKER_THREADS)
        except Exception as e:
            print("Error during A->B pass:", e, file=sys.stderr)

    if mode_val in ("B_TO_A", "BOTH"):
        print(f"Running B -> all A comparisons: {folderB} -> {folderA}")
        try:
            run_pass(folderB, folderA, output_dir, NUM_WORKER_THREADS)
        except Exception as e:
            print("Error during B->A pass:", e, file=sys.stderr)

    print(f"\nDone. Diff outputs are in: {output_dir}")
    # List generated files
    try:
        for p in sorted(output_dir.iterdir()):
            if p.is_file():
                print(" -", p.name)
    except Exception:
        pass

if __name__ == "__main__":
    main()
