#! /usr/bin/env python

import sourmash
from sourmash_plugin_branchwater import sourmash_plugin_branchwater as branch
import zipfile
import gzip
import sourmash
import tempfile
import os
import io
import argparse
import re
import csv

def clean_sig_name(name, ksize, scaled, threshold_bp, moltype, max_length=80,):
    cleaned = re.sub(r"[ ,/]+", "_", name)
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "", cleaned)
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rstrip("_-.")
    cleaned += f".m{moltype}_k{ksize}_s{scaled}_th{threshold_bp}" 
    if not cleaned.endswith(".csv"):
        cleaned += ".csv"
    return cleaned

def main(args):
    print(f"Loading sketches from: {args.db}")
    db2 = sourmash.load_file_as_index(args.db)
    db2 = db2.select(moltype=args.moltype, ksize=args.ksize, scaled=args.scaled)
    print(f"Loaded {len(db2)} sketches from {args.db}")

    summary_dict = []
    zip_path = args.query
    with zipfile.ZipFile(zip_path, 'r') as zf:
        sig_files = [f for f in zf.namelist() if f.endswith(".sig.gz")]
        print(f"Found {len(sig_files)} .sig.gz files in the zip.")

        for sig_name in sig_files:
            print(f"Processing: {sig_name}")

            with zf.open(sig_name) as sig_file_compressed:
                decompressed_data = gzip.decompress(sig_file_compressed.read())
                
                sig = sourmash.load_one_signature(io.StringIO(decompressed_data.decode('utf-8')))
                print("Signature name:", sig.name)
                filename = clean_sig_name(
                    name=sig.name,
                    ksize=args.ksize,
                    scaled=args.scaled,
                    threshold_bp=0,
                    moltype=args.moltype
                )
                gather_path = os.path.join(args.gather_output, filename)
                prefetch_path = os.path.join(args.prefetch_output, filename)
                
                with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=".sig.gz") as tmp:
                    tmp.write(gzip.compress(decompressed_data))
                    tmp_path = tmp.name

            try:
                branch.do_fastgather(
                    tmp_path,
                    args.db,
                    0,
                    args.ksize,
                    args.scaled,
                    args.moltype,
                    gather_path,
                    prefetch_path,
                )
            finally:
                os.remove(tmp_path)

            gather_lines = 0
            prefetch_lines = 0
            if os.path.exists(gather_path):
                with open(gather_path, 'r') as f:
                    gather_lines = sum(1 for line in f) - 1
            if os.path.exists(prefetch_path):
                with open(prefetch_path, 'r') as f:
                    prefetch_lines = sum(1 for line in f) - 1

            summary_dict.append({
                'sig_name': sig.name,
                'gather_file': gather_path,
                'gather_match_count': gather_lines,
                'prefetch_file': prefetch_path,
                'prefetch_match_count': prefetch_lines,
            })

    with open(args.summary_output, 'w', newline='') as csvfile:
        fieldnames = ['sig_name', 'gather_file', 'gather_match_count', 'prefetch_file', 'prefetch_match_count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_dict)

    print(f"\nWrote summary to {args.summary_output}")
   
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Compute pairwise Jaccard similarity from a Sourmash sketch collection.")
    p.add_argument("query", help="Path to query sketch files.")
    p.add_argument("db", help="Path to db sketch files.")
    p.add_argument("--gather-output", help="Path to output file.")
    p.add_argument("--prefetch-output", help="Path to output file.")
    p.add_argument("--summary-output", help="Path to output file.")
    p.add_argument("--moltype", default="DNA", type=str, help="Path to output file.")
    p.add_argument("--ksize", default=31, type=int, help="Path to output file.")
    p.add_argument("--scaled", default=1000, type=int, help="Path to output file.")

    args = p.parse_args()

    main(args)

