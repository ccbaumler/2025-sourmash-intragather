#! /usr/bin/env python

import sourmash
import sourmash_utils
import argparse
import sys
import os
from multiprocessing import Pool, cpu_count
import time
from datetime import timedelta
import itertools

import polars as pl
from tqdm import tqdm

def compute_intersect(sig1, sig2):
    """Compute intersection between two signatures."""
    mh1 = sig1.minhash
    mh2 = sig2.minhash
    common, usize = mh1.intersection_and_union_size(mh2)
    return common

def compute_row(task):
    sig1, sigs2_list = task
    row = [sig1.name] + [compute_intersect(sig1, sig2) for sig2 in sigs2_list]
    return row

def compute_sparse_row(task):
    sig1, sigs2_list = task
    entries = []

    for sig2 in sigs2_list:
        common = compute_intersect(sig1, sig2)
        if common > 0:
            entries.append((sig1.name, sig2.name, common))

    return entries  # return list of non-zero entries for this row

def chunk_signatures(sig_iter, chunk_size):
    """Yield successive chunks from a signature iterator."""
    while True:
        chunk = list(itertools.islice(sig_iter, chunk_size))
        if not chunk:
            break
        yield chunk

def main(args):
    select_mh = sourmash_utils.create_minhash_from_args(args)

    print(f"Loading sketches from: {args.sketch1}")
    db1 = sourmash_utils.load_index_and_select(args.sketch1, select_mh)
    sigs1_gen = db1.signatures()

    if args.sketch1 == args.sketch2:
        sigs2_list = list(db1.signatures())
    else:
        print(f"Loading sketches from: {args.sketch2}")
        db2 = sourmash_utils.load_index_and_select(args.sketch2, select_mh)
        sigs2_list = list(db2.signatures())

    if os.path.exists(args.output):
        os.remove(args.output)

    if args.sparse:
        with open(args.output, 'w') as fp:
            fp.write(f"{os.path.basename(args.sketch1)},{os.path.basename(args.sketch2)},intersect_bp\n")
    elif args.dense:
        col_names = [f'"{sig.name}"' for sig in sigs2_list]
        with open(args.output, "w") as f:
            f.write("," + ",".join(col_names) + "\n")
    else:
        raise ValueError("Must choose either --dense or --sparse")

    print(f"Computing and writing to '{args.output}'...")
    cpus = cpu_count() if not args.cpus else args.cpus
    print(f"    Using {cpus} cpus in parallel")
    
    with Pool(processes=cpu_count()) as pool:
        counter = 0
        start_time = time.time()
        buffer = []
        BUFFER_FRACTION = args.fraction
        CHUNK_SIZE = args.chunk_size or max(1, int(len(db1) * BUFFER_FRACTION))
        sig_iter = db1.signatures()  # fresh generator

        modes = {
            "dense": {
                "schema": lambda: ["sketch"] + col_names,
                "func": compute_row,
                "buffer": lambda r: buffer.append(r),
            },
            "sparse": {
                "schema": lambda: ["sketch1", "sketch2", "intersect_bp"],
                "func": compute_sparse_row,
                "buffer": lambda r: buffer.extend(r),
            }
        }
        
        # Determine mode
        mode = "dense" if args.dense else "sparse" if args.sparse else None
        if mode is None:
            raise ValueError("Must choose either --dense or --sparse")
        
        schema = modes[mode]["schema"]()
        compute_func = modes[mode]["func"]
        get_buffer = modes[mode]["buffer"]

        for chunk in chunk_signatures(sig_iter, CHUNK_SIZE):
            print(f"\nProcessing chunk of size {len(chunk)}...\n", flush=True)
        
            tasks = ((sig1, sigs2_list) for sig1 in chunk)
        
            for row in pool.imap(compute_func, tasks):
                get_buffer(row)
        
                counter += 1
        
                if counter % CHUNK_SIZE == 0:
                    elapsed = time.time() - start_time
                    avg_time_per_row = elapsed / counter
                    estimated_total = avg_time_per_row * len(db1)
                    eta = estimated_total - elapsed
        
                    print(f"[{time.strftime('%H:%M:%S')}] Processed {counter} sketches "
                          f"({row[0]}), "
                          f"Elapsed: {timedelta(seconds=int(elapsed))}, "
                          f"ETA: {timedelta(seconds=int(eta))}", flush=True)
        
                if len(buffer) >= CHUNK_SIZE:
                    df_chunk = pl.DataFrame(buffer, schema=schema, orient="row")
                    with open(args.output, "a") as f:
                        f.write(df_chunk.write_csv(separator=",", include_header=False))
                    buffer.clear()
       
        if buffer:
            df_chunk = pl.DataFrame(buffer, schema=schema, orient="row")
            with open(args.output, "a") as f:
                f.write(df_chunk.write_csv(separator=",", include_header=False))

    print("Saved Jaccard similarity matrix to 'jaccard_matrix.csv'")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Compute pairwise Jaccard similarity from a Sourmash sketch collection.")
    p.add_argument("sketch1", help="Path to sketch files.")
    p.add_argument("sketch2", help="Path to sketch files.")
    p.add_argument("-o","--output", help="Path to output file.")
    p.add_argument('-c','--cpus', help='cpu to use for parallel jobs')
    p.add_argument('-f','--fraction', default=1, type=float, help='The fraction of the sketch1 to process at a time.')
    p.add_argument('--chunk-size', type=int, default=None,
              help='Number of sketch1 signatures to process per chunk')

    g = p.add_mutually_exclusive_group(required=True)

    g.add_argument('--sparse', action='store_true', help='Sparse output file')
    g.add_argument('--dense', action='store_true', help='Dense output file.')

    sourmash_utils.add_standard_minhash_args(p)
    args = p.parse_args()

    main(args)

