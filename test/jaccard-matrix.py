#! /usr/bin/env python

import sourmash
import sourmash_utils
import itertools
import argparse
import sys
import os
import polars as pl

def compute_jaccard(sig1, sig2):
    """Compute Jaccard similarity between two signatures (MinHash sketches)."""
    mh1 = sig1.minhash
    mh2 = sig2.minhash
    return mh1.similarity(mh2)

def main(args):
    select_mh = sourmash_utils.create_minhash_from_args(args)

    print(f"Loading sketches from: {args.sketch1}")
    db1 = sourmash_utils.load_index_and_select(args.sketch1, select_mh)
    sigs1_gen = db1.signatures()  # streamed

    if args.sketch1 == args.sketch2:
        sigs2_list = list(db1.signatures())
    else:
        print(f"Loading sketches from: {args.sketch2}")
        db2 = sourmash_utils.load_index_and_select(args.sketch2, select_mh)
        sigs2_list = list(db2.signatures())

    col_names = [f'"{sig.name}"' for sig in sigs2_list]

    if os.path.exists(args.output):
        os.remove(args.output)

    print(f"Computing and writing to '{args.output}'...")

    # Write header row manually
    with open(args.output, 'w') as f:
        f.write("," + ",".join(col_names) + "\n")

    for sig1 in sigs1_gen:  # streamed rows
        values = [compute_jaccard(sig1, sig2) for sig2 in sigs2_list]
        df_row = pl.DataFrame([[f'"{sig1.name}"'] + values], schema=["sketch"] + col_names, orient='row')
    
        # Convert to CSV string (one row)
        csv_string = df_row.write_csv(separator=",", include_header=False)
    
        # Append to file manually
        with open(args.output, "a") as f:
            f.write(csv_string)

    print("Saved Jaccard similarity matrix to 'jaccard_matrix.csv'")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Compute pairwise Jaccard similarity from a Sourmash sketch collection.")
    p.add_argument("sketch1", help="Path to sketch files.")
    p.add_argument("sketch2", help="Path to sketch files.")
    p.add_argument("-o","--output", help="Path to output file.")
    sourmash_utils.add_standard_minhash_args(p)
    args = p.parse_args()

    main(args)

