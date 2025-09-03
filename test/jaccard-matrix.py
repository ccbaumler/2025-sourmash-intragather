#! /usr/bin/env python

import sourmash
import sourmash_utils
import itertools
import argparse
import sys
import polars as pl

def compute_jaccard(sig1, sig2):
    """Compute Jaccard similarity between two signatures (MinHash sketches)."""
    mh1 = sig1.minhash
    mh2 = sig2.minhash
    return mh1.similarity(mh2)

def main(args):
    select_mh = sourmash_utils.create_minhash_from_args(args)

    print(f"loading sketches from file '{args.sketch1}'")
    db1 = sourmash_utils.load_index_and_select(args.sketch1, select_mh)
    sigs1 = list(db1.signatures())
    print(f"'{args.sketch1}' contains {len(sigs1)} signatures")

    if not sigs1:
        sys.exit("No signatures found in sketch1!")

    if args.sketch2 == args.sketch1:
        sigs2 = sigs1  # self-comparison
    else:
        print(f"loading sketches from file '{args.sketch2}'")
        db2 = sourmash_utils.load_index_and_select(args.sketch2, select_mh)
        sigs2 = list(db2.signatures())
        print(f"'{args.sketch2}' contains {len(sigs2)} signatures")

        if not sigs2:
            sys.exit("No signatures found in sketch2!")

    # Extract readable names
    row_names = [sig.name for sig in sigs1]
    col_names = [sig.name for sig in sigs2]
    print("Computing Jaccard similarities...")

    rows = []
    for sig1 in sigs1:
        row = [compute_jaccard(sig1, sig2) for sig2 in sigs2]
        rows.append(row)

    # Convert to Polars DataFrame
    df = pl.DataFrame(rows, schema=col_names)
    df = df.with_columns(pl.Series("sketch", row_names))
    df = df.select(["sketch"] + col_names)

    # Save to CSV
    df.write_csv("jaccard_matrix.csv")
    print("Saved Jaccard similarity matrix to 'jaccard_matrix.csv'")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Compute pairwise Jaccard similarity from a Sourmash sketch collection.")
    p.add_argument("sketch1", help="Path to sketch files.")
    p.add_argument("sketch2", help="Path to sketch files.")
    sourmash_utils.add_standard_minhash_args(p)
    args = p.parse_args()

    main(args)

