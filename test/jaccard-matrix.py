#! /usr/bin/env python

import sourmash
import sourmash_utils
import itertools
import argparse
import sys

def compute_jaccard(sig1, sig2):
    """Compute Jaccard similarity between two signatures (MinHash sketches)."""
    mh1 = sig1.minhash
    mh2 = sig2.minhash
    return mh1.similarity(mh2)

def main(sigfile):
    select_mh = sourmash_utils.create_minhash_from_args(args)
    print(f"loading sketches from file '{args.sigfile}'")
    db = sourmash_utils.load_index_and_select(args.sigfile, select_mh)

    print(f"'{args.sigfile}' contains {len(db)} signatures")


    if not db:
        sys.exit("No signatures found!")

    # Compute pairwise Jaccard similarities
    print("Computing Jaccard similarities...\n")
    print("," + ",".join(f"sig{i}" for i in range(len(db))))

    for n, ss1 in enumerate(db.signatures()):
        row = [f"sig{n}"]
        for j, ss2 in enumerate(db.signatures()):
            similarity = compute_jaccard(ss1, ss2)
            row.append(f"{similarity:.4f}")
        print(",".join(row))

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Compute pairwise Jaccard similarity from a Sourmash sketch collection.")
    p.add_argument("sigfile", help="Path to .sig file, or a directory containing multiple .sig files.")
    sourmash_utils.add_standard_minhash_args(p)
    args = p.parse_args()

    main(args.sigfile)

