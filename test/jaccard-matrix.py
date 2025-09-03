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

def main(args):
    select_mh = sourmash_utils.create_minhash_from_args(args)

    if args.sketch1 == args.sketch2:
        print(f"loading sketches from file '{args.sketch1}'")
        db = sourmash_utils.load_index_and_select(args.sketch1, select_mh)
        print(f"'{args.sketch1}' contains {len(db)} signatures")
    
        if not db:
            sys.exit("No signatures found!")
    
        # Compute pairwise Jaccard similarities
        print("Computing Jaccard similarities...\n")
        print("\t" + "\t".join(f"{i.name}" for i in db.signatures()))
    
        for n, ss1 in enumerate(db.signatures()):
            row = [f"{ss1.name}"]
            for j, ss2 in enumerate(db.signatures()):
                similarity = compute_jaccard(ss1, ss2)
                row.append(f"{similarity:.4f}")
            print("\t".join(row))
    else:
        print(f"loading sketches from file '{args.sketch1}'")
        db = sourmash_utils.load_index_and_select(args.sketch1, select_mh)
        print(f"'{args.sketch1}' contains {len(db)} signatures")
        print(f"loading sketches from file '{args.sketch2}'")
        db2 = sourmash_utils.load_index_and_select(args.sketch2, select_mh)
        print(f"'{args.sketch2}' contains {len(db)} signatures")
    
        if not db or not db2:
            sys.exit("No signatures found!")
    
        # Compute pairwise Jaccard similarities
        print("Computing Jaccard similarities...\n")
        print("\t" + "\t".join(f"{i.name}" for i in db.signatures()))
    
        for n, ss1 in enumerate(db.signatures()):
            row = [f"{ss1.name}"]
            for j, ss2 in enumerate(db2.signatures()):
                similarity = compute_jaccard(ss1, ss2)
                row.append(f"{similarity:.4f}")
            print("\t".join(row))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Compute pairwise Jaccard similarity from a Sourmash sketch collection.")
    p.add_argument("sketch1", help="Path to sketch files.")
    p.add_argument("sketch2", help="Path to sketch files.")
    sourmash_utils.add_standard_minhash_args(p)
    args = p.parse_args()

    main(args)

