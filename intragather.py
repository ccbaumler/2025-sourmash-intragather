#! /usr/bin/env python

import os
import argparse
import sourmash
import sourmash_utils
from sourmash import sourmash_args
from sourmash import commands
from concurrent.futures import ProcessPoolExecutor, as_completed
import subprocess
import sys

def sanitize(name):
    return "".join(c if c.isalnum() or c in "-._" else "_" for c in name)

def work_generator(db, args):
    for n, ss in enumerate(db.signatures()):
        sig_name = ss.name
        #print(dir(ss))
        sanitized = sanitize(sig_name)
        out_csv = os.path.join(args.outdir, f"{sanitized}.gather.csv")
        out_csv_gz = out_csv + ".gz" if args.gzip else out_csv

        per_sig_kwargs = vars(args).copy()
        per_sig_kwargs.update({
            "query": ss,
            "databases": args.data,
            "output": out_csv,
            "quiet": False,
            "debug": False,
            "picklist": False,
            "include_db_pattern": False,
            "exclude_db_pattern": False,
            "md5": False, #ss.md5sum,
        })

        yield {
            "db": args.data,
            "sig_name": sig_name,
            "sig_md5": ss.md5sum,  
            "per_sig_kwargs": per_sig_kwargs,
            "out_csv": out_csv,
            "out_csv_gz": out_csv_gz,
            "outdir": args.outdir,
        }

def process_signature(args_dict):
    per_sig_kwargs = args_dict["per_sig_kwargs"]
    out_csv = args_dict["out_csv"]
    out_csv_gz = args_dict["out_csv_gz"]
    sig_name = args_dict["sig_name"]
    sig_md5 = args_dict["sig_md5"]
    data = args_dict["db"]
    outdir = args_dict["outdir"]

    if os.path.exists(out_csv) or os.path.exists(out_csv_gz):
        return f"Skipping {out_csv} or {out_csv_gz}: file already exists."
    
    # Load the database again (safe to do in subprocess)
    db = sourmash.load_file_as_index(data)
    print("Total signatures in DB:", sum(1 for _ in db.signatures()))
    print(_.name for _ in db.signatures())

    #print(dir(db))
    #help(db.select())
    # select one signature by md5 or name
    ss = next((s for s in db.signatures() if s.md5sum() == sig_md5), sys.exit())


    # Save the selected signature to a temporary file
    query_file = os.path.join(outdir, f"{sanitize(sig_name)}.query.sig")
    with open(query_file, "wt") as fp:
        sourmash.save_signatures(ss, fp)

    # Now call gather using the saved sig file
    cmd_args = {
        "query": ss, #query_file,
        "databases": [args.data],
        "output": out_csv,
    }
    commands.gather(argparse.Namespace(**cmd_args))
    #commands.gather(argparse.Namespace(**per_sig_kwargs))
    return f"Processed {sig_name}"

def main():
    p = argparse.ArgumentParser()

    p.add_argument("data", help="sourmash signature database zip file")
    p.add_argument("--outdir", default="", help="Directory for output CSV files")
    p.add_argument("--gzip", action="store_true")
    p.add_argument("--threads", type=int, default=1, help="Number of threads/processes for parallel processing")

    sourmash_utils.add_standard_minhash_args(p)

    args = p.parse_args()

    select_mh = sourmash_utils.create_minhash_from_args(args)

    os.makedirs(args.outdir, exist_ok=True)

    print(f"loading sketches from file '{args.data}'")
    db = sourmash_utils.load_index_and_select(args.data, select_mh)
    print(f"'{args.data}' contains {len(db)} signatures")


    # Parallel processing!!!
    if args.threads > 1:
        with ProcessPoolExecutor(max_workers=args.threads) as executor:
            work_iter = work_generator(db, args)
            futures = {executor.submit(process_signature, w): w["sig_name"] for w in work_iter}
            for future in as_completed(futures):
                print(future.result())
    # Single-threaded fallback?
    else:
        work = list(work_generator(db, args))
        for i, w in enumerate(work, 1):
            msg = process_signature(w)
            print(f"[{i}/{len(work)}] {msg}")

if __name__ == "__main__":
    main()
