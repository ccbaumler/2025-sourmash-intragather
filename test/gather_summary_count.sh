#! /bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 <input_csv_file> <gather_threshold> <prefetch_threshold>"
    exit 1
fi

echo

input_csv="$1"
gather_th="$2"
prefetch_th="$3"

echo "Here is the prefetch threshold $gather_th"
echo "Here is the prefetch threshold $prefetch_th"

if [ ! -f "$input_csv" ]; then
    echo "Error: File '$input_csv' not found"
    exit 1
fi

# SRA metadata can have additional commas included between '()'
# replace the comma with an underscore
header=$(awk 'NR==1 {
    while (match($0, /\([^()]*,[^()]*\)/)) {
        s = substr($0, RSTART, RLENGTH);
        gsub(",", "_", s);
        $0 = substr($0, 1, RSTART - 1) s substr($0, RSTART + RLENGTH);
    }
    print;
    next
} 1' $input_csv)

IFS=',' read -ra columns <<< "$header"

num_cols=${#columns[@]}
echo "Found '$num_cols' columns in '$input_csv' file"

#https://stackoverflow.com/a/31889595
FPAT='([^,]*)|("[^"]*")'

for ((i=1; i<=num_cols; i++)); do
    col_name="${columns[i-1]}"
    col_name=$(echo "${columns[i-1]}" | tr -d '"' | tr -d '\r\n')

    if [[ "$i" -eq 3 ]]; then
        unique_count=$(awk -v FPAT="$FPAT" -v col="$i" -v threshold="$gather_th" 'NR > 1 && $col > threshold && ($col != "") {print $col}' "$input_csv" | sort | wc -l)
        total_count=$(awk -v FPAT="$FPAT" -v col="$i" 'NR > 1 {if ($col != "") print $col}' "$input_csv" | wc -l)
        echo "=== $unique_count Unique values (of $total_count) in column $i: $col_name ==="

        echo "-- Rows where column '$col_name' > $gather_th --"
        awk -v FPAT="$FPAT" -v col="$i" -v threshold="$gather_th" '
            NR > 1 && $col > threshold {
                printf "    %s, %s\n", $col, $(col-2)
            }
        ' "$input_csv" | sort -nr
    fi
    if [[ "$i" -eq 5 ]]; then
        unique_count=$(awk -v FPAT="$FPAT" -v col="$i" -v threshold="$prefetch_th" 'NR > 1 && $col > threshold {if ($col != "") print $col}' "$input_csv" | sort | wc -l)
        total_count=$(awk -v FPAT="$FPAT" -v col="$i" 'NR > 1 {if ($col != "") print $col}' "$input_csv" | wc -l)
        echo "=== $unique_count Unique values (of $total_count) in column $i: $col_name ==="
    
        printf -- "-- Rows where column \"%s\" > %s --\n" "$col_name" "$prefetch_th"
        awk -v FPAT="$FPAT" -v col="$i" -v threshold="$prefetch_th" '
            NR > 1 && $col > threshold {
                val = $col
                name = $(col - 4)
        
                # Remove unwanted characters
                gsub(/["\r\n,]/, "", name)
                gsub(/["\r\n,]/, "", val)
        
                printf "    %s, %s\n", val, name
            }
        ' "$input_csv" | sort -nr
    fi
    echo
done
