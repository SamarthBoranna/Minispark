#!/bin/bash

# Clean up build artifacts
rm -f applications/*.o solution/*.o lib/*.o solution/*.a
rm -rf bin
make clean

# Check if partners.csv exists in the current directory.
if [ ! -f "partners.csv" ]; then
    echo "Submission could not be compressed: partners.csv not found. Even if you are working without a partner you must fill up partners.csv."
    exit 1
fi

FILE_PATH="partners.csv"

# Read the file line by line into an array.
raw_lines=()
while IFS= read -r line || [ -n "$line" ]; do
    raw_lines+=("$line")
done < "$FILE_PATH"

# Filter out any lines that are empty or consist only of whitespace.
lines=()
for line in "${raw_lines[@]}"; do
    trimmed=$(echo "$line" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
    if [ -n "$trimmed" ]; then
        lines+=("$trimmed")
    fi
done

# The file should have exactly 2 non-empty lines: a header and one data row.
if [ "${#lines[@]}" -ne 2 ]; then
    echo "Submission could not be compressed: partners.csv must contain exactly 2 non-empty lines (header and data row). Found ${#lines[@]} line(s)."
    exit 1
fi

# Function to trim whitespace from a string.
trim() {
    echo "$1" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
}

############################################
# Validate Header
############################################

header="${lines[0]}"
# Split header by comma.
IFS=',' read -r h1 h2 extra <<< "$header"
if [ -n "$extra" ]; then
    echo "Submission could not be compressed: Header has extra fields. Expected exactly 2 fields but found extra: '$extra'."
    exit 1
fi

# Trim each header token.
h1=$(trim "$h1")
h2=$(trim "$h2")

# The header tokens must exactly match the required values.
if [ "$h1" != "cslogin1" ] || [ "$h2" != "cslogin2" ]; then
    echo "Submission could not be compressed: Invalid header. Expected 'cslogin1,cslogin2' but got '$h1,$h2'."
    exit 1
fi

############################################
# Validate Data Row
############################################

data="${lines[1]}"
# Split data row based on whether a comma exists.
if echo "$data" | grep -q ','; then
    IFS=',' read -r d1 d2 extra <<< "$data"
    if [ -n "$extra" ]; then
        echo "Submission could not be compressed: Data row has extra fields. Expected exactly 2 fields but found extra: '$extra'."
        exit 1
    fi
else
    # No comma found; assume the entire row is cslogin1 and cslogin2 is empty.
    d1="$data"
    d2=""
fi

# Trim each data token.
d1=$(trim "$d1")
d2=$(trim "$d2")

# Function to check if a field is a single word (non-empty and without any whitespace)
is_single_word() {
    if [[ $1 =~ ^[^[:space:]]+$ ]]; then
        return 0
    else
        return 1
    fi
}

# Validate cslogin1 field.
if ! is_single_word "$d1"; then
    echo "Submission could not be compressed: The cslogin1 field must be a single non-empty word. Found: '$d1'."
    exit 1
fi

# Validate cslogin2 field (if non-empty).
if [ -n "$d2" ] && ! is_single_word "$d2"; then
    echo "Submission could not be compressed: The cslogin2 field must be a single word if not empty. Found: '$d2'."
    exit 1
fi

echo "partners.csv is valid"

############################################
# Check slipdays.txt and Create Archive
############################################

if [ -f "slipdays.txt" ]; then
    echo "slipdays.txt found. Including it in the archive."
    CONTENT=$(tr -d '[:space:]' < "slipdays.txt")
    if [[ ! "$CONTENT" =~ ^[0-9]$ ]]; then
        echo "Submission could not be compressed: Invalid slipdays.txt format. Expected a single digit (0-9) but found '$CONTENT'."
        exit 1
    fi
    tar -zcf p5.tar ./solution README.md Makefile slipdays.txt partners.csv
else
    echo "slipdays.txt not found. Proceeding without it."
    tar -zcf p5.tar ./solution README.md Makefile partners.csv
fi