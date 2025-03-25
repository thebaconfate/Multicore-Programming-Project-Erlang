#!/bin/bash

# Compile all .erl files in the current directory
for file in *.erl; do
    if [ -f "$file" ]; then
        echo "Compiling $file..."
        erlc "$file"
        if [ $? -ne 0 ]; then
            echo "Compilation failed for $file"
        exit 1
        fi
    fi
done

echo "All .erl files compiled successfully."

