#!/usr/bin/env python3
import json
import glob

def combine_and_dedupe(pattern, output_file):
    """Combine JSON Lines files and deduplicate by URL"""
    seen_urls = set()
    unique_items = []
    
    # Get all batch files matching the pattern
    files = sorted(glob.glob(pattern))
    print(f"Processing {len(files)} files matching {pattern}")
    
    for filename in files:
        print(f"Reading {filename}...")
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    item = json.loads(line.strip())
                    url = item.get('url')
                    
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        unique_items.append(item)
                except json.JSONDecodeError as e:
                    print(f"Error parsing line in {filename}: {e}")
                    continue
    
    # Write to output file
    print(f"Writing {len(unique_items)} unique items to {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(unique_items, f, indent=2, ensure_ascii=False)
    
    print(f"Done! {len(unique_items)} unique items (from {sum(1 for _ in open(files[0] if files else '', encoding='utf-8')) if files else 0} total)")
    return len(unique_items)

# Combine books
book_count = combine_and_dedupe('book_batch*.jl', 'books-scraped.json')

# Combine authors
author_count = combine_and_dedupe('author_batch*.jl', 'authors-scraped.json')

print(f"\nSummary:")
print(f"  Books: {book_count} unique")
print(f"  Authors: {author_count} unique")
