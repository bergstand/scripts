#!/usr/bin/env python3
"""
fixrg.py

A script to process a BAM file and update read groups based on a tab-delimited input.
"""

import argparse
import collections
import csv
import sys
import pysam

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Update Read Groups in a BAM file.")
    parser.add_argument("--in", dest="input_bam", required=True, 
                        help="An input BAM to be processed")
    parser.add_argument("--rgs", dest="rgs_file", required=False, 
                        help="A tab-delimited file from where to get read group information")
    parser.add_argument("--out", dest="output_bam", required=True, 
                        help="An output BAM")
    parser.add_argument("--tag", dest="tags", action="append",
                        help="Additional RG tags in format TAG:value")
    parser.add_argument("--preserve-LB", dest="preserve_lb", action="store_true",
                        help="Preserve existing LB tags on reads")
    parser.add_argument("--strip-PG", dest="strip_pg", action="store_true",
                        help="Remove PG lines from header")
    return parser.parse_args()

def read_rgs(rgs_file):
    """Reads the tab-delimited read group file."""
    rgs_data = []
    try:
        with open(rgs_file, 'r', newline='') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                rgs_data.append(row)
    except IOError as e:
        print(f"Error reading RGS file: {e}", file=sys.stderr)
        sys.exit(1)
    return rgs_data

def get_flowcell_lane(read_name):
    """Extracts flowcell and lane from Illumina read name."""
    parts = read_name.split(':')
    if len(parts) >= 4:
        return parts[2], parts[3]
    return "unknown_fc", "unknown_lane"

def resolve_rg(rg_template, flowcell, lane):
    """Substitutes placeholders in RG template."""
    new_rg = rg_template.copy()
    for k, v in new_rg.items():
        if '[FLOWCELL]' in v:
            v = v.replace('[FLOWCELL]', flowcell)
        if '[LANE]' in v:
            v = v.replace('[LANE]', lane)
        new_rg[k] = v
    return new_rg

def process_bam(input_path, output_path, rgs_data, preserve_lb=False, strip_pg=False):
    """Reads input BAM and writes to output BAM."""
    try:
        # Check for placeholders
        placeholders = False
        for rg in rgs_data:
            for v in rg.values():
                if '[FLOWCELL]' in v or '[LANE]' in v:
                    placeholders = True
                    break

        final_rgs = {} # ID -> dict

        need_pass1 = placeholders or preserve_lb
        rg_to_lb = {}

        # Pass 1: Detect RGs if placeholders exist or LB needs preserving
        if need_pass1:
            with pysam.AlignmentFile(input_path, "rb") as in_bam:
                if preserve_lb:
                    for rg_entry in in_bam.header.get('RG', []):
                        if 'ID' in rg_entry and 'LB' in rg_entry:
                            rg_to_lb[rg_entry['ID']] = rg_entry['LB']

                for read in in_bam:
                    fc, lane = get_flowcell_lane(read.query_name)
                    
                    current_lb = None
                    if preserve_lb:
                        if read.has_tag("LB"):
                            current_lb = read.get_tag("LB")
                        elif read.has_tag("RG"):
                            current_lb = rg_to_lb.get(read.get_tag("RG"))

                    for template in rgs_data:
                        resolved = resolve_rg(template, fc, lane)
                        if current_lb is not None:
                            resolved['LB'] = str(current_lb)
                            if 'ID' in resolved:
                                resolved['ID'] = f"{resolved['ID']}.{current_lb}"
                            else:
                                resolved['ID'] = str(current_lb)
                        if 'ID' in resolved:
                            final_rgs[resolved['ID']] = resolved
        else:
            for rg in rgs_data:
                if 'ID' in rg:
                    final_rgs[rg['ID']] = rg

        # Open input BAM file
        with pysam.AlignmentFile(input_path, "rb") as in_bam:
            # Update header with new RGs
            header = in_bam.header.to_dict()
            header['RG'] = []

            if strip_pg and 'PG' in header:
                del header['PG']
            
            sorted_rgs = sorted(final_rgs.values(), key=lambda x: (x.get('SM', ''), x.get('LB', ''), x.get('ID', '')))
            for rg in sorted_rgs:
                header['RG'].append(rg)

            # Open output BAM file, using the header from the input file
            with pysam.AlignmentFile(output_path, "wb", header=header) as out_bam:
                for line in str(out_bam.header).splitlines():
                    if line.startswith('@RG'):
                        print(f"Adding RG to header: {line}", file=sys.stderr)

                counts = collections.defaultdict(int)
                for read in in_bam:
                    rg_id = None
                    resolved = {}
                    if need_pass1:
                        fc, lane = get_flowcell_lane(read.query_name)
                        
                        current_lb = None
                        if preserve_lb:
                            if read.has_tag("LB"):
                                current_lb = read.get_tag("LB")
                            elif read.has_tag("RG"):
                                current_lb = rg_to_lb.get(read.get_tag("RG"))

                        if rgs_data:
                            # Use first template for assignment
                            resolved = resolve_rg(rgs_data[0], fc, lane)
                            if current_lb is not None:
                                resolved['LB'] = str(current_lb)
                                if 'ID' in resolved:
                                    resolved['ID'] = f"{resolved['ID']}.{current_lb}"
                                else:
                                    resolved['ID'] = str(current_lb)
                            rg_id = resolved.get('ID')
                    else:
                        if rgs_data:
                            resolved = rgs_data[0]
                            if 'ID' in resolved:
                                rg_id = resolved['ID']

                    if rg_id:
                        read.set_tag("RG", rg_id, value_type='Z')

                    # Count reads assigned to RG
                    rg_id = read.get_tag("RG") if read.has_tag("RG") else "None"
                    counts[rg_id] += 1
                    # No manipulation yet, just writing to output
                    out_bam.write(read)
                
                print("Summary of reads assigned to each RG ID:", file=sys.stderr)
                for rg_id, count in counts.items():
                    print(f"{rg_id}: {count}", file=sys.stderr)
    except ValueError as e:
        print(f"Error handling BAM files: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    args = parse_args()
    if not args.rgs_file and not args.tags:
        sys.exit("Error: Either --rgs or --tag must be provided.")

    if args.rgs_file:
        rgs_data = read_rgs(args.rgs_file)
    else:
        rgs_data = [{}]

    if args.tags:
        extra_tags = {}
        for tag_str in args.tags:
            if ':' in tag_str:
                k, v = tag_str.split(':', 1)
                extra_tags[k] = v
        for rg in rgs_data:
            rg.update(extra_tags)

    process_bam(args.input_bam, args.output_bam, rgs_data, args.preserve_lb, args.strip_pg)

if __name__ == "__main__":
    main()