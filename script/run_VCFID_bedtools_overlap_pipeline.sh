#!/usr/bin/env bash
set -euo pipefail

EXCEL_DIR="/content"
DOWNLOADS="/content"

CATEGORIES=(
  "FunctionalTechnicallyDifficult"
  "GCcontent"
  "LowComplexity"
)

OUT_DIR="$EXCEL_DIR/annotated_with_bedtools_overlaps"
BED_OUT_DIR="$OUT_DIR/bed_from_excel"
QC_DIR="$OUT_DIR/qc_reports"
DETAIL_DIR="$OUT_DIR/detail_reports"

mkdir -p "$OUT_DIR" "$BED_OUT_DIR" "$QC_DIR" "$DETAIL_DIR"

command -v bedtools >/dev/null || { echo "ERROR: bedtools not found"; exit 1; }
python3 -c "import openpyxl" >/dev/null 2>&1 || {
  echo "ERROR: openpyxl not installed. Run: conda install -y -c conda-forge openpyxl"
  exit 1
}

export EXCEL_DIR DOWNLOADS OUT_DIR BED_OUT_DIR QC_DIR DETAIL_DIR

python3 - <<'PY'
import os, re, glob, gzip, subprocess
from collections import defaultdict, Counter
from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill

EXCEL_DIR = os.environ["EXCEL_DIR"]
DOWNLOADS = os.environ["DOWNLOADS"]
OUT_DIR   = os.environ["OUT_DIR"]
BED_OUT_DIR = os.environ["BED_OUT_DIR"]
QC_DIR = os.environ["QC_DIR"]
DETAIL_DIR = os.environ["DETAIL_DIR"]

CATEGORIES = [
    "FunctionalTechnicallyDifficult",
    "GCcontent",
    "LowComplexity"
]

EXCLUDE_BED_FILES = ()
# I skipped this part because, in my original task, there were certain BED files that we excluded based on instructions from the lead.

VCF_RE = re.compile(r'^(chr[^:]+):(\d+)(?:_([^_]+)_([^_]+))?$')

def list_bed_files(cat_folder):
    return sorted(
        glob.glob(os.path.join(cat_folder, "*.bed")) +
        glob.glob(os.path.join(cat_folder, "*.bed.gz")) +
        glob.glob(os.path.join(cat_folder, "*.bgz.bed.gz"))
    )

def should_exclude(basename):
    return basename in EXCLUDE_BED_FILES

def normalize_bed_file_name(basename):
    x = basename
    if x.startswith("GRCh38_"):
        x = x[len("GRCh38_"):]
    if x.endswith(".bed.gz"):
        x = x[:-7]
    elif x.endswith(".bed"):
        x = x[:-4]
    return x

def run_intersect_wa_wb(a_bed, b_path):
    if b_path.endswith(".gz"):
        z = subprocess.Popen(["zcat", b_path], stdout=subprocess.PIPE, text=True)
        p = subprocess.Popen(
            ["bedtools", "intersect", "-wa", "-wb", "-a", a_bed, "-b", "-"],
            stdin=z.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        z.stdout.close()
        out, err = p.communicate()
        if p.returncode != 0:
            raise RuntimeError(f"bedtools intersect failed for {b_path}:\n{err}")
        return out.splitlines()
    else:
        p = subprocess.Popen(
            ["bedtools", "intersect", "-wa", "-wb", "-a", a_bed, "-b", b_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        out, err = p.communicate()
        if p.returncode != 0:
            raise RuntimeError(f"bedtools intersect failed for {b_path}:\n{err}")
        return out.splitlines()

def find_col(ws, name):
    for c in range(1, ws.max_column + 1):
        h = ws.cell(row=1, column=c).value
        if h is not None and str(h).strip() == name:
            return c
    return None

def parse_variant_to_bed_interval(value):
    if value is None:
        return None
    s = str(value).strip()
    m = VCF_RE.match(s)
    if not m:
        return None

    chrom = m.group(1)
    pos1 = int(m.group(2))
    ref = m.group(3)

    start0 = pos1 - 1
    if ref is None or ref == "":
        ref_len = 1
    else:
        if ref.startswith("<") and ref.endswith(">"):
            ref_len = 1
        else:
            ref_len = len(ref)
    end0 = start0 + ref_len

    region_bed = f"{chrom}:{start0}-{end0}"
    region_1based = f"{chrom}:{pos1}-{pos1 + ref_len - 1}"
    return chrom, start0, end0, region_bed, region_1based, ref_len

def add_new_cols(ws, names):
    start_col = ws.max_column + 1
    mapping = {}
    for i, n in enumerate(names):
        c = start_col + i
        ws.cell(row=1, column=c, value=n)
        mapping[n] = c
        ws.column_dimensions[get_column_letter(c)].width = 90 if n in ("BED_OVERLAPS","BED_OVERLAPS_DETAIL") else 22
    return mapping

def write_excel_report(xlsx_path, sheets_dict):
    wb = Workbook()
    wb.remove(wb.active)

    header_fill = PatternFill(fill_type="solid", fgColor="D9E1F2")
    header_font = Font(bold=True)

    for sname, (headers, rows) in sheets_dict.items():
        ws = wb.create_sheet(title=sname[:31])
        ws.append(headers)
        for c in ws[1]:
            c.font = header_font
            c.fill = header_fill
        for r in rows:
            ws.append(r)
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        widths = {}
        for row in ws.iter_rows(values_only=True):
            for i, v in enumerate(row, 1):
                L = len(str(v)) if v is not None else 0
                widths[i] = max(widths.get(i, 0), min(L, 120))
        for i, w in widths.items():
            ws.column_dimensions[get_column_letter(i)].width = min(max(12, w + 2), 80)

    wb.save(xlsx_path)

xlsx_files = sorted(
    f for f in glob.glob(os.path.join(EXCEL_DIR, "*.xlsx"))
    if not os.path.basename(f).startswith("~$")
)
if not xlsx_files:
    raise SystemExit(f"ERROR: No .xlsx files found in {EXCEL_DIR}")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(BED_OUT_DIR, exist_ok=True)
os.makedirs(QC_DIR, exist_ok=True)
os.makedirs(DETAIL_DIR, exist_ok=True)

for xlsx in xlsx_files:
    base = os.path.splitext(os.path.basename(xlsx))[0]
    qc_path = os.path.join(QC_DIR, f"{base}.vcf_parse_qc.txt")

    wb = load_workbook(xlsx)
    bed_path = os.path.join(BED_OUT_DIR, f"{base}.bed")

    variant_lookup = {}
    total_variant_rows = set()

    qc = Counter()
    bad_examples = []
    longref_examples = []

    with open(bed_path, "w", encoding="utf-8") as out_bed:
        for ws in wb.worksheets:
            vcf_col = find_col(ws, "VCF_ID")
            pos_col = find_col(ws, "POS_ID")
            status_col = find_col(ws, "status_check")
            status_ti_col = find_col(ws, "status_check_TI")

            use_col = vcf_col if vcf_col is not None else pos_col
            use_name = "VCF_ID" if vcf_col is not None else ("POS_ID" if pos_col is not None else None)
            if use_col is None:
                continue

            for r in range(2, ws.max_row + 1):
                qc["rows_seen"] += 1

                vcf_val = ws.cell(row=r, column=vcf_col).value if vcf_col else None
                pos_val = ws.cell(row=r, column=pos_col).value if pos_col else None
                status_val = ws.cell(row=r, column=status_col).value if status_col else ""
                status_ti_val = ws.cell(row=r, column=status_ti_col).value if status_ti_col else ""
                chosen_val = ws.cell(row=r, column=use_col).value

                if chosen_val is None or str(chosen_val).strip() == "":
                    qc["empty"] += 1
                    continue

                parsed = parse_variant_to_bed_interval(chosen_val)
                if not parsed:
                    qc["unparsed"] += 1
                    if len(bad_examples) < 30:
                        bad_examples.append(f"{use_name}={chosen_val} (sheet={ws.title}, row={r})")
                    continue

                chrom, start0, end0, region_bed, region_1based, ref_len = parsed
                qc["parsed"] += 1
                if ref_len == 1:
                    qc["ref_len_1"] += 1
                else:
                    qc["ref_len_gt1"] += 1
                    if len(longref_examples) < 30:
                        longref_examples.append(f"{chosen_val} -> BED:{chrom}:{start0}-{end0} (REFlen={ref_len})")

                out_bed.write(f"{chrom}\t{start0}\t{end0}\t{ws.title}\t{r}\n")
                total_variant_rows.add((ws.title, r))

                variant_lookup[(ws.title, r)] = {
                    "VCF_ID": "" if vcf_val is None else str(vcf_val).strip(),
                    "POS_ID": "" if pos_val is None else str(pos_val).strip(),
                    "status_check": "" if status_val is None else str(status_val),
                    "status_check_TI": "" if status_ti_val is None else str(status_ti_val),
                    "REGION_CHR": chrom,
                    "REGION_START": start0,
                    "REGION_END": end0,
                    "REGION": region_bed,
                    "REGION_1BASED": region_1based,
                }

    with open(bed_path, "rb") as fin, gzip.open(bed_path + ".gz", "wb") as fout:
        fout.writelines(fin)

    with open(qc_path, "w", encoding="utf-8") as q:
        q.write(f"Excel: {xlsx}\n")
        q.write(f"BED:   {bed_path}.gz\n\n")
        for k in ["rows_seen","parsed","unparsed","empty","ref_len_1","ref_len_gt1"]:
            q.write(f"{k}\t{qc.get(k,0)}\n")
        q.write("\nExamples (unparsed):\n")
        q.write("\n".join(bad_examples) + ("\n" if bad_examples else "None\n"))
        q.write("\nExamples (REF length >1):\n")
        q.write("\n".join(longref_examples) + ("\n" if longref_examples else "None\n"))

    total_vcf_rows = len(total_variant_rows)

    overlaps_cat = defaultdict(lambda: defaultdict(set))
    detail_hits = set()
    excluded_skips_rows = []
    overlapped_rows = set()

    for cat in CATEGORIES:
        cat_dir = os.path.join(DOWNLOADS, cat)
        bed_files = list_bed_files(cat_dir)
        if not bed_files:
            continue

        for b in bed_files:
            bname = os.path.basename(b)
            if should_exclude(bname):
                excluded_skips_rows.append((cat, bname, "excluded_by_user_list"))
                continue

            bname_norm = normalize_bed_file_name(bname)

            for ln in run_intersect_wa_wb(bed_path, b):
                parts = ln.split("\t")
                if len(parts) < 8:
                    continue
                sheet = parts[3]
                try:
                    row = int(parts[4])
                except ValueError:
                    continue

                overlaps_cat[(sheet, row)][cat].add(bname_norm)
                meta = variant_lookup.get((sheet, row), {})
                overlapped_rows.add((sheet, row))

                detail_hits.add((
                    base, sheet, row,
                    meta.get("VCF_ID",""),
                    meta.get("POS_ID",""),
                    meta.get("status_check",""),
                    meta.get("status_check_TI",""),
                    meta.get("REGION_CHR",""),
                    meta.get("REGION_START",""),
                    meta.get("REGION_END",""),
                    meta.get("REGION",""),
                    meta.get("REGION_1BASED",""),
                    cat,
                    bname_norm
                ))

    # add non-overlap rows
    no_overlap_rows = total_variant_rows - overlapped_rows
    for (sheet, row) in sorted(no_overlap_rows):
        meta = variant_lookup.get((sheet, row), {})
        detail_hits.add((
            base, sheet, row,
            meta.get("VCF_ID",""),
            meta.get("POS_ID",""),
            meta.get("status_check",""),
            meta.get("status_check_TI",""),
            meta.get("REGION_CHR",""),
            meta.get("REGION_START",""),
            meta.get("REGION_END",""),
            meta.get("REGION",""),
            meta.get("REGION_1BASED",""),
            "NA",
            "NA"
        ))

    # Annotated Excel
    wb2 = load_workbook(xlsx)
    for ws in wb2.worksheets:
        vcf_col = find_col(ws, "VCF_ID")
        pos_col = find_col(ws, "POS_ID")
        use_col = vcf_col if vcf_col is not None else pos_col
        if use_col is None:
            continue

        newcols = add_new_cols(ws, ["REGION_CHR","REGION_START","REGION_END","REGION","REGION_1BASED","BED_OVERLAPS"])
        for r in range(2, ws.max_row + 1):
            key = (ws.title, r)
            meta = variant_lookup.get(key)
            if not meta:
                for c in ["REGION_CHR","REGION_START","REGION_END","REGION","REGION_1BASED","BED_OVERLAPS"]:
                    ws.cell(row=r, column=newcols[c], value="")
                continue

            ws.cell(row=r, column=newcols["REGION_CHR"], value=meta["REGION_CHR"])
            ws.cell(row=r, column=newcols["REGION_START"], value=int(meta["REGION_START"]))
            ws.cell(row=r, column=newcols["REGION_END"], value=int(meta["REGION_END"]))
            ws.cell(row=r, column=newcols["REGION"], value=meta["REGION"])
            ws.cell(row=r, column=newcols["REGION_1BASED"], value=meta["REGION_1BASED"])

            hit = overlaps_cat.get(key)
            if not hit:
                ws.cell(row=r, column=newcols["BED_OVERLAPS"], value="")
            else:
                parts = []
                for cat in CATEGORIES:
                    if cat in hit:
                        parts.append(f"{cat}=" + ",".join(sorted(hit[cat])))
                ws.cell(row=r, column=newcols["BED_OVERLAPS"], value="|".join(parts))

    out_xlsx = os.path.join(OUT_DIR, f"{base}.with_bedtools_overlaps.xlsx")
    wb2.save(out_xlsx)

    # detail rows
    detail_rows = [list(t) for t in sorted(
        detail_hits,
        key=lambda x: (x[0], x[12], x[13], x[7], x[8], x[1], x[2])
    )]

    # detail_region_grouped (same as before)
    grouped = defaultdict(set)
    for rec in detail_rows:
        bedf = rec[13]
        if bedf == "NA":
            continue
        key = tuple(rec[:13])  # all fields except bed_file
        grouped[key].add(bedf)

    grouped_rows = []
    for k, bfs in sorted(grouped.items(), key=lambda x: (x[0][12], x[0][7], x[0][8], x[0][1], x[0][2])):
        grouped_rows.append(list(k) + [len(bfs), "|".join(sorted(bfs))])

    # summary_by_bedfile (unique by (sheet,row))
    by_bed = defaultdict(set)
    for rec in detail_rows:
        cat = rec[12]
        bedf = rec[13]
        if bedf == "NA" or cat == "NA":
            continue
        by_bed[(cat, bedf)].add((rec[1], rec[2]))
    summary_bed_rows = []
    for (cat, bedf), rowset in sorted(by_bed.items(), key=lambda x: (-len(x[1]), x[0][0], x[0][1])):
        ov = len(rowset)
        pct = (ov / total_vcf_rows * 100.0) if total_vcf_rows else 0.0
        summary_bed_rows.append([base, cat, bedf, total_vcf_rows, ov, round(pct, 2)])

    # summary_by_category_total (unique by (sheet,row))
    by_cat = defaultdict(set)
    for rec in detail_rows:
        cat = rec[12]
        if cat == "NA":
            continue
        by_cat[cat].add((rec[1], rec[2]))
    summary_cat_rows = []
    for cat in CATEGORIES:
        ov = len(by_cat.get(cat, set()))
        pct = (ov / total_vcf_rows * 100.0) if total_vcf_rows else 0.0
        summary_cat_rows.append([base, cat, total_vcf_rows, ov, round(pct, 2)])

    # pivots INCLUDE NA bed_file
    # every row
    pivot_counts_every = defaultdict(Counter)
    status_set = set()
    for rec in detail_rows:
        bedf = rec[13]  # includes "NA"
        st = (rec[5] or "").strip() or "NA"
        pivot_counts_every[bedf][st] += 1
        status_set.add(st)
    status_cols = sorted(status_set)
    pivot_rows_every = []
    for bedf in sorted(pivot_counts_every.keys()):
        pivot_rows_every.append([bedf] + [pivot_counts_every[bedf].get(st, 0) for st in status_cols])

    # unique by (sheet,row)
    pivot_seen = defaultdict(lambda: defaultdict(set))
    status_set2 = set()
    for rec in detail_rows:
        bedf = rec[13]
        st = (rec[5] or "").strip() or "NA"
        pivot_seen[bedf][st].add((rec[1], rec[2]))
        status_set2.add(st)
    status_cols2 = sorted(status_set2)
    pivot_rows_unique = []
    for bedf in sorted(pivot_seen.keys()):
        pivot_rows_unique.append([bedf] + [len(pivot_seen[bedf].get(st, set())) for st in status_cols2])

    # per-sample excel report with requested sheets
    sample_prefix = os.path.join(DETAIL_DIR, base)
    sample_report_xlsx = sample_prefix + ".overlap_detail_report.xlsx"

    write_excel_report(sample_report_xlsx, {
        "detail_region_to_bedfile": (
            ["sample","sheet","row","VCF_ID","POS_ID","status_check","status_check_TI",
             "chrom","start0","end0","region_bed","region_1based","category","bed_file"],
            detail_rows
        ),
        "detail_region_grouped": (
            ["sample","sheet","row","VCF_ID","POS_ID","status_check","status_check_TI",
             "chrom","start0","end0","region_bed","region_1based","category","n_bed_files_hit","overlap_bed_files_detail"],
            grouped_rows
        ),
        "summary_by_bedfile": (
            ["sample","category","bed_file","total_vcf_rows","overlap_rows","percent_of_vcf_rows"],
            summary_bed_rows
        ),
        "summary_by_category_total": (
            ["sample","category","total_vcf_rows","overlap_rows","percent_of_vcf_rows"],
            summary_cat_rows
        ),
        "pivot_bedfile_status_every": (
            ["bed_file"] + status_cols,
            pivot_rows_every
        ),
        "pivot_bedfile_status_unique": (
            ["bed_file"] + status_cols2,
            pivot_rows_unique
        ),
        "excluded_skips": (
            ["category","bed_file","reason"],
            [list(r) for r in sorted(set(excluded_skips_rows))]
        ),
    })

    print(f"WROTE: {out_xlsx}")
    print(f"  REPORT: {sample_report_xlsx}")

print("DONE.")
print("OUT_DIR:", OUT_DIR)
print("DETAIL_DIR:", DETAIL_DIR)
print("QC_DIR:", QC_DIR)
PY

echo "✅ Done"
echo "OUT_DIR: $OUT_DIR"
echo "DETAIL_DIR: $DETAIL_DIR"
echo "QC_DIR: $QC_DIR"
