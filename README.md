# genome-stratification-overlap-pipeline
This script performs a genomic data annotation and quality‑control workflow.
In practical terms, it takes variant lists from Excel, converts them into BED genomic intervals, and then performs genome‑region overlap analysis using large sets of BED files (e.g., GC content, mappability, low complexity, technically difficult regions). It then generates annotated Excel reports, QC summaries, and detailed overlap statistics.
Detail bed files are avaible here : https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/genome-stratifications/v3.6/GRCh38@all/
