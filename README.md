# MSSNG DB6 Joint Genotyping by region
Joint genotype samples on a per-region basis. Use Sentieon's [`generate_shards.sh` script](https://support.sentieon.com/appnotes/distributed_mode/) (also found in the github repo: `dockerfiles/sentieon-bcftools:201808.06/scripts/generate_shards.sh`) to generate region files from a reference genome index or dict using a specified region size (e.g. 50 million base pairs will split the genome into 65 shards). This workflow should then be run once per region. 

`gvcf_URLs` is a file specifying the gs:// bucket locations of each of the gvcfs output by step 1 (one per line).
