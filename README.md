CloudBrush
==========================================

CloudBrush is a de novo genome assembler based on the string graph and mapreduce framework, 
it is released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
as a Free and Open Source Software Project.

More details about CloudBrush Project you can get under: https://github.com/ice91/CloudBrush

--------
requirement: hadoop cluster

step 1: convert *.fastq to *.sfa

e.g. perl data/preprocessor.pl -renum data/Ec10k.sim.fastq > data/Ec10k.sim.sfa

step 2: upload *.sfa to HDFS

e.g. hadoop fs -put data/Ec10k.sim.sfa Ec10k

(You can use CloudRS(ReadStackCorrector) as preprocesser to correct sequencing errors and prepare *.sfa in HDFS. 
More details about CloudRS Project you can get under: 
https://github.com/ice91/ReadStackCorrector) 

step 3 perform the CloudBrush using hadoop cluster

e.g. hadoop jar CloudBrush.jar -reads Ec10k -asm Ec10k_Brush -k 21 -readlen 36

e.g. hadoop jar CloudBrush.jar -reads Ec10k -asm Ec10k_Brush -k 21 -readlen 36 -javaopts -Djava.util.Arrays.useLegacyMergeSort=True

step 4 download *.fasta from HDFS

e.g. hadoop fs -cat Ec10k_Brush/* > Ec10k_Brush.fasta

[Reference] Yu-Jung Chang, Chien-Chih Chen, Chuen-Liang Chen and Jan-Ming Ho, "De Novo Next Generation Genomic Sequence Assembler Based on String Graph and MapReduce Cloud Computing Framework," BMC Genomics, volume 13, number Suppl 7, pages S28, December 2012.
