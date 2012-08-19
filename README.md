CloudBrush
==========================================

CloudBrush is a de novo genome assembler based on the string graph and mapreduce framework, 
it is released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
as a Free and Open Source Software Project.

More details about CloudBrush Project you can get under: https://github.com/ice91/CloudBrush

--------
requirement: hadoop cluster

e.g hadoop fs -put data/Ec10k.sim.sfa Ec10k 
hadoop jar CloudBrush.jar -reads Ec10k -asm Ec10k_Brush -k 21 -readlen 36
