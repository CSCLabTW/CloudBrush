####

CloudBrush is derived from Contrail Project on SourceForge, released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

More information about Contrail Project you can find under: http://sourceforge.net/apps/mediawiki/contrail-bio/

####


requirement: hadoop cluster

e.g hadoop fs -put data/Ec10k.sim.sfa Ec10k 
hadoop jar CloudBrush.jar -reads Ec10k -asm Ec10k_Brush -k 21 -readlen 36

