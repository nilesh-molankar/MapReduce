This MapReduce program is a distributed virus scanning utility.

This program is meant to read in several well known Windows 
viruses. These input files have been excluded from this 
repository for obvious reasons. My intention is to process
each file to look for common byte patterns.

The input file will be converted to a byte array, and each key 
will be composed of 67 bytes. This is the average size of a
virus signature stored in ClamAV (Yue Hao). Each array of 67
bytes will then be sent to the reducer for aggregation. The 
results of the reduce phase will show which sequence of bytes
appeared the most within each input file.


Yue Hao, Computational Intelligence and Security: International Conference, CIS 2005