To Do
=====

1.  For example, from the citation data set you may be interested 
in finding the ten most- cited patents. A sequence of two MapReduce 
jobs can do this. The first one creates the “inverted” citation 
data set and counts the number of citations for each patent, and 
the second job finds the top ten in that “inverted” data.

2.  Given our patent data sets, you may want to find out if certain 
countries cite patents from another country. You’ll have to look 
at citation data (cite75_99.txt) as well as patent data for country 
information (apat63_99. txt).

3.  In both patent data sets we’ve used (cite75_99.txt and 
apat63_99.txt), the first row is metadata (column names). So far 
we’ve had to explicitly or implicitly filter out that row in our 
mappers, or interpret our results knowing that the metadata record 
has some deterministic influence. A more permanent solution is to 
remove the metadata row from the input data and keep track of it 
elsewhere. Another solution is to write a mapper as a preprocessor 
that filters all records that look like metadata. (For example, 
records that don’t start with a numeric patent number.) Write such 
a mapper and use ChainMapper/ChainReducer to incorporate it into 
your MapReduce programs.
