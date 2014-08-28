import MapReduce
import sys

# Consider a set of key-value pairs where each key
#  is sequence id and each value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA....
# Write a MapReduce query to remove the last 10 
# characters from each string of nucleotides, 
# then remove any duplicates generated.

mr = MapReduce.MapReduce()

def mapper(record):
    seq_id = record[0]
    nucleotides = record[1]
    
    mr.emit_intermediate(nucleotides[:-10],seq_id)

def reducer(key, list_of_values):
    mr.emit(key)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)