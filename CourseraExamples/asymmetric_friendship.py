import MapReduce
import sys

#The relationship "friend" is often symmetric, 
#meaning that if I am your friend, you are my friend.
#Implement a MapReduce algorithm to 
#check whether this property holds. Generate a 
#list of all non-symmetric friend relationships.

mr = MapReduce.MapReduce()

def mapper(record):
  # emit every combo of friends
  mr.emit_intermediate(record[0], record[1])
  mr.emit_intermediate(record[1], record[0])

def reducer(key, list_of_values):
    # 
    friends = {}
    for value in list_of_values:
        if value in friends:
          del friends[value]
        else:
          friends[value] = 'symmetric'
    for friend in friends:
        mr.emit((key, friend))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)