"""
Import sample data for classification engine
"""

import predictionio
import argparse

def import_events(client, file):
  f = open(file, 'r')
  count = 0
  print "Importing data..."
  for line in f:
    data = line.rstrip('\r\n')
    attr = data.split(" ")
    client.create_event(
      event="$set",
      entity_type="profile",
      entity_id=str(count), # use the count num as user ID
      properties= {
        "attr0" : int(attr[0]),
        "attr1" : int(attr[1]),
        "attr2" : int(attr[2])
      }
    )
    count += 1
  f.close()
  print "%s events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for classification engine")
  parser.add_argument('--access_key', default='Hp3CTtxyXGSXbBpxP4QKqcOBKWKyCz200EeaZEjDDywyaBLoVhUa7JoZN3f8Viu4')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/data.txt")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
