import argparse
import random
import sys
from time import gmtime, strftime

CUSTOMER_NO_PREFIX = 'customer-'
ETICKET_NO_PREFIX = 'etck-'
FLIGHT_NO_PREFIX = 'ww'

def default_args(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--cust_no_max',
      default=50,
      type=int,
      help='Maximum integer value to use for customerNo value.')
  parser.add_argument(
      '--eticket_no_max',
      default=50,
      type=int,
      help='Maximum integer value to use for eTicketNo value.')
  parser.add_argument(
      '--num_records',
      default=50,
      type=int,
      help='Number of records to output.')
  parser.add_argument(
      '--flight_no_max',
      default=20,
      type=int,
      help='Maximum integer value to use for flightNo value.')
  parser.add_argument(
      '--points_max',
      default=100,
      type=int,
      help='Maximum integer value to use for points value.')
  parser.add_argument(
      '--load_id',
      required=True,
      help='Value to use for the loadId column.')
  parser.add_argument(
      '--output',
      required=True,
      help='Output file')
  parsed_args, _ = parser.parse_known_args(argv)
  return parsed_args


def main(argv):
  args = default_args(argv)

  with open(args.output, 'w+') as f:
      for i in range(args.num_records):
          customer_no = CUSTOMER_NO_PREFIX + str(random.randint(1, args.cust_no_max))
          eticket_no = ETICKET_NO_PREFIX + CUSTOMER_NO_PREFIX + str(random.randint(1, args.eticket_no_max))
          flight_no = FLIGHT_NO_PREFIX + str(random.randint(1, args.flight_no_max))
          points = str(random.randint(1, args.points_max))
          datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
          load_id = args.load_id
          out = ','.join([load_id, customer_no, eticket_no, flight_no, points, datetime])
          f.write(out + '\n')

if __name__ == '__main__':
  main(sys.argv[1:])
