#!/usr/bin/env python3

import os, glob, argparse, time

parser = argparse.ArgumentParser(description = 'Periodically remove files matching a pattern from a directory')

parser.add_argument('dir', help='Directory to monitor')
parser.add_argument('glob', help='Glob to match files to remove (escape this!)')
parser.add_argument('-i', '--interval', type=float, default=10.0, help='Period to run')

args = parser.parse_args()

pattern = os.path.join(args.dir, args.glob)
while True:
	for fn in glob.glob(pattern):
		print('Removing:', fn)
		os.unlink(fn)
	print('Waiting...')
	time.sleep(args.interval)