#!/usr/bin/env python3

import argparse, os, sqlite3, threading, json, time, csv, glob

import run

class Database(object):
	CASE_KEYS = {
		'profile': 'TEXT',
		'evidence': 'TEXT',
		'contributors': 'INTEGER',
		'deducible': 'INTEGER',
		'quantity': 'REAL',
		'theta': 'REAL',
		'labkitid': 'TEXT',
	}
	RESULT_KEYS = {
		'odata': 'BLOB',
	}
	OPERATIONAL_KEYS = {
		'claimant': 'INTEGER',
	}
	TABLE = 'batch_data'
	def __init__(self, fn, autosave=True):
		self.con = sqlite3.connect(fn, check_same_thread=False)
		self.autosave = autosave
		self.lock = threading.Lock()
		
		self.ALL_KEYS = {}
		for keyset in (self.CASE_KEYS, self.RESULT_KEYS, self.OPERATIONAL_KEYS):
			self.ALL_KEYS.update(keyset)
			
		self.CASE_KEY_SEQ = list(self.CASE_KEYS.keys())
			
		self.con.execute(
			f'CREATE TABLE IF NOT EXISTS {self.TABLE} ({", ".join(k + " " + v for k, v in self.ALL_KEYS.items())})'
		)
		self.con.execute(
			f'CREATE INDEX IF NOT EXISTS {self.TABLE}_claims ON {self.TABLE} (claimant)'
		)
		self.con.execute(
			f'CREATE INDEX IF NOT EXISTS {self.TABLE}_unfinished ON {self.TABLE} (odata) WHERE odata IS NULL'
		)
		
	def add_case(self, case):
		self.con.execute(
			f'INSERT INTO {self.TABLE} ({", ".join(self.CASE_KEY_SEQ)}) VALUES ({", ".join("?" for i in self.CASE_KEY_SEQ)})',
			tuple(getattr(case, i) for i in self.CASE_KEY_SEQ)
		)
		if self.autosave:
			self.save()
			
	def save(self):
		if self.con.in_transaction:
			try:
				self.con.commit()
			except sqlite3.OperationalError:
				pass  # raced end of transaction
		
	def claim_batch(self, claim_key, batch_size=64):
		if claim_key is None:
			raise ValueError('bad claim_key')
			
		with self.lock:
			self.save()
			self.con.execute('BEGIN EXCLUSIVE')
			cur = self.con.execute(f'SELECT rowid FROM {self.TABLE} WHERE claimant IS NULL AND odata IS NULL LIMIT {batch_size}')
			rowids = [i[0] for i in cur.fetchall()]
			self.con.executemany(f'UPDATE {self.TABLE} SET claimant=? WHERE rowid=?',
				((claim_key, rowid) for rowid in rowids)
			)
			self.con.commit()
			
			cases = [None] * len(rowids)
			for idx, rowid in enumerate(rowids):
				case = run.Case()
				cur = self.con.execute(f'SELECT rowid, {", ".join(self.CASE_KEY_SEQ)} FROM {self.TABLE} WHERE rowid=?', (rowid,))
				values = cur.fetchone()
				for k, v in zip(self.CASE_KEY_SEQ, values[1:]):
					setattr(case, k, v)
				case.deducible = bool(case.deducible)
				case.rowid = values[0]
				cases[idx] = case
		
		return cases
		
	def write_results(self, case, odata):
		with self.lock:
			self.con.execute(
				f'UPDATE {self.TABLE} SET odata=? WHERE rowid=?',
				(json.dumps(odata), case.rowid)
			)
			if self.autosave:
				self.save()
			
	def total_cases(self):
		cur = self.con.execute(f'SELECT count(*) FROM {self.TABLE}')
		return cur.fetchone()[0]
		
	def finished_cases(self):
		cur = self.con.execute(f'SELECT count(*) FROM {self.TABLE} WHERE odata IS NOT NULL')
		return cur.fetchone()[0]
		
	def progressing_cases(self):
		cur = self.con.execute(f'SELECT count(*) FROM {self.TABLE} WHERE claimant IS NOT NULL AND odata IS NULL')
		return cur.fetchone()[0]
		
	def clean(self):
		cur = self.con.execute(f'UPDATE {self.TABLE} SET claimant=NULL WHERE odata IS NULL')
		if self.autosave:
			self.save()
		return cur.rowcount
		
	def iter_results(self):
		for row in self.con.execute(f'SELECT odata, {", ".join(self.CASE_KEY_SEQ)} FROM {self.TABLE} WHERE odata IS NOT NULL'):
			case = run.Case()
			for k, v in zip(self.CASE_KEY_SEQ, row[1:]):
				setattr(case, k, v)
			yield case, json.loads(row[0])
		
class BatchExecutor(threading.Thread):
	def __init__(self, db, intf, batch_size=64, compare_tmout=30, verbose=True):
		super().__init__()
		
		self.db = db
		self.intf = intf
		self.batch_size = batch_size
		self.compare_tmout = compare_tmout
		self.verbose = verbose
		self.finished = False
		
	def print(self, *args, **kwargs):
		if self.verbose:
			print(self.name, *args,**kwargs)
		
	def run(self):
		claim_key = self.ident
		
		while True:
			cases = self.db.claim_batch(claim_key, batch_size=self.batch_size)
			if not cases:
				self.print('no more cases!')
				self.finished = True
				return
					
			self.print(f'Processing a batch of size {len(cases)}')
			for case in cases:
				odata = self.intf.run(case)
				self.db.write_results(case, odata)
			self.db.save()
			self.print('Batch finished')
			
class ProgressObserver(threading.Thread):
	def __init__(self, db, interval=10):
		super().__init__(daemon=True)
		
		self.db = db
		self.interval = interval
		
	def run(self):
		while True:
			tm = time.ctime()
			total = self.db.total_cases()
			finished = self.db.finished_cases()
			progressing = self.db.progressing_cases()
			
			print(f'{tm} Progressing/Finished/Total {progressing}/{finished}/{total} ({100*finished/total:.2f}%)')
			
			time.sleep(self.interval)
			
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Prepare, run, and export data from FST over multiple inputs')
	parser.add_argument('dbfile', help='The database to operate on')
	subparsers = parser.add_subparsers()
	
	def cmd_prepare(args):
		db = Database(args.dbfile, autosave=False)
		
		param_map = {}
		for row in csv.DictReader(open(args.parameters)):
			param_map[row['Evidence']] = row
			
		profiles = glob.glob(os.path.join(args.profile_dir, '**', args.profile_glob), recursive=True)
		evidence = glob.glob(os.path.join(args.evidence_dir, '**', args.evidence_glob), recursive=True)
		
		if (not evidence) or (not profiles):
			raise ValueError(f'Either no evidence ({len(evidence)}) or no profiles ({len(profiles)})--check paths!')
			
		print(f'Adding {len(evidence)} evidence, {len(profiles)} profiles...')
		
		case = run.Case()
		
		for idx, ev in enumerate(evidence):
			params = param_map[os.path.basename(ev)]
			ev = os.path.abspath(ev)
			case\
				.set_evidence(ev)\
				.set_contributors(int(params['Contributors']))\
				.set_deducible(params['Deducible'].lower() in ('yes', 'd'))\
				.set_quantity(params['Quantity'])\
				.set_theta(params.get('Theta', args.default_theta))\
				.set_labkitid(params.get('LabKitId', args.default_labkitid))
			for prof in profiles:
				prof = os.path.abspath(prof)
				case.set_profile(prof)
				db.add_case(case)
			print(f'\r{" "*50}\r{idx+1}/{len(evidence)} {ev}')
			if args.save_every_evidence:
				db.save()
				
		print('Done.')
		db.save()
		
	parser_prepare = subparsers.add_parser('prepare')
	parser_prepare.set_defaults(func=cmd_prepare)
	parser_prepare.add_argument('-p', '--profile-dir', required=True, help='Load profiles under this directory')
	parser_prepare.add_argument('--profile-glob', default='*.csv', help='Loaded profiles must match this glob pattern')
	parser_prepare.add_argument('-e', '--evidence-dir', required=True, help='Load evidence under this directory')
	parser_prepare.add_argument('--evidence-glob', default='*.csv', help='Loaded evidence must match this glob pattern')
	parser_prepare.add_argument('-P', '--parameters', required=True, help='Mapping file for evidence filenames to parameters (must have Evidence,Contributors,Deducible,Quantity, optionally Theta,LabKitId)')
	parser_prepare.add_argument('--default-theta', type=float, default=0.01, help='Default theta where not specified in parameters')
	parser_prepare.add_argument('--default-labkitid', help='Default labkitid where not specified in parameters')
	parser_prepare.add_argument('--save-every-evidence', action='store_true', help='Commit the database after adding each evidence batch (per all profiles)')
	
	def cmd_status(args):
		db = Database(args.dbfile, autosave=False)
		
		progressing = db.progressing_cases()
		finished = db.finished_cases()
		total = db.total_cases()
		
		print(f'Progressing/Finished/Total {progressing}/{finished}/{total} ({100*finished/total:.2f}%)')
		
	parser_status = subparsers.add_parser('status')
	parser_status.set_defaults(func=cmd_status)
	
	def cmd_clean(args):
		db = Database(args.dbfile, autosave=True)
		print('Cleaned', db.clean(), 'rows')
		
	parser_clean = subparsers.add_parser('clean')
	parser_clean.set_defaults(func=cmd_clean)
	
	def cmd_run(args):
		db = Database(args.dbfile, autosave=False)
		
		print('Creating interfaces...')
		
		intfs = [
			run.FSTInterface(
				output_path = args.output_path,
				base_uri = args.base_uri,
				page_load_tmout = args.load_tmout,
				implicit_wait = args.implicit_wait,
				wsize = (args.window_width, args.window_height),
				debug = args.debug,
			)
			for i in range(args.jobs)
		]
		
		print('Logging in...')
		
		for intf in intfs:
			intf.login(args.username, args.password)
		
		print('Creating threads...')
		
		threads = [
			BatchExecutor(db, intf,
				batch_size = args.batch_size,
				compare_tmout = args.compare_tmout,
				verbose = args.verbose,
			)
			for intf in intfs
		]
		
		watch = ProgressObserver(db, interval = args.progress_interval)
		watch.start()
		for thr in threads:
			thr.start()
		
		while True:
			if all(t.finished for t in threads):
				break
				
			for idx, t in enumerate(threads):
				if (not t.is_alive()) and (not t.finished):
					print('Main thread: restarting a crashed thread...')
					intfs[idx].close()
					intfs[idx] = run.FSTInterface(
						output_path = args.output_path,
						base_uri = args.base_uri,
						page_load_tmout = args.load_tmout,
						implicit_wait = args.implicit_wait,
						wsize = (args.window_width, args.window_height),
						debug = args.debug,
					)
					threads[idx] = BatchExecutor(db, intfs[idx],
						batch_size = args.batch_size,
						compare_tmout = args.compare_tmout,
						verbose = args.verbose,
					)
					threads[idx].start()
					
			time.sleep(args.progress_interval)
			
		print('Main thread finished!')
		for intf in intfs:
			intf.close()
		db.save()
		
	parser_run = subparsers.add_parser('run')
	parser_run.set_defaults(func=cmd_run)
	parser_run.add_argument('-j', '--jobs', type=int, default=8, help='Number of simultaneous interfaces')
	parser_run.add_argument('-b', '--batch-size', type=int, default=64, help='Number of cases to batch before writing back to DB')
	parser_run.add_argument('-v', '--verbose', action='store_true', help='Print messages from the workers')
	parser_run.add_argument('--compare-tmout', type=int, default=30, help='How long to wait for comparison before crashing')
	parser_run.add_argument('-i', '--progress-interval', type=int, default=10, help='Wait for this long between status messages')
	
	gintf = parser_run.add_argument_group('Interface', 'Settings for the driver interface')
	gintf.add_argument('-O', '--output-path', required=True, help='Path where FST is compiled to output data files (usually under repo root\\FST.Web\\Admin\\Upload). REQUIRED if you want this to output data.')
	gintf.add_argument('--no-unlink', action='store_true', help='Don\'t remove output directories after they\'re scanned for output (only with -O)')
	gintf.add_argument('-H', '--base-uri', default='http://localhost:2926', help='Base URI where the running FST instance is serving')
	gintf.add_argument('--load-tmout', type=int, default=15, help='Page load timeout for selenium driver (seconds)')
	gintf.add_argument('--implicit-wait', type=int, help='Set implicit wait for this number of seconds to serialize loading')
	gintf.add_argument('--window-width', type=int, default=1024, help='Selenium browser window width (pixels)')
	gintf.add_argument('--window-height', type=int, default=768, help='Selenium browser window height (pixels)')
	gintf.add_argument('--username', default='admin', help='Username to log into FST as')
	gintf.add_argument('--password', default='', help='Password to log into FST')
	gintf.add_argument('-d', '--debug', action='store_true', help='Spit out debugging information for troubleshooting')
	
	
	args = parser.parse_args()
	args.func(args)