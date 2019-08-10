#!/usr/bin/env python3

import argparse, os, glob, csv, sys, shutil

import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.keys import Keys

def name_of_path(pth):
	return os.path.splitext(os.path.basename(pth))[0]

class Case(object):
	def __init__(self):
		self.name = None  # Overrides automatic name
		self.profile = None  # Path to TSV/CSV
		self.evidence = None  # Path to TSV/CSV
		self.contributors = None  # int
		self.deducible = None  # boolean
		self.quantity = None  # float or int, unit picograms (pg)
		self.theta = 0.03  # float--text value used to match exactly on dropdown
		self.labkitid = None  # str--GUID in the database (not set from default if None)
		
	def is_valid(self):
		if not all(x is not None
			for x in (self.profile, self.evidence, self.contributors, self.deducible, self.quantity)
		):
			print('failed presence:', self.profile, self.evidence, self.contributors, self.deducible, self.quantity)
			return False
		if not all(isinstance(x, y) for x, y in (
			(self.profile, (str, bytes)),
			(self.evidence, (str, bytes)),
			(self.contributors, int),
			(self.deducible, bool),
			(self.quantity, (float, int)),
			(self.theta, float),
		)):
			print('failed typecheck:', type(self.profile), type(self.evidence), type(self.contributors), type(self.deducible), type(self.quantity), type(self.theta))
			return False
		return True
		
	def get_name(self):
		if self.name is not None:
			return self.name
		return f'C_{name_of_path(self.profile)}.E_{name_of_path(self.evidence)}.Q_{self.quantity}.{"D" if self.deducible else "ND"}.N_{self.contributors}.T_{self.theta}'
		
	def set_name(self, name):
		self.name = name
		return self
		
	def set_profile(self, profile):
		self.profile = profile
		return self
	
	def set_evidence(self, evidence):
		self.evidence = evidence
		return self
		
	def set_contributors(self, contributors):
		self.contributors = contributors
		return self
		
	def set_deducible(self, deducible):
		self.deducible = deducible
		return self
		
	def set_quantity(self, quantity):
		self.quantity = quantity
		return self
		
	def set_theta(self, theta):
		self.theta = theta
		return self
		
	def set_labkitid(self, labkitid):
		self.labkitid = labkitid
		return self
		
	COMPARISON_IDS = {2: '5', 3: '6'}
	def get_comparison_id(self):
		return self.COMPARISON_IDS.get(self.contributors, None)

class FSTInterface(object):
	def __init__(self, output_path=None, base_uri='http://localhost:2926', page_load_tmout=15, implicit_wait=None, wsize=(1024, 768), no_unlink=False, debug=False):
		self.output_path = output_path
		self.base_uri = base_uri
		self.no_unlink = no_unlink
		self.debug = debug
		
		# Try to defeat the download prompt
		self.profile = webdriver.FirefoxProfile()
		self.profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf")
		self.profile.set_preference("browser.download.manager.showWhenStarting", False)
		self.profile.set_preference("pdfjs.disabled", True)
		
		self.driver = webdriver.Firefox(firefox_profile=self.profile)
		#self.driver = webdriver.Firefox()
		
		self.driver.set_page_load_timeout(page_load_tmout)
		if implicit_wait is not None:
			self.driver.implicitly_wait(implicit_wait)
		self.driver.set_window_size(*wsize)
		
		self.login_uname = None
		
	def get_element_by_id(self, id, tmout=10, poll=0.5):
		if self.debug:
			print('Getting element:', id, file=sys.stderr)
		return WebDriverWait(self.driver, tmout, poll).until(
			expected_conditions.presence_of_element_located((
				By.ID, id
			))
		)
		
	def get_uri(self, path):
		return self.base_uri + path
		
	def fetch_uri(self, path):
		if self.debug:
			print('Fetching URI:', path, file=sys.stderr)
		self.driver.get(self.get_uri(path))
		
	LOGIN_PATH = '/'
	def login(self, user='admin', password='', tmout=10):
		if self.debug:
			print('Logging in as', user, 'password', password, file=sys.stderr)
		if self.login_uname is not None:
			raise RuntimeError('Tried to log in again')
		self.fetch_uri(self.LOGIN_PATH)
		self.get_element_by_id("txbUserName").send_keys(user)
		self.get_element_by_id("txbPassWord").send_keys(password)
		#self.get_element_by_id("form1").submit()
		self.get_element_by_id("btnLogin").click()  # Probably ASP stupidity here
		
		# Wait for the login to proceed
		WebDriverWait(self.driver, tmout).until(
			expected_conditions.url_contains('frmDefault.aspx')
		)
		self.login_uname = user
		
	COMPARISON_PATH = '/frmDefault.aspx'
	FORMPFX = 'ctl00_MainContentHolder_'
	def run(self, case, compare_tmout=30):
		if self.debug:
			print('Running case:', case.get_name(), file=sys.stderr)
			
		if not case.is_valid():
			raise ValueError(f'Case {case.get_name()} is not valid!')
			
		if self.login_uname is None:
			self.login()
			
		self.fetch_uri(self.COMPARISON_PATH)
		
		compid = case.get_comparison_id()
		if compid is None:
			raise ValueError(f'Case {case.get_name()} has no valid comparison ID')
		Select(self.get_element_by_id(f'{self.FORMPFX}ddlCase_Type')).select_by_value(compid)
		
		thetasel = Select(self.get_element_by_id(f'{self.FORMPFX}ddlTheta'))
		try:
			thetasel.select_by_visible_text(str(case.theta))
		except NoSuchElementException:
			raise ValueError(f'Case {case.get_name()} has unusable theta {case.theta}')
		
		if case.labkitid is not None:
			labkitsel = Select(self.get_element_by_id(f'{self.FORMPFX}ddlLabKit'))
			try:
				labkitsel.select_by_value(case.labkitid)
			except NoSuchElementException:
				raise ValueError(f'Case {case.get_name()} has unusable labkitid {case.labkitid}')
				
		self.get_element_by_id(f'{self.FORMPFX}txtSuspPrfl1_Nm').send_keys(case.get_name())
		self.get_element_by_id(f'{self.FORMPFX}btnGo').click()
		
		try:
			profin = self.get_element_by_id(f'{self.FORMPFX}txtFileSuspectInput')
			evidin = self.get_element_by_id(f'{self.FORMPFX}txtFileUnknownInput')
		except TimeoutException:
			raise RuntimeError(f'Case {case.get_name()}--consistency violated while transitioning to frmUpload!')
			
		profin.send_keys(case.profile)
		evidin.send_keys(case.evidence)
		if self.debug:
			print('File input text: Profile:', profin.text, 'Evidence:', evidin.text, file=sys.stderr)
		# Why in the hell is this named "dropout"?
		quant = case.quantity
		if quant == int(quant):
			quant = int(quant)  # Workaround to a crasher at 500.0 pg
		self.get_element_by_id(f'{self.FORMPFX}txDropout').send_keys(str(quant))
		Select(self.get_element_by_id(f'{self.FORMPFX}dlDeducible')).select_by_value('Yes' if case.deducible else 'No')
		self.get_element_by_id(f'{self.FORMPFX}btnLoadData').click()
		
		if self.debug:
			print('Waiting for compare button...', file=sys.stderr)
		
		WebDriverWait(self.driver, compare_tmout).until(
			expected_conditions.visibility_of_element_located((
				By.ID, f"{self.FORMPFX}btnRead"
			))
		)
		
		if self.debug:
			print('Done waiting for compare button', file=sys.stderr)
		
		try:
			# Timing issues in here; is Selenium racy? p2 appears to hang for seconds while the comparison is already done.
			#print('p1')
			btn = self.get_element_by_id(f'{self.FORMPFX}btnRead')
			#print('p2')
			btn.send_keys(Keys.ENTER)
			#print('p3')
		except TimeoutException:
			raise RuntimeError(f'Case {case.get_name()}--consistency violated while transitioning to frmUpload phase read!')
			
		if self.debug:
			print('Waiting for comparison...', file=sys.stderr)
			
		WebDriverWait(self.driver, compare_tmout).until(
			expected_conditions.invisibility_of_element_located((
				By.ID, 'divSpinningIndicator'
			))
		)
		
		if self.debug:
			print('Done waiting for comparison', file=sys.stderr)
		
		if self.output_path:
			candidates = [dir for dir in os.listdir(self.output_path) if case.get_name() in dir]
			if not candidates:
				raise RuntimeError(f'Case {case.get_name()} could not be found in {self.output_path}')
			if len(candidates) > 1:
				raise RuntimeError(f'Case {case.get_name()} had too many candidates ({len(candidates)}--did you clean out the output directory?')
			odata = {}
			fulldir = os.path.join(self.output_path, candidates[0])
			for fn in os.listdir(fulldir):
				if fn == '_instrumentation.txt':  # Handle specially
					with open(os.path.join(fulldir, fn)) as fo:
						for row in csv.reader(fo):
							odata[row[0]] = row[1:]
					continue
				orow = None
				for row in csv.DictReader(open(os.path.join(fulldir, fn))):
					if row['Locus'] == '_OVERALL_':
						orow = row
						break
				else:
					raise RuntimeError(f'Case {case.get_name()}: could not find _OVERALL_ data')
				key = os.path.splitext(fn)[0]
				try:
					odata[key] = float(orow['LRlog10'])
				except ValueError:  # XXX Encoding issues...
					odata[key] = float('-inf')
			if not self.no_unlink:
				shutil.rmtree(fulldir)  # Avoid making listdir expensive, especially on Windows
			if self.debug:
				print('Output data:', odata, file=sys.stderr)
			return odata
			
		return None  # implicit, but nonetheless...
		
	def close(self):
		self.driver.quit()
		
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Run FST over one input')
	
	def try_int_then_float(s):
		try:
			return int(s)
		except ValueError:
			return float(s)
			
	gintf = parser.add_argument_group('Interface', 'Settings for the driver interface')

	gintf.add_argument('-O', '--output-path', help='Path where FST is compiled to output data files (usually under repo root\\FST.Web\\Admin\\Upload). REQUIRED if you want this to output data.')
	gintf.add_argument('--no-unlink', action='store_true', help='Don\'t remove output directories after they\'re scanned for output (only with -O)')
	gintf.add_argument('-H', '--base-uri', default='http://localhost:2926', help='Base URI where the running FST instance is serving')
	gintf.add_argument('--load-tmout', type=int, default=15, help='Page load timeout for selenium driver (seconds)')
	gintf.add_argument('--implicit-wait', type=int, help='Set implicit wait for this number of seconds to serialize loading')
	gintf.add_argument('--window-width', type=int, default=1024, help='Selenium browser window width (pixels)')
	gintf.add_argument('--window-height', type=int, default=768, help='Selenium browser window height (pixels)')
	gintf.add_argument('--username', default='admin', help='Username to log into FST as')
	gintf.add_argument('--password', default='', help='Password to log into FST')
	gintf.add_argument('-d', '--debug', action='store_true', help='Spit out debugging information for troubleshooting')
	
	gcase = parser.add_argument_group('Case', 'Settings for the test case')
	
	gcase.add_argument('-p', '--profile', required=True, help='Path to POI/test profile')
	gcase.add_argument('-e', '--evidence', required=True, help='Path to evidence/replicates')
	gcase.add_argument('-n', '--contributors', type=int, required=True, help='Number of contributors')
	gcase.add_argument('-D', '--deducible', action='store_true', help='Set if the number of contributors is deducible from the evidence')
	gcase.add_argument('-q', '--quantity', type=try_int_then_float, required=True, help='Picograms of genetic material replicated')
	gcase.add_argument('-t', '--theta', type=float, default=0.03, help='Theta correction')
	gcase.add_argument('-k', '--labkitid', help='FST GUID of a lab kit used (default uses FST\'s default)')
	gcase.add_argument('-N', '--name', help='Name of this case (must be filename-safe)')
	
	args = parser.parse_args()
	
	intf = FSTInterface(
		output_path = args.output_path,
		base_uri = args.base_uri,
		page_load_tmout = args.load_tmout,
		implicit_wait = args.implicit_wait,
		wsize = (args.window_width, args.window_height),
		no_unlink = args.no_unlink,
		debug = args.debug,
	)
	
	intf.login(args.username, args.password)
	
	try:
		data = intf.run(Case()\
			.set_name(args.name)\
			.set_profile(os.path.abspath(args.profile))\
			.set_evidence(os.path.abspath(args.evidence))\
			.set_contributors(args.contributors)\
			.set_deducible(args.deducible)\
			.set_quantity(args.quantity)\
			.set_theta(args.theta)\
			.set_labkitid(args.labkitid)
		)
		#if data is not None:
		#	writer = csv.DictWriter(sys.stdout, sorted(data.keys()))
		#	writer.writeheader()
		#	writer.writerow(data)
		print(data)
	finally:
		#input('Pausing for a second for postmortem analysis... press enter when done.')
		intf.close()