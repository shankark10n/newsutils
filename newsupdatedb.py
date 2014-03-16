from collections import defaultdict as dd
import datetime as dt
import newsutils as nu
import textutils as tu
import os
import sys
import re
from itertools import chain
import pymongo as pm

def nchoosetwo(l):
	'''Returns a list of pairs of elements chosen from a list in sorted order.'''
	tup = lambda k1: [(k1, k2) for k2 in l if k1<k2]
	return [elm for elm in chain.from_iterable(map(tup, l))]
boul = lambda x: 1 if x else 0
tcmp = lambda x,y: -1 if x[1] < y[1] else boul(y[1] < x[1])
strtodate = lambda date: dt.datetime.strptime(date, '%Y%m%d')
datetostr = lambda date: dt.datetime.strftime(date, '%Y-%m-%d')

def build_keyword_graph_single_folder(path = '/home/shankar/work/data/news/politico/',\
									  conditions = [lambda x: x.endswith('.html')],
									 filehandle = '', dbhandle = ''):
	'''single folder keyword graph
	@param path
	@param conditions list of conditions the file name pattern should satisfy
	'''
	files = [f for f in os.listdir(path) if f.endswith('.html')]
	files = [f for f in files if all([g(f) for g in conditions])]
	news_source = [s for s in path.split('/') if s][-1]

	last_pid = 0
	last_date = ''
	if filehandle:
		dump = open(filehandle, 'a')
	elif dbhandle:
		kwc = dbhandle['keywords']
		kwe = dbhandle['edges']
	else:
		dump = sys.stdout
	for each in files:
		pid,date,source = [s.encode('utf8') for s in each.split('.')[:3]]
		if int(pid) > last_pid:
			last_pid = int(pid)
		if date > last_date:
			last_date = date
		#date = dt.datetime.strptime(date, '%Y%m%d')
		page = open(path + each).read()
		if (page == None or len(page) == 0):
			continue # ignore empty files
			# raise RuntimeError('Error. Empty file %s' % path + each)
		keywords = nu.get_keywords(page=open(path + each).read())
		if filehandle:
			# only dump edges
			dump.write('\n'.join([','.join([k[0].encode('utf8'), k[1].encode('utf8'),\
											 date, source, pid])\
									for k in nchoosetwo(keywords)]))
			dump.write('\n')
		elif dbhandle:
			# insert keyword document
			for k in keywords:
				kwc.insert({'keyword': k.encode('utf8'), 'date': date,\
							 'source': source, 'pid': pid})
			for edge in nchoosetwo(keywords):
				kwe.insert({'keyword1': edge[0].encode('utf8'),\
							'keyword2': edge[1].encode('utf8'),\
							'date': date,
							'source': source,
							'pid': pid})
		else:
			pass
	if filehandle:
		dump.close()
	elif dbhandle:
		# log state for each call
		if not(dbhandle['state'].find({'source': news_source}).count()):
			dbhandle['state'].insert({'source': news_source,\
									'last_date': last_date,\
									'last_pid': last_pid})
		else:
			known_last_pid = dbhandle['state'].find_one({'source': news_source})['last_pid']
			known_last_date = dbhandle['state'].find_one({'source': news_source})['last_date']
			if last_pid > known_last_pid:
				dbhandle['state'].update({'source': news_source},\
						{"$set": {'last_pid': last_pid}})
			if last_date > known_last_date:
				dbhandle['state'].update({'source': news_source},\
						{"$set": {'last_date': last_date}})
	else:
		pass
	# if not(of):
	# 	print '\n'.join([','.join(edge) for edge in edges])
	# else:
	# 	open(of, 'w').write('\n'.join([','.join(edge) for edge in edges]))

def test_build_keyword_graph_single_folder():
	# db errors
	pass

def init_keyword_db_wrapper(end_date = '20130501'):
	'''Adds keywords and keyword edges for all papers starting from May 1, 2013.
	Only meant to initialize the database.
	'''
	news_sources = ['reuters']
	main_path = '/home/shankar/work/data/news/'
	db_conn = pm.Connection()
	dbhandle = db_conn['news']
	for source in news_sources:
		build_keyword_graph_single_folder(path=main_path + source + '/',\
										 dbhandle= dbhandle,\
										 conditions = [lambda x: x.endswith('.html'),\
										 	lambda x: x.split('.')[1] < end_date])
	db_conn.close()

def update_keyword_db(news_sources = [], logfile = '/tmp/dbstate.txt'):
	'''Updates the keyword and edge db with articles downloaded since the last state.
	'''
	main_path = '/home/shankar/work/data/news/'
	lf = open(logfile, 'w')
	if not(news_sources):
		news_sources = os.listdir(main_path)
		news_sources.remove('toi')
	db_conn = pm.Connection()
	dbhandle = db_conn['news']
	for source in news_sources:
		source_state = dbhandle['state'].find_one({'source': source})
		if (source_state):
			last_pid = source_state['last_pid']
			last_date = source_state['last_date']
		else:
			last_pid = 0
			last_date = '20130501'
		build_keyword_graph_single_folder(path=main_path + source + '/',\
										dbhandle = dbhandle,\
										conditions = [lambda x: x.split('.')[1] >= last_date,\
											lambda x: int(x.split('.')[0]) >= last_pid])
	lf.write('edges:%d, keywords:%d, articles:%d\n' %\
				 (dbhandle['edges'].count(),\
				  dbhandle['keywords'].count(),\
				  dbhandle.articles.count()))
	lf.close()
	db_conn.close()

def update_pid_db(news_sources = [], logfile = '/tmp/dbstate.txt'):
	'''Updates the pid-title db with articles downloaded since the last state.
	'''
	main_path = '/home/shankar/work/data/news/'
	if not(news_sources):
		news_sources = os.listdir(main_path)
	db_conn = pm.Connection()
	dbhandle = db_conn['news']
	for source in news_sources:
		source_state = dbhandle['state'].find_one({'source': source})
		if (source_state):
			if source_state.has_key('last_logged_line'):
				last_logged_line = source_state['last_logged_line']
			else:
				last_logged_line = 0
		else:
			last_logged_line = 0
		logfile = [f for f in os.listdir(main_path + source + '/')\
					if f.endswith('.log.txt')][0]
		loglines = open(main_path + source + '/' + logfile).readlines()
		if len(loglines) > last_logged_line:
			for line in loglines[last_logged_line:]:
				fields = line.strip().split('\t')
				try:
					pid = int(fields[0])
					date = dt.datetime.strptime(fields[1], '%a %b %d %H:%M:%S %Y')
					date = dt.datetime.strftime(date, '%Y%m%d')
					title = fields[2]
					dbhandle['articles'].insert({'source': source,\
												'pid': pid,\
												'date': date,
												'title': title})
				except:
					continue
		if not(dbhandle['state'].find({'source': source}).count()):
			dbhandle['state'].insert({'source': source,\
									'last_logged_line': len(loglines)})
		else:
			dbhandle['state'].update({'source': source},\
					{"$set": {'last_logged_line': len(loglines)}})
	db_conn.close()

if __name__ == '__main__':
	#sys.settrace(build_keyword_graph_single_folder)
	#test_build_keyword_graph_single_folder()
	#build_keyword_graph_single_folder(path='/home/shankar/work/data/news/reuters/', of='reuters-graph.csv')
	#test_find_frequent_coentities()
	#test_find_entity_timeseries()
	#init_keyword_db_wrapper()
	update_keyword_db()
	update_pid_db()