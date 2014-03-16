from collections import defaultdict as dd
import datetime as dt
import newsutils as nu
import textutils as tu
import os
import sys
import re
from itertools import chain
import pymongo as pm

boul = lambda x: 1 if x else 0
tcmp = lambda x,y: -1 if x[1] < y[1] else boul(y[1] < x[1])
strtodate = lambda date: dt.datetime.strptime(date, '%Y%m%d')
datetostr = lambda date: dt.datetime.strftime(date, '%Y-%m-%d')

def find_edges_with_entity(entity, unique = False, filehandle='', dbhandle=''):
	'''Returns all edges in the keywords graph that contain the entity.
	@param entity
	@param unique if True, return set of distinct co-occurring keywords
	'''
	if filehandle:
		all_edges = [l.strip() for l in open(filehandle).readlines()]
		edges_with_entity = [edge.split(',') for edge in all_edges\
								 if edge.startswith(entity)]
		edges_with_entity.extend([edge.split(',') for edge in all_edges\
								 if edge.split(',')[1].startswith(entity)])
	elif dbhandle:
		kwe = dbhandle['edges']
		edges_with_entity = [{'keyword1': edge['keyword2'],\
							'keyword2': edge['keyword1'],\
							'source': edge['source'],\
							'pid': edge['pid'],\
							'date': edge['date']}\
							 for edge in\
							 kwe.find({'keyword2': re.compile(entity)})]
		edges_with_entity.extend([{'keyword1': edge['keyword1'],\
							'keyword2': edge['keyword2'],\
							'source': edge['source'],\
							'pid': edge['pid'],\
							'date': edge['date']}\
							 for edge in\
							 kwe.find({'keyword1': re.compile(entity)})])
		if unique:
			edges_with_entity = set(edges_with_entity)
	else:
		edges_with_entity = []
	return edges_with_entity

def test_find_edges_with_entity_dbhandle():
	'''DB tests.'''
	db_conn = pm.Connection()
	dbhandle = db_conn['news']
	# non-unique
	coen = find_edges_with_entity(entity='irs', dbhandle=dbhandle)
	coen_hist = dd(int)
	for e in coen: coen_hist[e['keyword2']] += 1
	coen_hist = coen_hist.items()
	coen_hist.sort(tcmp)
	print '\n'.join(['%s: %d' % (item[0], item[1]) for item in coen_hist])

def test_find_edge_timeseries_dbhandle():
	'''DB tests for returning timeseries containing entity.'''
	db_conn = pm.Connection()
	dbhandle = db_conn['news']
	edges_with_entity = find_edges_with_entity(entity='immigration', dbhandle=dbhandle)
	edge_timeseries = [(edge['source'], edge['date'],\
						 edge['keyword1'], edge['keyword2'])\
						  for edge in edges_with_entity]
	edge_timeseries.sort(tcmp)
	print '\n'.join([','.join([str(i.encode('utf8')) for i in edge])\
					 for edge in edge_timeseries])

def plot_cooccurring_entitytimeseries_dbhandle():
	db_conn = pm.Connection()
	dbhandle = db_conn['news']
	

def plot_multiple_entitytimeseries_dbhandle(entities = [], output_file = 'immigration.html'):
	'''Plots timeseries for multiple entities.
	'''
	db_conn = pm.Connection()
	dbhandle = db_conn['news']
	from nvd3 import lineChart
	chart = lineChart(name='lineChart', height=400, width=700, date=True)
	for entity in entities:
		edges_with_entity = find_edges_with_entity(entity=entity, dbhandle=dbhandle)
		ts_dict = dd(int)
		for e in edges_with_entity: ts_dict[strtodate(e['date']).strftime('%s')] += 1
		ts_list = [(ts_dict[k], int(k)*1000) for k in ts_dict]
		ts_list.sort(tcmp)
		xdata = [i[1] for i in ts_list]
		ydata = [i[0] for i in ts_list]
		extra_serie = {"tooltip": {"y_start": "", "y_end": " mentions"}}
		chart.add_serie(name=entity, y=ydata, x=xdata, extra=extra_serie)

	of = open(output_file, 'w')
	chart.buildhtml()
	of.write(chart.htmlcontent)
	of.close()

def test_plot_multiple_entitytimeseries_dbhandle():
	plot_multiple_entitytimeseries_dbhandle(['oklahoma', 'sandy'], output_file='viz.html')


def find_cooccurring_entities(entity1, entity2, filehandle = '', dbhandle = ''):
	'''Finds edges with (entity1, entity2) or (entity2, entity1).

	Returns list of (entity1, entity2, source, date, pid) tuples.
	'''
	if filehandle:
		pass
	elif dbhandle:
		kwe = dbhandle['edges']
		articles = dbhandle['articles']
		matching_edges = [(item['keyword1'], item['keyword2'],\
				  item['source'], item['date'], item['pid'],\
				  articles.find_one({'pid': long(item['pid'])})['title'])\
				 for item in kwe.find({'$or': [{'keyword1': re.compile(entity1),\
								 'keyword2': re.compile(entity2)},\
								{'keyword2': re.compile(entity1),\
								 'keyword1': re.compile(entity2)}]})]
	else:
		matching_edges = []
	return matching_edges

def test_find_cooccurring_entities():
	db_conn = pm.Connection()
	dbhandle = db_conn['news']
	edges = find_cooccurring_entities(entity1='irs', entity2='rubio',\
			 dbhandle=dbhandle)
	print '\n'.join([','.join(item) for item in edges])


def find_entity_timeseries(entity, filehandle = '', dbhandle = ''):
	'''Retrieves a chronological list of entities. An element in the timeseries
	comprises id, time, source. Items are sorted in order of (date, pid)

	@param entity
	@param filehandle
	@param dbhandle
	'''
	entity = tu.strip_whitespace(entity.lower())
	edges_with_entity = find_edges_with_entity(entity, filehandle=filehandle, dbhandle = dbhandle)
	timeseries = []
	for edge in edges_with_entity:
		date, source, pid = edge[2], edge[3], edge[4]
		date = strtodate(date)
		pid = int(pid)
		timeseries.append((entity, date, pid, source))
	timeseries = list(set(timeseries)) # unique
	timeseries.sort(tcmp) # TODO: sort by pid also
	return timeseries

def test_find_entity_timeseries():
	ts = find_entity_timeseries('immigration', filehandle = 'total.csv')
	print '\n'.join(['immigration:%s, %d, %s' % (datetostr(item[1]), item[2], item[0])\
					 for item in ts])

def find_numerical_sentences(precontext=1, postcontext=1, dbhandle = '', filename = '', pid = '', date='', source=''):
	'''Retrieves all sentences with numerical values in them, also provides\
	precontext and postcontext.
	@param precontext number of sentences before numerical sentence.
	@param postcontext number of sentences after numerical sentence.
	@param filename
	@param dbhandle
	'''
	sentences = tu.get_sentences(tu.get_body(html=open(filename).read()))
	npat = '[0-9]+\.?[0-9]+'
	numerical_sentences = []
	for l in enumerate(sentences):
		numbers = re.findall(npat, l[1])
		context = {}
		if numbers:
			context['sentence'] = l[1]
			#TODO: [{entity:(number, measurement unit (%, km, Rs?), category (time?))}]
			context['num'] = []
			context['prolog'] = [sentences[l[0]-1-b] for b in range(precontext)\
								 if (l[0]-1-b)]
			context['epilog'] = [sentences[l[0]+1+a] for a in range(postcontext)\
								 if (l[0]+1+a < len(sentences))]
			numerical_sentences.append(context)
	if dbhandle:
		trends_coll = dbhandle['trends']
		trends_coll.insert({'date': date, 'source': source, 'pid': pid,\
			'numbers': numerical_sentences})
	return numerical_sentences
	# return [(l[1],\
	#  [sentences[l[0]-1-b] for b in range(precontext) if (l[0]-1-b)],
	#  [sentences[l[0]+1+a] for a in range(postcontext) if (l[0]+1+a < len(sentences))])\
	# 	for l in enumerate(sentences) if re.findall(npat, l[1])]

def test_find_numerical_sentences():
	# boundary conditions
	# db
	# sanity
	numerical_sentences = find_numerical_sentences(filename='foo.html')
	for i in numerical_sentences:
		if (len(i['prolog']) & len(i['epilog'])):
			print 'num: %s\nBEFORE: %s\nAFTER: %s\n' %\
			 (i['sentence'], i['prolog'][0], i['epilog'][0])
		else:
			print 'num: %s\n' % i['sentence']

def find_frequent_coentities(entity, threshold = 50, filehandle = '', dbhandle = ''):
	'''Retrieves commonly occurring entities along with a given entity.
	@param entity
	@param threshold how many coentities to return
	@param conditions list of boolean functions to apply. each elem is (fun, args)
	@param filehandle
	@param dbhandle
	'''
	entity = tu.strip_whitespace(entity.lower())
	coentities = dd(int)
	if filehandle:
		#assume CSV (k1, k2, date, id)
		all_edges = [l.strip() for l in open(filehandle).readlines()]
		edges_with_entity = [edge.split(',') for edge in all_edges\
								 if edge.startswith(entity)]
		edges_with_entity.extend([edge.split(',') for edge in all_edges\
								 if len(edge.split(',')) &\
								 	edge.split(',')[1].startswith(entity)])
		for edge in edges_with_entity:
			if (edge[0].startswith(entity)):
				coentities[edge[1]] += 1
			else:
				coentities[edge[0]] += 1
	elif dbhandle:
		edges_with_entity = find_edges_with_entity(entity, dbhandle=dbhandle,\
		 unique=False)
		for edge in edges_with_entity:
			if edge['keyword1'].startswith(entity):
				coentities[edge['keyword1']] += 1
			else:
				coentities[edge['keyword2']] += 1
			pass
	else:
		return None
	coentities = coentities.items()
	coentities.sort(tcmp)
	coentities.reverse()
	return coentities[:(threshold+1)]

def test_find_frequent_coentities():
	# file, no db
	coentities = find_frequent_coentities(entity='economic policy', filehandle='total.csv')
	# timeseries
	ts = find_entity_timeseries(entity='economic policy', filehandle='total.csv')
	for entity in coentities:
		ts.extend(find_entity_timeseries(entity=entity[0], filehandle='total.csv'))
	ts.sort(tcmp)
	print '\n'.join(['%s,%s,%d,%s' %\
				 	(item[0], datetostr(item[1]), item[2], item[3])\
				 	for item in ts])
	# print '\n'.join(['%s:%d' % (e[0], e[1]) for e in coentities])

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

def macro_get_articles_from_db(keyword=''):
	'''Macro function only to be used from command line. Returns a 
	chronological-sorted list of (date,pid,source,title) items from articles
	containing keyword occurring either as keyword or as title.
	@param keyword
	'''
	if keyword:
		c = pm.Connection()
		db = c.news
		edges = db.edges
		articles = db.articles
		# find pids of articles containing keywords
		l_pids = [item['pid'] for item in edges.find({'$or': \
												[{'keyword1': re.compile(keyword)},\
												{'keyword2': re.compile(keyword)}]}
												)]
		l_pids.extend([item['pid'] for item in articles.find({'title':\
												re.compile(keyword, re.IGNORECASE)
												})])
		l_pids = set(l_pids)
		a_pids = [articles.find_one({'pid': int(pid)}) for pid in l_pids]
		l = [(i['date'], i['pid'], i['source'], i['title']) for i in a_pids if i]
		l.sort()
		print '\n'.join(['%s,%d,%s: %s' % i for i in l])
		return l
	else:
		print 'No keyword given.'
		pass

if __name__ == '__main__':
	#sys.settrace(build_keyword_graph_single_folder)
	#test_build_keyword_graph_single_folder()
	#build_keyword_graph_single_folder(path='/home/shankar/work/data/news/reuters/', of='reuters-graph.csv')
	#test_find_frequent_coentities()
	#test_find_entity_timeseries()
	#init_keyword_db_wrapper()
	#update_keyword_db()
	#test_find_edges_with_entity_dbhandle()
	#test_find_edge_timeseries_dbhandle()
	#test_find_cooccurring_entities()
	#test_plot_multiple_entitytimeseries_dbhandle()
	#update_pid_db(news_sources=['firstpostin', 'reuters', 'nytimes'])
	test_find_numerical_sentences()