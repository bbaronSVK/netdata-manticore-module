# -*- coding: utf-8 -*-
# Description: Manticore netdata python.d module
# Author: Michal Paulovic (bbaron)
# SPDX-License-Identifier: GPL-3.0-or-later

from bases.FrameworkServices.SimpleService import SimpleService
from re import sub as replace
import json
import copy
from sys import exc_info

try:
    import MySQLdb

    PY_MYSQL = True
except ImportError:
    try:
        import pymysql as MySQLdb

        PY_MYSQL = True
    except ImportError:
        PY_MYSQL = False


QUERY_STATUS = 'SHOW STATUS'

STATUS_STATS = [
    'uptime',
    'connections',
    'maxed_out',
    'command_search',
    'command_excerpt',
    'command_update',
    'command_delete',
    'command_keywords',
    'command_persist',
    'command_status',
    'command_flushattrs',
    'command_set',
    'command_insert',
    'command_replace',
    'command_commit',
    'command_suggest',
    'command_json',
    'agent_connect',
    'agent_retry',
    'queries',
    'dist_queries',
    'query_wall',
    'query_cpu',
    'dist_wall',
    'dist_local',
    'dist_wait',
    'query_reads',
    'query_readkb',
    'query_readtime',
    'avg_query_wall',
    'avg_query_cpu',
    'avg_dist_wall',
    'avg_dist_local',
    'avg_dist_wait',
    'avg_query_reads',
    'avg_query_readkb',
    'avg_query_readtime',
    'qcache_max_bytes',
    'qcache_thresh_msec',
    'qcache_ttl_sec',
    'qcache_cached_queries',
    'qcache_used_bytes',
    'qcache_hits'
]


ORDER = [
    'commands',
    'queries',
    'uptime',
    'connections',
    'query_wall',
    'query_cpu',
    'query_reads',
    'query_readkb',
    'query_readtime',
    'indexed_bytes',
    'indexed_documents',
    'ram_bytes',
    'disk_bytes'
]

CHARTS = {
    'uptime': {
        'options': [None, 'Uptime', 's', 'uptime', '', 'line'],
        'lines': [
            ['uptime', 'uptime', 'absolute']
        ]
    },
    'connections': {
        'options': [None, 'Connections', 'connections', 'connections', '', 'line'],
        'lines': [
            ['connections', 'connections', 'incremental'],
            ['maxed_out', 'dismissing client', 'incremental']
        ]
    },
    'commands': {
        'options': [None, 'Commands', 'commands/s', 'commands', '', 'line'],
        'lines': [
            ['command_search', 'search', 'incremental'],
            ['command_excerpt', 'excerpt', 'incremental'],
            ['command_update', 'update', 'incremental'],
            ['command_delete', 'delete', 'incremental'],
            ['command_keywords', 'keywords', 'incremental'],
            ['command_persist', 'persist', 'incremental'],
            ['command_status', 'status', 'incremental'],
            ['command_flushattrs', 'flushattrs', 'incremental'],
            ['command_set', 'set', 'incremental'],
            ['command_insert', 'insert', 'incremental'],
            ['command_replace', 'replace', 'incremental'],
            ['command_commit', 'commit', 'incremental'],
            ['command_suggest', 'suggest', 'incremental'],
            ['command_json', 'json', 'incremental']
        ]
    },
    'queries': {
        'options': [None, 'Queries', 'queries/s', 'queries', '', 'line'],
        'lines': [
            ['queries', 'Queries', 'incremental'],
            ['dist_queries', 'Distributed Queries', 'incremental'],
        ]
    },
    'query_wall': {
        'options': [None, 'Query wall', '', 'query_wall', '', 'line'],
        'lines': [
            ['query_wall', 'Query wall', 'incremental'],
            ['avg_query_wall', 'avg. query wall', 'absolute'],
        ]
    },
    'query_cpu': {
        'options': [None, 'Query CPU', 'CPU/s', 'query_cpu', '', 'line'],
        'lines': [
            ['query_cpu', 'Query CPU', 'incremental'],
            ['avg_query_cpu', 'avg. Query CPU', 'absolute'],
        ]
    },
    'query_reads': {
        'options': [None, 'Query reads', 'reads/s', 'query_reads', '', 'line'],
        'lines': [
            ['query_reads', 'Query reads', 'incremental'],
            ['avg_query_reads', 'avg. query reads', 'absolute']
        ]
    },
    'query_readkb': {
        'options': [None, 'Query read KB', 'KB/s', 'query_readkb', '', 'line'],
        'lines': [
            ['query_readkb', 'Query read KB', 'incremental'],
            ['avg_query_readkb', 'avg. query read KB', 'absolute']
        ]
    },
    'query_readtime': {
        'options': [None, 'Query read time', 's', 'query_readtime', '', 'line'],
        'lines': [
            ['query_readtime', 'Query read time', 'incremental'],
            ['avg_query_readtime', 'avg. query read time', 'absolute']
        ]
    }
}

CHARTS_TPL = {
    'indexed_bytes': {
        'options': [None, 'Index size', 'B', 'indexed_bytes', '', 'stacked'],
        'lines':[
            [None, None, 'absolute']
        ]
    },
    'indexed_documents': {
        'options': [None, 'Indexed documents', 'doc', 'indexed_documents', '', 'stacked'],
        'lines': [
            [None, None, 'absolute']
        ]
    },
    'ram_bytes': {
        'options': [None, 'RAM bytes', 'B', 'ram_bytes', '', 'stacked'],
        'lines': [
            [None, None, 'absolute']
        ]
    },
    'disk_bytes': {
        'options': [None, 'Disk bytes', 'B', 'disk_bytes', '', 'stacked'],
        'lines': [
            [None, None, 'absolute']
        ]
    }
}

CHARTS_INDEX_TPL = {
#    'query_time_1min': {
#        'options': [None, 'Query time 1min ({0})', 'quqey/1min', '', 'line'],
#        'lines': [
#            [None, None, 'absolute']
#        ]
#    },
#    'query_time_5min': {
#        'options': [None, 'Query time 5min ({0})', 'query/5min', '', 'line'],
#        'lines': [
#            [None, None, 'absolute']
#        ]
#    },
#    'query_time_15min': {
#        'options': [None, 'Query time 15min ({0})', 'query/15min', '', 'line'],
#        'lines': [
#            [None, None, 'absolute']
#        ]
#    },
    'query_time_total': {
        'options': [None, 'Query time total ({0})', 'query/total', '', '', 'line'],
        'lines': [
            [None, None, 'absolute']
        ]
    },
#    'found_rows_1min': {
#        'options': [None, 'Found rows 1min ({0})', '', '', 'line'],
#        'lines': [
#            [None, None, 'absolute']
#        ]
#    },
#    'found_rows_5min': {
#        'options': [None, 'Found rows 5min ({0})', '', '', 'line'],
#        'lines': [
#            [None, None, 'absolute']
#        ]
#    },
#    'found_rows_15min': {
#        'options': [None, 'Found rows 15min ({0})', '', '', 'line'],
#        'lines': [
#            [None, None, 'absolute']
#        ]
#    },
    'found_rows_total': {
        'options': [None, 'Found rows total ({0})', '', '', '', 'line'],
        'lines': [
            [None, None, 'absolute']
        ]
    }
}

INDEX_LINES = [
    'queries',
    'avg_sec',
    'min_sec',
    'max_sec',
    'pct95_sec',
    'pct99_sec',
    'avg',
    'min',
    'max',
    'pct95',
    'pct99'
]

LINES = [
    'indexed_documents',
    'indexed_bytes',
    'ram_bytes',
    'disk_bytes'
]

class Service(SimpleService):
    def __init__(self, configuration=None, name=None):
	SimpleService.__init__(self, configuration=configuration, name=name)
	self.configuration = configuration
        self.order = ORDER
        self.definitions = CHARTS
	self.__connection = None
	self.__conn_properties = self.get_connection_properties(self.configuration)
        self.queries = dict(global_stats=QUERY_STATUS)
	self.tables = list()

    def get_connection_properties(self, conf):
	properties = dict()
	if conf.get('user'):
	    properties['user'] = conf['user']
	if conf.get('pass'):
	    properties['passwd'] = conf['pass']
	if conf.get('socket'):
	    properties['unix_socket'] = conf['socket']
	elif conf.get('host'):
	    properties['host'] = conf['host']
	    properties['port'] = int(conf.get('port', 3306))
	elif conf.get('my.cnf'):
	    if MySQLdb.__name__ == 'pymysql':
		self.error('"my.cnf" parsing is not working for pymysql')
	    else:
		properties['read_default_file'] = conf['my.cnf']

	return properties or None

    def __connect(self):
        try:
            connection = MySQLdb.connect(connect_timeout=self.update_every, **self.__conn_properties)
        except (MySQLdb.MySQLError, TypeError, AttributeError) as error:
            return None, str(error)
        else:
            return connection, None

    def _get_raw_data(self, queries=None, description=None):
        """
        Get raw data from manticore server
        :return: dict: fetchall() or (fetchall(), description)
        """

        if not self.__connection:
            self.__connection, error = self.__connect()
            if error:
                return None

        raw_data = dict()
        queries = dict(queries) if queries is not None else dict(self.queries)
        try:
            with self.__connection as cursor:
                for name, query in queries.items():
                    try:
                        cursor.execute(query)
                    except (MySQLdb.ProgrammingError, MySQLdb.OperationalError) as error:
                        if self.__is_error_critical(err_class=exc_info()[0], err_text=str(error)):
                            raise RuntimeError
                        self.error('Removed query: {name}[{query}]. Error: error'.format(name=name,
                                                                                         query=query,
                                                                                         error=error))
                        self.queries.pop(name)
                        continue
                    else:
                        raw_data[name] = (cursor.fetchall(), cursor.description) if description else cursor.fetchall()
            self.__connection.commit()
        except (MySQLdb.MySQLError, RuntimeError, TypeError, AttributeError):
            self.__connection.close()
            self.__connection = None
            return None
        else:
            return raw_data or None

    def check(self):
        if not PY_MYSQL:
            self.error('MySQLdb or PyMySQL module is needed to use manticore.chart.py plugin')
            return False

	raw_data = self._get_raw_data(queries={'tables':'SHOW TABLES'}, description=True)
	if not raw_data or 'tables' not in raw_data:
	    return True

	self.tables = list()
	for table in raw_data['tables'][0]:
	    self.tables.append(str(table[0]))
        if not self.tables:
            return True

        for name in LINES:
            chart = {'options': CHARTS_TPL[name]['options'], 'lines':[]}
            self.definitions[name] = chart
            for table in self.tables:
                line = list(CHARTS_TPL[name]['lines'][0])
                line[0] = '{name}_{table}'.format(name=name,table=table)
                line[1] = table
                self.definitions[name]['lines'].append(line)
                self.queries.update({'table_{0}'.format(table): 'SHOW INDEX {0} STATUS'.format(table)})
            del self.definitions[name]['lines'][0]
        
        for chart,opt in CHARTS_INDEX_TPL.items():
            for table in self.tables:
                name = '{0}_{1}'.format(table, chart)
                tempOpt = copy.deepcopy(opt)
                tempOpt['options'][1] = tempOpt['options'][1].format(table)
                self.definitions[name] = dict({'options':tempOpt['options'],'lines':[]})
                self.order.append(name)
                for lname in INDEX_LINES:
                    line = list(CHARTS_INDEX_TPL[chart]['lines'][0])
                    line[0] = '{0}_{1}'.format(name, lname)
                    line[1] = lname
                    self.definitions[name]['lines'].append(line)
        return True

    def _get_data(self):

        raw_data = self._get_raw_data(description=True)

        if not raw_data:
            return None

        to_netdata = dict()

        if 'global_stats' in raw_data:
            global_stats = dict(raw_data['global_stats'][0])
            for key in STATUS_STATS:
                if key in global_stats:
                    to_netdata[key] = float(global_stats[key])
	
	for table in self.tables:
            name = 'table_{0}'.format(table)
	    if name in raw_data:
		index_data = raw_data[name][0]
		for data in index_data:
		    if data[0] in LINES:
			to_netdata['{0}_{1}'.format(data[0], table)] = float(data[1])
                    if 'query_time' in data[0] or 'found_row' in data[0]:
                        tmp = json.loads(data[1])
                        chart = data[0]
                        for lname in INDEX_LINES:
                            if lname in tmp:
                                name = '{0}_{1}_{2}'.format(table, chart, lname)
                                to_netdata[name] = float(tmp[lname]) if tmp[lname] != '-' else 0 
        return to_netdata or None
