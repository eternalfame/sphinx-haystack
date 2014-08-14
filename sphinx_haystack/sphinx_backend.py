# encoding: utf-8
import time
import datetime
import logging
import warnings
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.encoding import force_unicode

from haystack.backends import BaseEngine, BaseSearchBackend, SearchNode, BaseSearchQuery, log_query
from haystack.exceptions import MissingDependency, SearchBackendError
from haystack.utils import get_identifier
from haystack.constants import ID, DJANGO_CT, DJANGO_ID

try:
    import MySQLdb
    from MySQLdb.cursors import DictCursor
except ImportError:
    raise MissingDependency("The 'sphinx' backend requires the installation of 'MySQLdb'. Please refer to the documentation.")
try:
    # Pool connections if SQLAlchemy is present.
    import sqlalchemy.pool as pool
    # TODO: troubleshoot 'MySQL server has gone away'
    # For now disable connection pool.
    # MySQLdb = pool.manage(MySQLdb)
    connection_pooling = True
except ImportError:
    connection_pooling = False

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 9306


class SphinxSearchBackend(BaseSearchBackend):
    # https://github.com/dcramer/django-sphinx/tree/master/djangosphinx/
    def __init__(self, connection_alias, **connection_options):
        # TODO: determine the version number of Sphinx.
        # Parse from server banner "Server version: 1.10-dev (r2153)"
        super(SphinxSearchBackend, self).__init__(connection_alias, **connection_options)
        self.log = logging.getLogger('haystack')
        self.conn_kwargs = {
            'host': connection_options.get('HOST', DEFAULT_HOST),
            'port': connection_options.get('PORT', DEFAULT_PORT),
        }
        if self.conn_kwargs.get('host') == 'localhost':
            self.log.warning('Using the host \'localhost\' will connect via the MySQL socket. Sphinx listens on a TCP socket.')
        try:
            self.index_name = connection_options['INDEX_NAME']
        except KeyError, e:
            raise ImproperlyConfigured('Missing index name for sphinx-haystack. Please define %s.' % e.args[0])
        if not connection_pooling:
            self.log.warning('Connection pooling disabled for sphinx-haystack. Install SQLAlchemy.')

    def _from_python(self, value):
        if isinstance(value, datetime.datetime):
            value = time.mktime((value.year, value.month, value.day, value.hour, value.minute, value.second, 0, 0, 0))
        elif isinstance(value, bool):
            if value:
                value = 1
            else:
                value = 0
        elif isinstance(value, (list, tuple)):
            value = u','.join([force_unicode(v) for v in value])
        elif isinstance(value, (int, long, float)):
            # Leave it alone.
            pass
        else:
            value = force_unicode(value)
        return value

    def _connect(self):
        return MySQLdb.connect(**self.conn_kwargs)

    def update(self, index, iterable):
        """
        Issue a REPLACE INTO query to Sphinx. This will either insert or update
        a document in the index. If the document ID exists, an update is performed.
        Otherwise a new document is inserted.
        """
        values = []
        # TODO determine fields.
        fields, field_names = [], ['id']

        for name, field in index.fields.items():
            fields.append((name, field))
            field_names.append(name)

        for item in iterable:
            row = index.full_prepare(item)
            # Use item pk in Sphinx
            row['id'] = item.pk
            values.append([row[f] for f in field_names])
        conn = self._connect()
        try:
            curr = conn.cursor()
            # TODO: is executemany() better than many execute()s?
            curr.executemany('REPLACE INTO {0} ({1}) VALUES ({2})'.format(
                self.index_name,
                # Comma-separated list of field names
                ', '.join(field_names),
                # Comma-separated list of "%s", same number of them as field names.
                ', '.join(('%s', ) * len(field_names))
            ), values)
        finally:
            conn.close()

    def remove(self, obj):
        """
        Issue a DELETE query to Sphinx. Called on post_delete signal, so, object is already removed
        """
        conn = self._connect()
        try:
            # TODO: use a transaction to delete both atomically.
            curr = conn.cursor()
            curr.execute('DELETE FROM {0} WHERE id = %s'.format(self.index_name), (obj.pk, ))
        finally:
            conn.close()

    def clear(self, models=[], commit=True):
        """
        Clears all contents from index. This method iteratively gets a list of document
        ID numbers, then deletes them from the index. It does this in a while loop because
        Sphinx will limit the result set to 1,000.
        """
        # TODO: add sphinx version check and try to truncate index
        raise NotImplementedError('Sphinx rt index truncate is supported since 2.2.0')

    @log_query
    def search(self, query_string, sort_by=None, start_offset=0, end_offset=None,
               fields='', highlight=False, facets=None, date_facets=None, query_facets=None,
               narrow_queries=None, spelling_query=None, within=None,
               dwithin=None, distance_point=None,
               limit_to_registered_models=None, result_class=None, **kwargs):
        """
        Current implementation works only with MATCH, LIMIT and ORDER BY instructions.
        Returns list of ids
        TODO: Make support for facets etc
        TODO: Make support for getting and return final objects here
        TODO: Maybe rewrite with sphinxapi instead of mysqldb support?
        :param query_string:
        :param sort_by:
        :param start_offset:
        :param end_offset:
        :param fields:
        :param highlight:
        :param facets:
        :param date_facets:
        :param query_facets:
        :param narrow_queries:
        :param spelling_query:
        :param within:
        :param dwithin:
        :param distance_point:
        :param limit_to_registered_models:
        :param result_class:
        :param kwargs:
        :return:
        """
        # TODO: Quick workaround to build an sql string, need more work
        connector, params = query_string
        fulltext_search_params = ' AND '.join([x['value'] for x in params if x['type']=='fulltext'])
        usual_search_params = ' AND '.join([x['value'] for x in params if x['type']=='usual'])
        query = 'SELECT * FROM {0} WHERE MATCH(%s) {1} {2}'
        if sort_by:
            #print 'hERE', sort_by
            fields, reverse = [], None
            for field in sort_by:
                if field.startswith('-'):
                    field = field[1:]
                    if reverse == False:
                        raise SearchBackendError('Sphinx can only sort by ASC or DESC, not a mix of the two.')
                    reverse = True
                else:
                    if reverse == True:
                        raise SearchBackendError('Sphinx can only sort by ASC or DESC, not a mix of the two.')
                    reverse = False
                fields.append(field)
            query += ' ORDER BY {0}'.format(', '.join(fields))
            if reverse:
                query += ' DESC'
            else:
                query += 'ASC'
        if start_offset and end_offset:
            query += ' LIMIT {0}, {1}'.format(start_offset, end_offset)
        if end_offset:
            query += ' LIMIT {0}'.format(end_offset)
        query += ' OPTION ranker=sph04'
        conn = self._connect()
        try:
            curr = conn.cursor(DictCursor)
            rows = curr.execute(query.format(self.index_name, connector, usual_search_params), (fulltext_search_params, ))
        finally:
            conn.close()
        results = curr.fetchall()
        hits = len(results)
        return {
            'results': [r['id'] for r in results],
            'hits': hits,
        }

    def prep_value(self, value):
        return force_unicode(value)

    def more_like_this(self, model_instance, additional_query_string=None, result_class=None):
        raise NotImplementedError("Subclasses must provide a way to fetch similar record via the 'more_like_this' method if supported by the backend.")

    def extract_file_contents(self, file_obj):
        raise NotImplementedError("Subclasses must provide a way to extract metadata via the 'extract' method if supported by the backend.")

    def build_schema(self, fields):
        raise NotImplementedError("Subclasses must provide a way to build their schema.")


class SphinxSearchQuery(BaseSearchQuery):

    def build_params(self, *args, **kwargs):
        params = super(SphinxSearchQuery, self).build_params(*args, **kwargs)
        return params

    def build_query(self):
        if not self.query_filter:
            return u''  #  Shall we match all or none?
        else:
            return self._make_query_from_search_node(self.query_filter)


    def _make_query_from_search_node(self, search_node, is_not=False):
        query_list = []
        # Let's treat all except 'content' var as non-fulltext fields
        for child in self.query_filter.children:
            if isinstance(child, SearchNode):
                query_list.append(
                    self._make_query_from_search_node(child, child.negated)
                )
            else:
                expression, term = child
                field_name, filter_type = search_node.split_expression(expression)
                constructed_query = self._query_from_term(term, field_name, filter_type, is_not)
                query_list.append(constructed_query)

        return (search_node.connector, query_list)

    def _query_from_term(self, term, field_name, filter_type, is_not):
        # TODO: check for fulltext fields (from index definition)
        filter_types = {
            'contains': u'{0} {1}',
            'startswith': u'{0}^{1}',
            'exact': u'{0}={1}',
        }
        if field_name == 'content':
            # Full text search
            query = {
                'type': 'fulltext',
                'value': u'{} {}'.format(
                    'NOT' if is_not else '',
                    filter_types[filter_type].format('@*',term)).strip()
            }
        else:
            query = {
                'type': 'usual',
                'value': u'{} {}'.format(
                    'NOT' if is_not else '',
                    filter_types[filter_type].format(field_name,term)).strip()
            }

        return query

class SphinxEngine(BaseEngine):
    backend = SphinxSearchBackend
    query = SphinxSearchQuery
