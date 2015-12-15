# encoding: utf-8
import time
import sys
import math
import re
import datetime
import logging
import warnings
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ImproperlyConfigured
from django.utils.encoding import force_unicode
from django.utils import six
try:
    from django.utils.encoding import force_text
except ImportError:
    from django.utils.encoding import force_unicode as force_text

from haystack.backends import BaseEngine, BaseSearchBackend, SearchNode, BaseSearchQuery, log_query, SearchResult
from haystack.exceptions import MissingDependency, SearchBackendError
from haystack.constants import VALID_FILTERS, FILTER_SEPARATOR, DEFAULT_ALIAS
from haystack import connections

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
        if connection_options.get('UNIX_SOCKET'):
            self.conn_kwargs = {
                'unix_socket': connection_options['UNIX_SOCKET']
            }
        else:
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
        conn = MySQLdb.connect(**self.conn_kwargs)
        conn.set_character_set('utf8')
        return conn

    def update(self, index, iterable, commit=None):
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
        Issue a DELETE query to Sphinx. Doesn't check if object is already removed
        """
        conn = self._connect()
        try:
            curr = conn.cursor()
            curr.execute('DELETE FROM {0} WHERE id = %s'.format(self.index_name), (obj.pk, ))
        finally:
            conn.close()

    def clear(self, models=[], commit=True):
        """
        Clears all contents from index.
        """
        conn = self._connect()
        #TODO: if conn._server_version < (2,1) make a while loop to delete whole index
        try:
            curr = conn.cursor(DictCursor)
            result = curr.execute('TRUNCATE RTINDEX {0}'.format(self.index_name))
        except Exception as e:
            self.log.error(str(e))
            raise e
        finally:
            conn.close()
        return True

    @log_query
    def search(self, query_string, sort_by=None, start_offset=0, end_offset=None,
               fields='', highlight=False, facets=None, date_facets=None, query_facets=None,
               narrow_queries=None, spelling_query=None, within=None,
               dwithin=None, distance_point=None,
               limit_to_registered_models=None, result_class=None, **kwargs):
        """
        Current implementation
        Made support for some spatial functions e.g. dwithin
        Return list of SearchResult objects
        TODO: Support multiple full-text queries
        TODO: Make support for facets etc

        :param query_string: Tuple from query builder, first element is query string and second is full-text query params
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
        query_string, query_params = query_string
        selectors = self._get_fields() if not fields else fields.split(',')
        query = 'SELECT {0} FROM {1} WHERE {2}'

        if dwithin:
            # Since sphinx does not support single field coordinates,
            # we suggest that we have (latitude, longitude) pair of fields in index
            # and return is_near as boolean parameter
            latitude, longitude = dwithin['point'].get_coords()
            selectors.append('GEODIST({0}, {1}, latitude, longitude) < {2} as is_near'.format(
                math.radians(latitude),
                math.radians(longitude),
                dwithin['distance'].m
            ))

        if sort_by:
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
                query += ' ASC'
        if start_offset and end_offset:
            query += ' LIMIT {0}, {1}'.format(start_offset, end_offset)
        if end_offset:
            query += ' LIMIT {0}'.format(end_offset)
        query += ' OPTION ranker=sph04'
        query_params = (query_params, ) if query_params else tuple()
        conn = self._connect()
        try:
            curr = conn.cursor(DictCursor)
            rows = curr.execute(query.format(', '.join(selectors), self.index_name, query_string).replace('text', '*'), query_params)
        except Exception as e:
            self.log.error(str(e))
            raise e
        finally:
            conn.close()
        results = curr.fetchall()
        return self._process_results(
            results,
            result_class=result_class,
        )


    def _get_fields(self):
        model, app_label, model_name = self._get_indexed_model()
        unified_index = connections[self.connection_alias].get_unified_index()
        return unified_index.indexes[model].fields.keys()


    def _get_indexed_model(self):
        model, app_label, model_name = None, '', ''

        unified_index = connections[self.connection_alias].get_unified_index()
        indexed_models = unified_index.get_indexed_models()

        # XXX: Name convention - index named as modelname_index
        index_model_name = self.index_name.split('_')[0]
        for model in indexed_models:
            if model.__name__.lower() == index_model_name:
                ct = ContentType.objects.get_for_model(model)
                app_label = ct.app_label
                model_name = model.__name__
                break
        return model, app_label, model_name

    def _process_results(self, raw_results, result_class=None):
        if not result_class:
            result_class = SearchResult

        hits = len(raw_results) or 0

        index_model, app_label, model_name = self._get_indexed_model()

        results = []
        for result in raw_results:
            results.append(result_class(
                app_label, model_name, result['id'], 0, **result
            ))
        return {
            'results': results,
            'hits': 0
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

    def _escape_string(self, string):
        return re.sub(u"([=\(\)|\-!@~\"&/\\\^\$\=])", u"\\\1", string)

    def _repr_query_fragment_callback(self, field, filter_type, value):
        if six.PY3:
            value = force_text(value)
        else:
            value = force_text(value).encode('utf8')

        return u"%s%s%s=%s" % (field, FILTER_SEPARATOR, filter_type, value.encode('utf-8'))

    def _make_query_from_search_node(self, search_node, is_not=False):
        ft_list = []
        query_list = []
        # Let's treat all except 'content' var as non-fulltext fields
        for child in search_node.children:
            if isinstance(child, SearchNode):
                query, ft = self._make_query_from_search_node(child, child.negated)
                query_list.append(query)
                ft_list.append(ft or [])
            else:
                expression, term = child
                field_name, filter_type = search_node.split_expression(expression)
                query, ft = self._query_from_term(term, field_name, filter_type, is_not)
                query_list.append(query)
                ft_list.append(ft or [])
        return u' {} '.format(search_node.connector).join(query_list), u' '.join(filter(None, ft_list))

    def _query_from_term(self, term, field_name, filter_type, is_not):
        # TODO: check for fulltext fields (from index definition)
        filter_types = {
            'contains': u'{0} {1}',
            'not_contains': u'{0} -{1}',
            'startswith': u'{0} ^{1}',
            'endswith': u'{0} {1}$',
            'exact': u'{0}={1}',
            'not_exact': u'{0}!={1}',
            'in': u'{0} IN ({1})',
            'not_in': u'{0} NOT IN ({1})'
        }
        filter_type = 'not_{}'.format(filter_type) if is_not else filter_type
        if field_name == 'content':
            # Full text search
            if isinstance(term, basestring):
                # Escape string value
                term = self._escape_string(term)
            else:
                # Call prepare() method of haystack Input type since it's not a string
                try:
                    term = term.prepare()
                except:
                    pass
            query = {
                'type': 'fulltext',
                'value': u'{}'.format(
                    filter_types[filter_type].format(u'@*', term)).strip()
            }
        else:
            if isinstance(term, list):
                term = u','.join(map(lambda i: str(i), term))
            elif isinstance(term, bool):
                term = int(term)
            query = {
                'type': 'usual',
                'value': u'{}'.format(
                    filter_types[filter_type].format(field_name, term)).strip()
            }

        if query['type'] == 'fulltext':
            return (u'MATCH(%s)', query['value'])
        else:
            return (query['value'], None)

class SphinxEngine(BaseEngine):
    backend = SphinxSearchBackend
    query = SphinxSearchQuery