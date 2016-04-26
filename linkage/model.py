import os
import six
import logging
from hashlib import sha1
from collections import OrderedDict

import fingerprints

from sqlalchemy import MetaData, select, func, create_engine
from sqlalchemy import Unicode, Float
from sqlalchemy.sql.expression import cast
from sqlalchemy.schema import Table, Column

from linkage.exc import LinkageException

log = logging.getLogger(__name__)


class ViewTable(object):
    """A table to be joined in."""

    def __init__(self, config, view, data):
        self.config = config
        self.view = view
        if isinstance(data, six.string_types):
            data = {'table': data}
        self.data = data
        self.table_ref = data.get('table')
        self.alias = data.get('alias', self.table_ref)

    @property
    def table(self):
        table = Table(self.table_ref, self.config.meta, autoload=True)
        if self.alias != table.name:
            table = table.alias(self.alias)
        return table

    @property
    def refs(self):
        if not hasattr(self, '_refs'):
            self._refs = {}
            for column in self.table.columns:
                name = '%s.%s' % (self.alias, column.name)
                self._refs[name] = column
        return self._refs


class ViewField(object):
    """A field to be included in output reports."""

    def __init__(self, config, view, data):
        self.config = config
        self.view = view
        self.data = data
        self.column_ref = data.get('column')
        self.label = data.get('label', self.column_ref)
        self.key = data.get('key', False)
        self.column = view.get_column(self.column_ref)
        self.table = view.get_table(self.column_ref)

    def distinct(self, count=False):
        dist = func.distinct(self.column)
        if count:
            dist = func.count(dist)
        q = select(columns=[dist], from_obj=self.view.from_clause)
        q = self.view.apply_filters(q)
        q = q.where(self.column != None)  # noqa
        rp = self.config.engine.execute(q)
        while True:
            row = rp.fetchone()
            if not row:
                break
            yield row[0]


class View(object):
    """A view describes one set of data to be compared in cross-referencing."""

    def __init__(self, config, name, data):
        self.name = six.text_type(name)
        self.label = data.get('label', name)
        self.config = config
        self.data = data
        self.tables = [ViewTable(config, self, f)
                       for f in data.get('tables', [])]
        self.fields = [ViewField(config, self, f)
                       for f in data.get('fields', [])]
        self.key_fields = [f for f in self.fields if f.key]

        if not len(self.key_fields):
            raise LinkageException("No key column for: %r" % self.name)

    def get_column(self, ref):
        for table in self.tables:
            if ref in table.refs:
                return table.refs.get(ref)

    def get_table(self, ref):
        for table in self.tables:
            if ref == table.alias or ref in table.refs:
                return table

    @property
    def from_clause(self):
        return [t.table for t in self.tables]

    def apply_filters(self, q):
        for col, val in self.data.get('filters', {}).items():
            q = q.where(self.get_column(col) == val)
        for join in self.data.get('joins', []):
            left = self.get_column(join.get('left'))
            right = self.get_column(join.get('right'))
            q = q.where(left == right)
        return q

    @property
    def serial(self):
        if not hasattr(self, '_serial'):
            hashgen = sha1()
            hashgen.update(self.name.encode('utf-8'))
            for field in self.key_fields:
                hashgen.update(field.column_ref.encode('utf-8'))
            self._serial = hashgen.hexdigest()
        return self._serial

    def check_linktab(self):
        source_sum = 0
        for field in self.key_fields:
            for field_sum in field.distinct(count=True):
                source_sum += field_sum
        cnt = func.count(self.config.linktab.c.key)
        q = select(columns=[cnt], from_obj=self.config.linktab)
        q = q.where(self.config.linktab.c.view == self.name)
        q = q.where(self.config.linktab.c.serial == self.serial)
        rp = self.config.engine.execute(q)
        linktab_sum = rp.scalar()
        return source_sum == linktab_sum

    def generate_linktab(self, chunk_size=10000):
        with self.config.engine.begin() as connection:
            q = self.config.linktab.delete()
            q = q.where(self.config.linktab.c.view == self.name)
            connection.execute(q)
            for field in self.key_fields:
                self.generate_field_linktab(connection, field)

    def generate_field_linktab(self, connection, field, chunk_size=10000):
        chunk = []
        for i, value in enumerate(field.distinct()):
            fp = fingerprints.generate(value)
            # this is due to postgres' levenshtein
            fp = fp[:255]
            chunk.append({
                'view': self.name,
                'serial': self.serial,
                'key': value,
                'fingerprint': fp
            })
            if len(chunk) == chunk_size:
                log.info('Linktab %s (%s): %s',
                         self.name, field.column_ref, i + 1)
                connection.execute(self.config.linktab.insert(), chunk)
                chunk = []
        if len(chunk):
            connection.execute(self.config.linktab.insert(), chunk)


class CrossRef(object):
    """Try to match up records in two columns."""

    def __init__(self, config, left, right, left_key, right_key):
        self.config = config
        self.left = left
        self.right = right
        self.left_key = left_key
        self.right_key = right_key

    def query(self):
        tables = self.left.from_clause + self.right.from_clause
        left_lt = self.config.linktab.alias('__left_linktab')
        right_lt = self.config.linktab.alias('__right_linktab')
        tables += [left_lt, right_lt]

        columns = []
        score_length = func.greatest(func.length(left_lt.c.fingerprint),
                                     func.length(right_lt.c.fingerprint))
        score_leven = func.levenshtein(left_lt.c.fingerprint,
                                       right_lt.c.fingerprint)
        score_leven = cast(score_leven, Float)
        score = score_leven / score_length
        columns.append(score.label("Match Score"))

        for field in self.left.fields:
            label = '%s: %s' % (self.left.label, field.label)
            columns.append(field.column.label(label))
        for field in self.right.fields:
            label = '%s: %s' % (self.right.label, field.label)
            columns.append(field.column.label(label))

        q = select(columns=columns, from_obj=tables)
        q = self.left.apply_filters(q)
        q = self.right.apply_filters(q)
        q = q.where(left_lt.c.key == self.left_key.column)
        q = q.where(left_lt.c.view == self.left.name)
        q = q.where(right_lt.c.key == self.right_key.column)
        q = q.where(right_lt.c.view == self.right.name)

        # TODO: make this levenshteinable
        q = q.where(right_lt.c.fingerprint == left_lt.c.fingerprint)

        q = q.order_by(score.asc())
        return q

    def results(self):
        rp = self.config.engine.execute(self.query())
        while True:
            print rp.rowcount
            row = rp.fetchone()
            if not row:
                break
            yield row


class Linkage(object):
    """A full linkage configuration."""

    def __init__(self, data):
        self.data = data
        self.engine_url = os.path.expandvars(data.get('database'))
        self.engine = create_engine(self.engine_url)
        self.meta = MetaData()
        self.meta.bind = self.engine
        self.views = [View(self, n, v) for n, v in data.get('views').items()]
        self.linktab_name = data.get('linktab', '_linkage')

    @property
    def linktab(self):
        if not hasattr(self, '_linktab'):
            if self.engine.has_table(self.linktab_name):
                self._linktab = Table(self.linktab_name, self.meta,
                                      autoload=True)
            else:
                table = Table(self.linktab_name, self.meta)
                col = Column('view', Unicode, index=True)
                table.append_column(col)
                col = Column('serial', Unicode(40))
                table.append_column(col)
                col = Column('key', Unicode, index=True)
                table.append_column(col)
                col = Column('fingerprint', Unicode(255), index=True)
                table.append_column(col)
                table.create(self.engine)
                self._linktab = table
        return self._linktab

    @property
    def crossrefs(self):
        for left in self.views:
            for right in self.views:
                if left.name >= right.name:
                    continue
                for left_key in left.key_fields:
                    for right_key in right.key_fields:
                        yield CrossRef(self, left, right, left_key, right_key)
