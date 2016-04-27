import os
import six
import logging
from normality import slugify
from hashlib import sha1
from collections import OrderedDict

import fingerprints

from sqlalchemy import MetaData, select, func, create_engine
from sqlalchemy import Unicode, Float
from sqlalchemy.sql.expression import cast
from sqlalchemy.schema import Table, Column, Index

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
        self.column = view.get_column(self.column_ref)
        self.table = view.get_table(self.column_ref)


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
        self.key_ref = data.get('key')

    def get_column(self, ref):
        for table in self.tables:
            if ref in table.refs:
                return table.refs.get(ref)
        raise LinkageException("Cannot find column: %s" % ref)

    def get_table(self, ref):
        for table in self.tables:
            if ref == table.alias or ref in table.refs:
                return table
        raise LinkageException("Cannot find table: %s" % ref)

    @property
    def key(self):
        return self.get_column(self.key_ref)

    @property
    def index_name(self):
        return 'ix_%s_%s' % (slugify(self.name, '_'),
                             slugify(self.key_ref, '_'))

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
            hashgen.update(self.key_ref.encode('utf-8'))
            self._serial = hashgen.hexdigest()
        return unicode(self._serial)

    def distinct_key(self):
        dist = func.distinct(self.key)
        q = select(columns=[dist], from_obj=self.from_clause)
        q = self.apply_filters(q)
        q = q.where(self.key != None)  # noqa
        rp = self.config.engine.execute(q)
        while True:
            row = rp.fetchone()
            if not row:
                break
            yield row[0]

    def check_linktab(self):
        cnt = func.count(self.config.linktab.c.key)
        q = select(columns=[cnt], from_obj=self.config.linktab)
        q = q.where(self.config.linktab.c.view == self.name)
        q = q.where(self.config.linktab.c.serial == self.serial)
        rp = self.config.engine.execute(q)
        return rp.scalar() > 0

    def generate_key_index(self):
        for index in self.key.table.indexes:
            if index.columns == [self.key]:
                return
        index = Index(self.index_name, self.key)
        index.create(self.config.engine)

    def generate_linktab(self, chunk_size=10000):
        with self.config.engine.begin() as connection:
            q = self.config.linktab.delete()
            q = q.where(self.config.linktab.c.view == self.name)
            connection.execute(q)
            chunk = []
            for i, value in enumerate(self.distinct_key()):
                fp = fingerprints.generate(value)
                if fp is None:
                    continue
                # this is due to postgres' levenshtein
                fp = fp[:255]
                chunk.append({
                    'view': self.name,
                    'serial': self.serial,
                    'key': value,
                    'fingerprint': fp
                })
                if len(chunk) == chunk_size:
                    log.info('Linktab %s (%s): %s', self.name, self.key_ref, i)
                    connection.execute(self.config.linktab.insert(), chunk)
                    chunk = []
            if len(chunk):
                connection.execute(self.config.linktab.insert(), chunk)


class CrossRef(object):
    """Try to match up records in two columns."""

    def __init__(self, config, left, right):
        self.config = config
        self.left = left
        self.right = right

    @property
    def skip(self):
        name = max([self.left.name, self.right.name]), \
            min([self.left.name, self.right.name])
        for skip_name in self.config.skip:
            skip_name = max(skip_name), min(skip_name)
            if skip_name == name:
                return True
        return False

    @property
    def ignore(self):
        if self.skip:
            return True
        if len(self) == 0:
            return True
        return False

    @property
    def label(self):
        return '(%s) %s - %s' % (len(self), self.left.label, self.right.label)

    @property
    def headers(self):
        if not len(self):
            return []
        return self.results[0].keys()

    def query(self):
        tables = self.left.from_clause + self.right.from_clause
        left_lt = self.config.linktab.alias('__left_linktab')
        right_lt = self.config.linktab.alias('__right_linktab')
        tables += [left_lt, right_lt]

        columns = []
        score_length = func.greatest(func.length(self.left.key),
                                     func.length(self.right.key))
        score_leven = func.levenshtein(self.left.key, self.right.key)
        score_leven = cast(score_leven, Float)
        score = 1 - (score_leven / score_length)
        columns.append(score.label("score"))

        for field in self.left.fields:
            columns.append(field.column.label(field.column_ref))
        for field in self.right.fields:
            columns.append(field.column.label(field.column_ref))

        q = select(columns=columns, from_obj=tables)
        q = self.left.apply_filters(q)
        q = self.right.apply_filters(q)
        q = q.where(left_lt.c.key == self.left.key)
        q = q.where(left_lt.c.view == self.left.name)
        q = q.where(right_lt.c.key == self.right.key)
        q = q.where(right_lt.c.view == self.right.name)

        # TODO: make this levenshteinable
        q = q.where(right_lt.c.fingerprint == left_lt.c.fingerprint)
        q = q.limit(self.config.cutoff + 1)
        q = q.order_by(score.desc())
        q = q.distinct()

        # print q
        return q

    @property
    def results(self):
        if self.skip:
            return []
        if not hasattr(self, '_results'):
            log.info("Running: %s ./. %s", self.left.label, self.right.label)
            rp = self.config.engine.execute(self.query())
            self._results = []
            i = 0
            while True:
                row = rp.fetchone()
                if not row or i > self.config.cutoff:
                    break
                self._results.append(OrderedDict(row.items()))
                i + 1
            log.info("Found: %s", len(self._results))
        return self._results

    @property
    def overflow(self):
        return len(self) >= self.config.cutoff

    def __len__(self):
        return len(self.results)


class Linkage(object):
    """A full linkage configuration."""

    def __init__(self, data):
        self.data = data
        self.cutoff = data.get('cutoff', 5000)
        self.report = data.get('report', 'Linkage Report.xlsx')
        self.engine_url = os.path.expandvars(data.get('database'))
        self.engine = create_engine(self.engine_url)
        self.meta = MetaData()
        self.meta.bind = self.engine
        self.views = [View(self, n, v) for n, v in data.get('views').items()]
        self.linktab_name = data.get('linktab', '_linkage')
        self.skip = data.get('skip', [])

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
        if not hasattr(self, '_crossrefs'):
            self._crossrefs = []
            for left in self.views:
                for right in self.views:
                    if left.name >= right.name:
                        continue
                    self._crossrefs.append(CrossRef(self, left, right))
        return self._crossrefs
