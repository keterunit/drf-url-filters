import six
import re

from voluptuous import Invalid
from rest_framework.exceptions import ParseError

from .metaclasses import MetaFiltersMixin
from .schema import base_query_params_schema


def _get_query_groups(query_params):
    '''
    Group prefixes by the integer number.
    '''
    ors = {0: set()}
    for query in query_params:
        match = re.match(r'or([0-9]*[1-9]+)-', query)
        if match:
            k = int(match.group(1))
            value = match.group(0)
        else:
            k = 0
            zero_match = re.match(r'or(0*)-', query)
            if zero_match:
                value = zero_match.group(0)
            else:
                continue

        if k in ors:
            ors[k].add(value)
        else:
            ors[k] = {value}
    return ors

def _remove_or_prefix(query_params, or_group):
    '''
    Given a group get all terms that belong to it.
    '''
    terms = {}

    for query, value in query_params.items():
        found = False
        new_query = query
        for prefix in or_group[1]:
            if query.startswith(prefix):
                new_query = query.removeprefix(prefix)
                found = True
                break
        if found or (or_group[0] == 0 and
                     not re.match(r'or[0-9]*-', query)):
            terms[new_query] = value
    return terms


@six.add_metaclass(MetaFiltersMixin)
class FiltersMixin(object):
    '''
    This viewset provides dynamically generated
    filters by applying defined filters on generic
    queryset.
    '''

    def __get_queryset_filters(self, query_params, *args, **kwargs):
        '''
        get url_params and query_params and make db_queries
        to filter the queryset to the finest.
        [1] ~ sign is used to negated / exclude a filter.
        [2] when a CSV is passed as value to a query params make a filter
            with 'IN' query.
        [3] a filter prefixed with 'or[0-9]*-' is grouped by the integer number
            in a separate query, then the results of all queries are joined. If
            it is not prefixed is assumed to belong to first group.
        '''
        db_queries = []

        if getattr(self, 'filter_mappings', None) and query_params:
            filter_mappings = self.filter_mappings
            value_transformations = getattr(self, 'filter_value_transformations', {})

            ors = _get_query_groups(query_params)
            # [3] for each group of terms, get their queries and make the
            # filter.
            for or_group in ors.items():
                new_query_params = _remove_or_prefix(query_params, or_group)

                db_filters = []
                db_excludes = []
                db_filters_values = []
                db_excludes_values = []

                try:
                    # check and raise 400_BAD_REQUEST for invalid query params
                    filter_validation_schema = getattr(
                        self,
                        'filter_validation_schema',
                        base_query_params_schema
                    )
                    new_query_params = filter_validation_schema(new_query_params)
                except Invalid as inst:
                    raise ParseError(detail=inst)

                iterable_query_params = (
                    new_query_params.iteritems() if six.PY2 else new_query_params.items()
                )

                for query, value in iterable_query_params:
                    # [1] ~ sign is used to exclude a filter.
                    is_exclude = '~' in query
                    if query in self.filter_mappings and value:
                        query_filter = filter_mappings[query]
                        transform_value = value_transformations.get(query, lambda val: val)
                        transformed_value = transform_value(value)
                        # [2] multiple options is filter values will execute as `IN` query
                        if isinstance(transformed_value, list) and not query_filter.endswith('__in'):
                            # If lookup uses contains and is a CSV, needs to apply
                            # contains separately with each value.

                            lookups_with_subquery = ('__contains', '__icontains',
                                                     '__startswith', '__istartswith',
                                                     '__endswith', '__iendswith',
                                                     '__iexact',
                                                     '__regex', '__iregex')
                            found = False
                            for lookup_suffix in lookups_with_subquery:
                                if query_filter.endswith(lookup_suffix):
                                    lookup = (query_filter[:-len(lookup_suffix)],
                                              (lookup_suffix, transformed_value))
                                    if is_exclude:
                                        db_excludes_values.append(lookup)
                                    else:
                                        db_filters_values.append(lookup)
                                    found = True
                                    break
                            if found:
                                continue
                            query_filter += '__in'

                        if is_exclude:
                            db_excludes.append((query_filter, transformed_value))
                        else:
                            db_filters.append((query_filter, transformed_value))

                db_queries.append({
                    'db_filters': dict(db_filters),
                    'db_excludes': dict(db_excludes),
                    'db_filters_values': dict(db_filters_values),
                    'db_excludes_values': dict(db_excludes_values)
                })

        return db_queries

    def __merge_query_params(self, url_params, query_params):
        '''
        merges the url_params dict with query_params query dict and returns
        the merged dict.
        '''
        url_params = {}
        for key in query_params:
            url_params[key] = query_params.get(key) # get method on query-dict works differently than on dict.
        return url_params

    def get_db_queries(self, url_params, query_params):
        '''
        returns a dict with db_filters and db_excludes values which can be
        used to apply on viewsets querysets.
        '''

        # merge url and query params
        query_params = self.__merge_query_params(url_params, query_params)

        # get queryset filters
        db_queries = self.__get_queryset_filters(query_params)

        return db_queries

    def get_queryset(self):
        # Defined here to handle the case where the viewset
        # does not override get_queryset
        # (and hence the metaclass would not have been
        # able to decorate it with the filtering logic.)

        return super(FiltersMixin, self).get_queryset()
