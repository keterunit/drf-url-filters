from django.db.models import Q


def _make_query(queryset, method, filters, db_values):
    query = Q()
    for key, lookup in db_values.items():
        lookup_op = lookup[0]
        # If has `IN` already in query to this key, apply it.
        if key+'__in' in filters:
            methodfn = getattr(queryset, method)
            queryset = methodfn((key+'__in', filters[key+'__in']))
        # Combine all lookups.
        for value in lookup[1]:
            query = query | Q((key + lookup_op, value))
    return query


def decorate_get_queryset(f):
    def decorated(self):
        query_params = self.request.query_params
        url_params = self.kwargs

        # get db_queries from FiltersMixin
        db_queries = self.get_db_queries(url_params, query_params)

        result = f(self)
        first = True
        for db_query in db_queries:
            queryset = f(self)

            # This dict will hold filter kwargs to pass in to Django ORM calls.
            db_filters = db_query['db_filters']

            # This dict will hold exclude kwargs to pass in to Django ORM calls.
            db_excludes = db_query['db_excludes']

            # This dict will hold filter kwargs subqueries to pass in to Django ORM calls.
            db_filters_values = db_query['db_filters_values']

            # This dict will hold exclude kwargs subqueries to pass in to Django ORM calls.
            db_excludes_values = db_query['db_excludes_values']

            query = _make_query(queryset, 'filter', db_filters, db_filters_values)
            # Same logic as above, but for excludes.
            query_exclude = _make_query(queryset, 'exclude', db_excludes, db_excludes_values)

            queryset = queryset.filter(query, **db_filters).exclude(query_exclude, **db_excludes)

            # Join the queries together.
            if first:
                result = queryset
                first = False
            else:
                result = result | queryset
        return result
    return decorated
