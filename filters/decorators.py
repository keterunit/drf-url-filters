from django.db.models import Q


def make_query(queryset, method, filters, db_values):
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
        queryset = f(self)
        query_params = self.request.query_params
        url_params = self.kwargs

        # get queryset_filters from FiltersMixin
        queryset_filters = self.get_db_filters(url_params, query_params)

        # This dict will hold filter kwargs to pass in to Django ORM calls.
        db_filters = queryset_filters['db_filters']

        # This dict will hold exclude kwargs to pass in to Django ORM calls.
        db_excludes = queryset_filters['db_excludes']

        # This dict will hold filter kwargs subqueries to pass in to Django ORM calls.
        db_filters_values = queryset_filters['db_filters_values']

        # This dict will hold exclude kwargs subqueries to pass in to Django ORM calls.
        db_excludes_values = queryset_filters['db_excludes_values']

        query = make_query(queryset, 'filter', db_filters, db_filters_values)
        # Same logic as above, but for excludes.
        query_exclude = make_query(queryset, 'exclude', db_excludes, db_excludes_values)

        return queryset.filter(query, **db_filters).exclude(query_exclude, **db_excludes)
    return decorated
