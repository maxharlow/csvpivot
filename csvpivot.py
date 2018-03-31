import sys
import re
import math
import itertools
import numpy
import pandas

def run(
        data,
        headers,
        rows=None,
        columns=None,
        values=None
    ):
    valueset = interpret(values, headers)
    results, keys = pivot(data, headers, rows, columns, valueset)
    return results, keys

def interpret(values, headers):
    operations = {
        'concat': lambda x: ','.join(str(e) for e in x),
        'concatuniq': lambda x: ','.join(str(e) for e in x.unique()),
        'count': lambda x: len(x),
        'countuniq': lambda x: len(x.unique()),
        'sum': numpy.sum,
        'mean': numpy.mean,
        'median': numpy.median,
        'max': numpy.max,
        'min': numpy.min,
        'stddev': numpy.std
    }
    fields = []
    aggregators = {}
    extractor = re.compile('^(.+?)\((.+)\)$')
    definitions = values or []
    definitions_seen = [] # for duplicate checking
    for definition in definitions:
        if definition.lower() in definitions_seen: raise Exception(definition + ': cannot be specified multiple times')
        definitions_seen.append(definition.lower())
        match = re.match(extractor, definition)
        if match is None: raise Exception(definition + ': not in the correct format')
        operation = match.group(1)
        field = match.group(2)
        if operation.lower() not in operations: raise Exception(definition + ': operation not found')
        if field not in headers: raise Exception(definition + ': not found in headers')
        if field in fields: aggregators.get(field).append(operations.get(operation))
        else: aggregators.update({field: [operations.get(operation)]})
        fields.append(field)
    return {'definitions': definitions, 'fields': fields, 'aggregators': aggregators}

def pivot(data, headers, rows, columns, valueset):
    if rows:
        for row in rows:
            if row not in headers: raise Exception(row + ': not found in headers')
    if columns:
        for column in columns:
            if column not in headers: raise Exception(row + ': not found in headers')
    frame = pandas.DataFrame(data, columns=headers)
    if rows is None or valueset.get('fields') == []: raise Exception('rows and values must both be specified')
    valueset_fields = [field for field in valueset.get('fields') if field not in rows] # should't be given as a value if specified as a row
    pivoted = frame.pivot_table(index=rows, columns=columns, values=valueset_fields, aggfunc=valueset.get('aggregators'))
    results_set = pivoted.where(pandas.notnull(pivoted), None).reset_index().values
    results = [[int(v) if isinstance(v, float) and math.floor(v) == v else v for v in result] for result in results_set] # to int where possible
    columns_values = pivoted.columns.levels[2:]
    columns_values_names = [':'.join(column_value) for column_value in list(itertools.product(*columns_values))]
    columns_values_definitions = [definition + ':' + column for definition in valueset.get('definitions') for column in columns_values_names]
    keys = rows + (valueset.get('definitions') if columns is None else columns_values_definitions)
    return results, keys
