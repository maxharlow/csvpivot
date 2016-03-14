import sys
import os
import io
import argparse
import re
import math
import csv
import chardet
import itertools
import numpy
import pandas

def main():
    try:
        args = arguments()
        headers, data = read(args['FILE'])
        values = interpret(args['values'], headers)
        fields, data = pivot(data, headers, args['rows'], args['columns'], values)
        results = output(data, fields)
        print(results)
    except BaseException as e: sys.exit(e.message.lower())

def arguments():
    parser = argparse.ArgumentParser(description='pivot tables for CSV files')
    parser.add_argument('FILE', nargs='?', default='-', help='the CSV file to operate on -- if omitted, will accept input on STDIN')
    parser.add_argument('-r', '--rows', nargs='+', type=str, help='one or more field names that should be used')
    parser.add_argument('-c', '--columns', nargs='+', type=str, help='one or more field names that should be used')
    parser.add_argument('-v', '--values', nargs='+', type=str, help='one or more field names that should be used, including aggregation functions')
    args = vars(parser.parse_args())
    if args['FILE'] == '-' and sys.stdin.isatty():
        parser.print_help(sys.stderr)
        parser.exit('')
    return args

def read(filename):
    if not os.path.isfile(filename) and filename != '-': raise Exception(filename + ': no such file')
    file = sys.stdin if filename == '-' else io.open(filename, 'rb')
    text = file.read()
    if text == '': raise Exception(filename + ': file is empty')
    text_decoded = text.decode(chardet.detect(text)['encoding'])
    data_io = io.StringIO(text_decoded) if sys.version_info >= (3, 0) else io.BytesIO(text_decoded.encode('utf8'))
    data = list(csv.reader(data_io))
    headers = data[0]
    if len(headers) != len(set(headers)): raise Exception(filename + ': has multiple columns with the same name')
    reader_io = io.StringIO(text_decoded) if sys.version_info >= (3, 0) else io.BytesIO(text_decoded.encode('utf8'))
    reader = csv.DictReader(reader_io)
    rows = []
    for row in reader:
        item = {}
        for key, value in row.items(): # detect and convert to ints or floats where appropriate
            if re.match('^-?\d+$', value.replace(',', '')): item[key] = int(value)
            elif re.match('^-?\d+(?:\.\d+)+$', value.replace(',', '')): item[key] = float(value)
            else: item[key] = value if sys.version_info >= (3, 0) else value.decode('utf8')
        rows.append(item)
    return headers, rows

def interpret(definitions, headers):
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
    extractor = re.compile('^(.+)\((.+)\)$')
    definitions = definitions or []
    for definition in definitions:
        match = re.match(extractor, definition)
        if match == None: raise Exception(definition + ': not in the correct format')
        operation = match.group(1)
        field = match.group(2)
        if match is None: raise Exception(definition + ': value not correctly specified')
        if operation.lower() not in operations: raise Exception(definition + ': operation not found')
        if field not in headers: raise Exception(definition + ': not found in headers')
        if field in fields: aggregators.get(field).append(operations.get(operation))
        else: aggregators.update({field: [operations.get(operation)]})
        fields.append(field)
    return {'definitions': definitions, 'fields': fields, 'aggregators': aggregators}

def pivot(data, headers, rows, columns, values):
    if rows:
        for row in rows:
            if row not in headers: raise Exception(row + ': not found in headers')
    if columns:
        for column in columns:
            if column not in headers: raise Exception(row + ': not found in headers')
    frame = pandas.DataFrame(data)
    if rows is None or values.get('fields') == []: raise Exception('rows and values must both be specified')
    pivoted = frame.pivot_table(index=rows, columns=columns, values=values.get('fields'), aggfunc=values.get('aggregators'))
    results_set = pivoted.where(pandas.notnull(pivoted), None).reset_index().values
    results = [[int(v) if isinstance(v, float) and math.floor(v) == v else v for v in result] for result in results_set] # to int where possible
    columns_values = pivoted.columns.levels[2:]
    columns_values_names = [':'.join(column_value) for column_value in list(itertools.product(*columns_values))]
    columns_values_definitions = [definition + ':' + column for definition in values.get('definitions') for column in columns_values_names]
    fields = rows + (values.get('definitions') if columns is None else columns_values_definitions)
    return fields, results

def output(data, headers):
    output = io.StringIO() if sys.version_info >= (3, 0) else io.BytesIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(data)
    return output.getvalue()

if __name__ == '__main__':
    main()
