import sys
import os
import io
import argparse
import re
import csv
import chardet
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
    except BaseException as e: sys.exit(e)

def arguments():
    parser = argparse.ArgumentParser(description='pivot CSV files')
    parser.add_argument('FILE', nargs='?', default='-', help='the CSV file to operate on -- if omitted, will accept input on STDIN')
    parser.add_argument('-r', '--rows', nargs='+', type=str, help='one or more field names that should be used')
    parser.add_argument('-c', '--columns', nargs='+', type=str, help='one or more field names that should be used')
    parser.add_argument('-v', '--values', nargs='+', type=str, help='one or more field names that should be used, including aggregation functions')
    args = vars(parser.parse_args())
    if args['FILE'] == '-' and sys.stdin.isatty():
        parser.print_help(sys.stderr)
        parser.exit(1)
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
        'concat': lambda x: ','.join(x),
        'concatuniq': lambda x: ','.join(x.unique()),
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
    extractor = re.compile('^(.+)\((.+)\)')
    definitions = definitions or []
    for definition in definitions:
        match = re.match(extractor, definition)
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
    if rows is None and values.get('fields') == []: raise Exception('rows and values are required')
    pivoted = frame.pivot_table(index=rows, columns=columns, values=values.get('fields'), aggfunc=values.get('aggregators'))
    results = pivoted.reset_index().values
    fields = rows + values.get('definitions')
    return fields, results

def output(data, headers):
    output = io.StringIO() if sys.version_info >= (3, 0) else io.BytesIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(data)
    return output.getvalue()

if __name__ == '__main__':
    main()
