import sys
import os
import io
import re
import argparse
import csv
import chardet
import csvpivot

__version__ = '1.4'

def main():
    try:
        file, args = arguments()
        data, headers = read(*file)
        results, keys = csvpivot.run(data, headers, **args)
        formatted = format(results, keys)
        print(formatted)
    except BaseException as e: sys.exit(e)

def arguments():
    parser = argparse.ArgumentParser(description='pivot tables for CSV files')
    parser.add_argument('--version', action='version', version=__version__)
    parser.add_argument('FILE', nargs='?', default='-', help='the CSV file to operate on -- if omitted, will accept input on STDIN')
    parser.add_argument('--enc', type=str, help='encoding of the file (default is to autodetect)')
    parser.add_argument('-r', '--rows', nargs='+', type=str, help='one or more field names that should be used')
    parser.add_argument('-c', '--columns', nargs='+', type=str, help='one or more field names that should be used')
    parser.add_argument('-v', '--values', nargs='+', type=str, help='one or more field names that should be used, including aggregation functions')
    args = vars(parser.parse_args())
    if args['FILE'] == '-' and sys.stdin.isatty():
        parser.print_help(sys.stderr)
        parser.exit(1)
    file = args.pop('FILE')
    enc = args.pop('enc')
    return (file, enc), args

def read(filename, encoding):
    if not os.path.isfile(filename) and filename != '-': raise Exception(filename + ': no such file')
    file = sys.stdin if filename == '-' else io.open(filename, 'rb')
    text = file.read()
    if text == '': raise Exception(filename + ': file is empty')
    if not encoding:
        encoding = chardet.detect(text)['encoding'] # can't always be relied upon
        sys.stderr.write(filename + ': autodetected character encoding as ' + encoding + '\n')
    text_decoded = text.decode(encoding)
    reader_io = io.StringIO(text_decoded) if sys.version_info >= (3, 0) else io.BytesIO(text_decoded.encode('utf8'))
    reader = csv.reader(reader_io)
    try:
        headers = next(reader)
        if len(headers) != len(set(headers)): raise Exception(filename + ': has multiple columns with the same name')
        data = coerce(reader)
        return data, headers
    except csv.Error as e: raise Exception(filename + ': could not read file -- try specifying the encoding')

def coerce(reader):
    for row in reader:
        out = []
        for value in row: # detect and convert to ints or floats where appropriate
            if re.match('^-?\d+$', value.replace(',', '')): out.append(int(value))
            elif re.match('^-?\d+(?:\.\d+)+$', value.replace(',', '')): out.append(float(value))
            else: out.append(value if sys.version_info >= (3, 0) else value.decode('utf8'))
        yield out

def format(results, keys):
    lines = [[value if sys.version_info >= (3, 0) or type(value) is not 'str' else value.encode('utf8') for value in row] for row in results]
    writer_io = io.StringIO() if sys.version_info >= (3, 0) else io.BytesIO()
    writer = csv.writer(writer_io, lineterminator='\n') # can't use dictwriter as headers are printed even when there's no results
    writer.writerow(keys)
    writer.writerows(lines)
    return writer_io.getvalue()

if __name__ == '__main__':
    main()
