# requires: pytest

import csvpivot

def test_simple():
    headers = ['name', 'location', 'population']
    data = [
        ['Bermuda', 'North Atlantic', 64000],
        ['Cayman Islands', 'Caribbean', 56000],
        ['Turks and Caicos Islands', 'North Atlantic', 32000],
        ['Gibraltar', 'Europe', 28800],
        ['British Virgin Islands', 'Caribbean', 27000]
    ]
    results, keys = csvpivot.run(data, headers, rows=['location'], values=['mean(population)'])
    assert keys == ['location', 'mean(population)']
    assert list(results) == [
        ['Caribbean', 41500],
        ['Europe', 28800],
        ['North Atlantic', 48000]
    ]
