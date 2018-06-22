# requires: pytest

import csvpivot

def test_mean():
    headers = ['location', 'name', 'population']
    data = [
        ['Caribbean', 'Cayman Islands', 56000],
        ['Caribbean', 'British Virgin Islands', 27000],
        ['North Atlantic', 'Bermuda', 64000],
        ['North Atlantic', 'Turks and Caicos Islands', 32000],
        ['Europe', 'Gibraltar', 28800],
    ]
    results, keys = csvpivot.run(data, headers, rows=['location'], values=['mean(population)'])
    assert keys == ['location', 'mean(population)']
    assert list(results) == [
        ['Caribbean', 41500],
        ['North Atlantic', 48000],
        ['Europe', 28800]
    ]

def test_median():
    headers = ['location', 'name', 'population']
    data = [
        ['Caribbean', 'Cayman Islands', 56000],
        ['Caribbean', 'British Virgin Islands', 27000],
        ['North Atlantic', 'Bermuda', 64000],
        ['North Atlantic', 'Turks and Caicos Islands', 32000],
        ['Europe', 'Gibraltar', 28800],
    ]
    results, keys = csvpivot.run(data, headers, rows=['location'], values=['median(population)'])
    assert keys == ['location', 'median(population)']
    assert list(results) == [
        ['Caribbean', 41500],
        ['North Atlantic', 48000],
        ['Europe', 28800]
    ]
