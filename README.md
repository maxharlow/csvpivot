CSV Pivot
=========

Pivot tables for CSV files in the terminal.

Tested on Python 3.6 and 2.7.


Installing
----------

    pip install csvpivot


Usage
-----

Say you have a CSV file such as:

    name,country,gender,salary
    Oliver,UK,M,10000
    Jack,UK,M,21000
    Emily,UK,F,32000
    Harry,UK,M,43000
    Adam,France,M,54000
    Paul,France,M,65000
    Louise,France,F,76000
    Alice,France,F,87000
    Emma,Germany,F,98000

We could then find the average salary in each country:

    $ csvpivot test.csv --rows country --values 'mean(salary)'

    country,mean(salary)
    France,70500
    Germany,98000
    UK,26500

It would be useful to find out the maximum and minimum values too though:

    $ csvpivot test.csv --rows country --values 'mean(salary)' 'min(salary)' 'max(salary)'

    country,mean(salary),min(salary),max(salary)
    France,70500,54000,87000
    Germany,98000,98000,98000
    UK,26500,10000,43000

As well as `mean`, `min`, and `max`, CSV Pivot also supports `median`, `sum`, `stddev`, `count`, `countuniq`, `concat`, and `concatuniq`. All require numerical values apart from the last two. If numbers contain commas they are interpreted as thousands separators and removed.

Columns are also supported. So we could break down our data by gender:

    $ csvpivot test.csv --rows country --values 'mean(salary)' --columns gender

    country,mean(salary):F,mean(salary):M
    France,81500,59500
    Germany,98000,
    UK,32000,24666.666666666668
