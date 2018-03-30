from setuptools import setup

setup(
    name='csvpivot',
    version='1.1',
    description='Pivot tables for CSV files in the terminal.',
    long_description=open('README.md').read(),
    author='Max Harlow',
    author_email='maxharlow@gmail.com',
    url='https://github.com/maxharlow/csvpivot',
    license='Apache',
    packages=[''],
    install_requires=[
        'chardet==3.0.4',
        'numpy==1.14.2',
        'pandas==0.22.0'
    ],
    entry_points = {
        'console_scripts': [
            'csvpivot = csvpivot:main'
        ]
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Utilities'
    ]
)
