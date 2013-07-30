import setuptools

# Get version info
__version__ = None
__release__ = None
exec open('dblayer/version.py')

setuptools.setup(
    name='dblayer',
    version=__release__,
    description='Database Abstraction Layer Generator',
    long_description='''\
Generates database abstraction layer in Python based on a
readable definition written as Python classes. Supports
defining of tables, basic column types, indexes,
constraints, full text search and complex queries.
Provides a light-weight approach of database access with
minimal runtime overhead and without any kind of hidden
magic, metaclasses or so. Provides maximum possible
support for auto completion in Python IDEs.''',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database',
        'Topic :: Software Development :: Code Generators',
        ],
    keywords='python database orm postgresql abstraction layer generator codegeneration performance',
    author='Viktor Ferenczi',
    author_email='viktor@ferenczi.eu',
    url='http://code.google.com/p/dblayer',
    license='MIT',
    packages=[
        'dblayer',
        'dblayer.backend.base',
        'dblayer.backend.postgresql',
        'dblayer.generator',
        'dblayer.graph',
        'dblayer.model',
        'dblayer.test',
        ],
    package_data={
        '': ['template/*.tpl'],
        },
    include_package_data=True,
    test_suite='unittest',
    zip_safe=False,
    install_requires=['bottle', 'psycopg2'],
    )
