from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name='t2db_buffer',
    version='0.1.1',
    description='Buffer for t2db',
    long_description = readme(),
    classifiers=[
      'Programming Language :: Python :: 3.2',
    ],
    url='http://github.com/ptorrest/t2db_buffer',
    author='Pablo Torres',
    author_email='pablo.torres@deri.org',
    license='GNU',
    packages=['t2db_buffer', 't2db_buffer.tests'],
    install_requires=[
        't2db_objects >= 0.5.3',
	    't2db_worker >= 0.3.0',
        'requests >= 2.0.0',
    ],
    entry_points = {
        'console_scripts':[
            't2db_buffer = t2db_buffer.buffer:main'
        ]
    },
    test_suite='t2db_buffer.tests',
    zip_safe = False
)
