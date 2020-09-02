import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='celery-dispatcher', # Replace with your own username
    version='1.0.0',
    author='gsfish',
    author_email='root@grassfish.net',
    description='Support a large number of subtasks for celery framework',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/gsfish/celery-dispatcher',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)