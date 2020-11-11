import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='celery-dispatcher',
    version='1.0.1',
    author='gsfish',
    author_email='root@grassfish.net',
    description='An extension for celery to dispatch large amount of subtasks within a main task',
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
    install_requires=['celery<5', 'redis']
)