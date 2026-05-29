
from setuptools import setup

__version__ = '0.2.2'


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='submitter',
    version=__version__,
    author='Steve Sclafani, Mike Richman, Woosik Kang',
    author_email='mike.d.richman@gmail.com',
    packages = ['submitter'],
    description='Job submission helper developed at UMD, updated at Drexel',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='github.com/woosikk/Submitter',
    install_requires=['numpy'],
)
