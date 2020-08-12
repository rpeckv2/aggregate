from setuptools import setup, find_packages

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
   name='aggregator',
   version='1.0.0',
   description='Module to combine csv files from partners, and clean data in the process',
   author='Russell Peck',
   author_email='peck.russell@gmail.com',
   packages=find_packages(),
   install_requires=required,
   url="<https://github.com/rpeckv2/aggregator>",
   classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Ubuntu 18.04.3 LTS",
    ],
   python_requires='>=3.6',
)
