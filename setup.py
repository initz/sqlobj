from setuptools import setup
with open('README.md','r') as f:
    readme = f.read()
with open('HISTORY.md','r') as f:
    history = f.read()
packages=[
  'sqlobj'
]
requires=[
  'sqlobject'
]
setup(name='sqlobj',
  version='0.3.0',
  author='iinitz',
  author_email='hanamiza555@gmail.com',
  description='',
  long_description=readme+'\n\n'+history,
  packages=packages,
  package_dir={'sqlobj':'sqlobj'},
  install_requires=requires
)
