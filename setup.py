import os
from setuptools import setup, find_packages

# figure out the version
about = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "veoibddatasubmission", "__version__.py")) as f:
    exec(f.read(), about)

setup(name='veoibddatasubmission',
      version=about['__version__'],
      description='Processing and validation for VEO-IBD',
      url='https://github.com/VEO-IBD/veoibd-data-submission',
      author='Kenneth Daily',
      author_email='kenneth.daily@sagebionetworks.org',
      license='MIT',
      packages=find_packages(),
      zip_safe=False,
      python_requires='>=3.5',
      install_requires=[
        'pandas>=0.20.0',
        'synapseclient>=1.9'])
