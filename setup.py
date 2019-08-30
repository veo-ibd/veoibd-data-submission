from setuptools import setup, find_packages

setup(name='veoibddatasubmission',
      version='0.1',
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
