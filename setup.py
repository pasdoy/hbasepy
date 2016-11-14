from setuptools import setup

setup(name='hbasepy',
      version='0.1.1',
      description='Hbase REST client',
      url='https://github.com/pasdoy/hbasepy',
      author='pasdoy',
      author_email='pasdoy12@gmail.com',
      license='MIT',
      packages=['hbasepy'],
      install_requires=[
          'requests',
      ],
      zip_safe=False)