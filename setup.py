try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='twsd',
      version='0.1.1',
      description='Twitter Stream Downloader.',
      author='Antonio Lima',
      author_email='anto87@gmail.com',
      url='',
      install_requires=[
              'requests',
              'requests-oauthlib',
          ],
      packages=['twsd'],
      entry_points="""
      [console_scripts]
      twsd = twsd:main
      """
      )
