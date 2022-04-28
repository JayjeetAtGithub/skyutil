import setuptools
from skyutil.version import Version


setuptools.setup(name='skyutil',
                 version=Version('1.0.0').number,
                 description='Misc. skyhook utilites',
                 long_description=open('README.md').read().strip(),
                 author='Jayjeet Chakraborty',
                 author_email='jayjeetc@ucsc.edu',
                 py_modules=['skyutil'],
                 install_requires=[
                     'pyarrow'
                 ],
                 license='MIT License',
                 zip_safe=False,
                 classifiers=['Packages'])
