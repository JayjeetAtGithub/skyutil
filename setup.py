import setuptools
from skyutil.version import Version


setuptools.setup(name='skyutil',
                 version=Version('1.0.0').number,
                 description='Misc. skyhook utilites',
                 author='Jayjeet Chakraborty',
                 author_email='jayjeetc@ucsc.edu',
                 packages=['skyutil'],
                 install_requires=[
                     'pyarrow'
                 ],
                 license='MIT License',
                 zip_safe=False,
                 classifiers=['Packages'])
