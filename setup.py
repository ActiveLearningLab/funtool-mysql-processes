from setuptools import setup

setup(name='funtool-mysql-processes',
        version='0.0.25',
        description='Mysql processes to be used with the FUN Tool ',
        author='Active Learning Lab',
        author_email='pjanisiewicz@gmail.com',
        license='MIT',
        packages=[
            'funtool_mysql_processes',
            'funtool_mysql_processes.adaptors',
            'funtool_mysql_processes.reporters',
        ],
        install_requires=[
            'funtool',
            'funtool_common_processes',
            'PyMySQL',
            'PyYAML'
        ],
        classifiers=[ 
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.2',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4'
        ],  
        zip_safe=False)
