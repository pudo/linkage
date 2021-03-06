from setuptools import setup, find_packages


setup(
    name='linkage',
    version='0.1',
    description="A data cross-referencing tool.",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4'
    ],
    keywords='sql cross-referencing',
    author='Friedrich Lindenberg',
    author_email='friedrich@pudo.org',
    url='http://github.com/pudo/montage',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    package_data={},
    include_package_data=True,
    zip_safe=False,
    test_suite='nose.collector',
    install_requires=[
        'click',
        'pyyaml',
        'fingerprints',
        'six',
        'sqlalchemy',
        'xlsxwriter',
        'normality'
    ],
    tests_require=[
        'nose',
        'coverage',
        'wheel'
    ],
    entry_points={
        'console_scripts': [
            'linkage = linkage.cli:cli'
        ]
    }
)
