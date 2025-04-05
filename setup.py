from setuptools import setup, find_packages

with open('README.md') as readme_file:
    README = readme_file.read()

setup_args = dict(
    name='deqarclient',
    version='1.1.0',
    description='Python classes to work with DEQAR APIs',
    long_description_content_type="text/markdown",
    long_description=README,
    license='GPL',
    packages=find_packages(),
    scripts=[
        'importInstitutions.py',
        'addIdentifiers.py',
        'fetchEwpRegistry.py',
        'findDuplicateReports.py',
    ],
    author='Colin Tück',
    author_email='colin.tueck@eqar.eu',
    keywords=['DEQAR'],
    url='https://github.com/EQAR/deqar-tools/',
    download_url='https://pypi.org/project/deqarclient/'
)

install_requires = [
    'coloredlogs',
    'requests',
    'tldextract',
    'transliterate'
]

if __name__ == '__main__':
    setup(**setup_args, install_requires=install_requires)

