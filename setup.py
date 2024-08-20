from setuptools import setup, find_packages

# from repo.repository import __version__

setup(
    name='ofml_api',
    version="1",
    author='Fabian Grünwald',
    author_email='fabian.gruenwald@koenig-neurath.de',
    # py_modules=find_packages(),
    packages=find_packages(),
    install_requires=["pandas"]
)