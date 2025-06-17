from setuptools import setup  # type:ignore

setup(
    name="ofml_api",
    version="1.0.3",
    author="Fabian Grünwald",
    author_email="fabian.gruenwald@koenig-neurath.de",
    packages=["ofml_api"],
    install_requires=["pandas"],
)
