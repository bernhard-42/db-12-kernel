from setuptools import find_packages, setup


setup(
    name="db_12_kernel",
    version="0.1.0",
    description="A Jupyter kernel for Databricks using REST API 1.2",
    long_description="A Jupyter kernel for Databricks using REST API 1.2",
    url="https://github.com/bernhard-42/db-12-kernel",
    author="Bernhard Walter",
    author_email="bernhard@databricks.com",
    install_requires=["metakernel", "jedi", "jupyterlab==2.1", "databricks-cli", "adal", "pyyaml"],
    packages=["db_12_kernel"],
    classifiers=[
        "Framework :: IPython",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        "Topic :: System :: Shells",
    ],
)
