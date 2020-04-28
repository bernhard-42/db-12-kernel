# A Jupyter Metakernel leveraging the Databricks REST API 1.2

## Installation

- Prepare the environment

    ```bash
    conda create -n databricks python=3.7
    conda activate databricks
    ```

- Install db-12-kernel

    ```bash
    pip install git+git://github.com/bernhard-42/db-12-kernel.git
    ```

## Databricks Kernel setup

- Edit `db_12_kernel_config.py` 
- Copy it to the Jupyter config folder

    ```bash
    cp db_12_kernel_config.py ~/.jupyter
    ```

- Create kernelspec

    ```bash
    python -m db_12_kernel install
    ```

## Use Databricks kernel

Start `jupyter lab`, select "DB Kernel". It will use the config in `~/.jupyter/db_12_kernel_config.py`. With execution of the first cell, the Python execution context will be created.
