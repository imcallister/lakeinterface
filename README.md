# lake_interface

This repo makes use of [nbdev](https://nbdev.fast.ai/) to:
- facilitate the path from the data exploration process into creation of robust libraries for use in production
- documentation of both the production code and the data sources

You will need pandas, jupyterlab and nbdev. 

```console
$ pip install pandas==2.0.0
$ pip install boto3==1.26.114
$ pip install nbdev==2.3.12
$ pip install jupyterlab==3.6.3

```

There are a couple of nbdev extensions needed:
```console
$ nbdev_install_hooks
$ nbdev_install_quarto
$ pip install jupyterlab-quarto==0.1.45
```

Documentation can be viewed locally with
```console
$ nbdev_preview
```