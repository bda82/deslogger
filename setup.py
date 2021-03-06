import setuptools

setuptools.setup(
    name='desire_logger',
    version='0.0.1',
    author='bda82',
    author_email='dmitry.bespalov@websailors.pro',
    description='Desire logger with ClickHouse Support',
    packages=setuptools.find_packages(),
    url="https://github.com/bda82/deslogger",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "requests==2.26.0"
    ]
)
