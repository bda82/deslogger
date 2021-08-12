# deslogger

To install, type this:

```
pip install git+https://github.com/bda82/deslogger#egg=desire-logger
```

To use:

```python
from desire_logger.desire_logger import getLogger
logger = getLogger('desire', filename='desire.log')
logger.info('Start here...')
```