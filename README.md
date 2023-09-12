> :warning: All positional arguments of decorated functions mapped to their respective keyword argument

> :warning: All arguments of decorated functions are hashed on their `__repr__()`

## As Decorators

### Simple Functions

```py
from cache_decorators import CacheDecoratorFactory
from ibis.expr.types import Table

# Creates (Path.cwd() / ".cache") if folder does not exist
my_decorator = CacheDecoratorFactory()


@my_decorator
def my_func() -> Table:
    # Some expensive or slow code
    ...


# Runs expensive code and saves to cache
call_1 = my_func()
# Retrieves data from cache
call_2 = my_func()
```

### File Reading Functions

```py
from cache_decorators import CachedFileReaderDecoratorFactory
from ibis.expr.types import Table

# Creates (Path.cwd() / ".cache") if folder does not exist
my_decorator = CachedFileReaderDecoratorFactory(path_kwd="my_path")


@my_decorator  # Expects "my_path" argument to be pathlike
def my_reader(my_path: str, my_arg: int = 0) -> Table:
    # Some expensive or slow code
    ...


input_file = "input_file.csv"

# Runs expensive code and saves to cache
call_1 = my_reader(input_file)
# Retrieves data from cache
call_2 = my_reader(input_file, 0)
# Runs expensive code and saves to cache
call_3 = my_reader(input_file, 1)
# Retrieves data from cache
call_4 = my_reader(my_path=input_file, my_arg=0)
# Retrieves data from cache
call_5 = my_reader(my_path=input_file, my_arg=1)
```


## As Wrappers

### Simple Functions

```py
from cache_decorators import CacheDecoratorFactory
from some_library import external_function

# Creates (Path.cwd() / ".cache") if folder does not exist
my_decorator = CacheDecoratorFactory()


decorated_function = my_decorator(external_function)


# Runs expensive code and saves to cache
call_1 = decorated_function("my_arg")
# Retrieves data from cache
call_2 = decorated_function("my_arg")
```


### File Reading Functions

```py
import pandas as pd
from cache_decorators import CachedFileReaderDecoratorFactory

# Creates (Path.cwd() / ".cache") if folder does not exist
my_decorator = CachedFileReaderDecoratorFactory(path_kwd="io")
decorated_function = my_decorator(pd.read_excel)

input_file = "input_file.xlsx"

# Runs expensive code and saves to cache
call_1 = decorated_function(input_file, "sheet 1")
# Retrieves data from cache
call_2 = decorated_function(io=input_file, sheet_name="sheet 1")
# Runs expensive code and saves to cache
call_3 = decorated_function(input_file, "sheet 2")
# Retrieves data from cache
call_4 = decorated_function(io=input_file, sheet_name="sheet 2")
```
