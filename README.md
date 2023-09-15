> :warning: Decorated functions are hashed on their source with [`insepct.getsource`](https://docs.python.org/3/library/inspect.html#inspect.getsource), meaning `lambda`'s or other `callable`'s without source code are not decorable.

> :warning: All arguments of decorated functions are bound to their respective keyword argument via [`inspect.Signature.bind`](https://docs.python.org/3/library/inspect.html#inspect.Signature.bind)

> :warning: All arguments of decorated functions are hashed on their [`__repr__()`](https://docs.python.org/3/library/functions.html?highlight=repr#repr)

## As decorators

```py
from cache_decorators import FileCacher
from ibis.expr.types import Table


# Creates (Path.cwd() / ".cache") if folder does not exist
@FileCacher()
def func_1(number: int, message: str) -> Table:
    # Some expensive or slow code
    ...


# Runs expensive code and saves to cache
f1_call_1 = func_1(1, "some str")
# Retrieves data from cache
f1_call_2 = func_1(1, message="some str")
# Retrieves data from cache
f1_call_3 = func_1(number=1, message="some str")
# Runs expensive code and saves to cache
f1_call_4 = func_1(1, "some other str")
# Retrieves data from cache
f1_call_5 = func_1(1, message="some other str")


# Creates ("./some/other/dir") if folder does not exist
my_decorator = FileCacher(cache_dir="./some/other/dir")


@my_decorator
def func_2() -> Table:
    # Some expensive or slow code
    ...


# Runs expensive code and saves to cache
f2_call_1 = func_2()
# Retrieves data from cache
f2_call_2 = func_2()
```

## As wrappers

```py
from cache_decorators import FileCacher
from some_library import external_function

# Creates (Path.cwd() / ".cache") if folder does not exist
my_decorator = FileCacher()


decorated_function = my_decorator(external_function)


# Runs expensive code and saves to cache
call_1 = decorated_function("my_arg")
# Retrieves data from cache
call_2 = decorated_function("my_arg")
# Runs expensive code and saves to cache
call_3 = decorated_function("new_arg")
```
## Real world use

After the first run, subsequent runs of the following code will not have to read the `my_excel_file.xlsx` file directly, and will instead prefer to read from the cached file. If `my_excel_file.xlsx` is updated, it will be re-cached when requested.

```py
import pandas
from cache_decorators import FileCacher, FileComparisonDecider

# Creates "my_cache_dir" if folder does not exist
my_read_excel = FileCacher(
    cache_dir="my_cache_dir",
    update_decider=FileComparisonDecider("io"),
)(pandas.read_excel)

# Runs expensive code and saves to cache
call_1 = my_read_excel("my_excel_file.xlsx")
# Retrieves data from cache
call_2 = my_read_excel(io="my_excel_file.xlsx")
```
