import asyncio
import time
from typing import Callable, Any, TypeVar

T = TypeVar("T")

async def wait_until_success(
    func: Callable[[], T],
    timeout: float = 30.0,
    check_interval: float = 1.0,
    exception_types: tuple = (AssertionError, Exception),
    error_msg: str = "Timed out waiting for condition"
) -> T:
    """
    Retries an async or sync function until it returns a truthy value or raises no exception.
    """
    start_time = time.time()
    last_exception = None

    while time.time() - start_time < timeout:
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func()
            else:
                result = func()
            
            if result:
                return result
        except exception_types as e:
            last_exception = e
        
        await asyncio.sleep(check_interval)

    raise AssertionError(f"{error_msg}. Last error: {last_exception}")
