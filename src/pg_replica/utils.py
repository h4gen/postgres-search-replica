import asyncio
import logging
import inspect
from typing import Callable, Any, Awaitable

logger = logging.getLogger(__name__)

async def wait_until(
    predicate: Callable[[], Any | Awaitable[Any]], 
    timeout: float = 30.0, 
    interval: float = 0.5, 
    message: str = "Timeout reached while waiting for predicate"
):
    """
    Blocks until the predicate returns a truthy value.
    Predicate can be a synchronous function or an asynchronous function.
    
    Raises:
        asyncio.TimeoutError: If timeout is reached.
    """
    start_time = asyncio.get_event_loop().time()
    
    while True:
        try:
            if inspect.iscoroutinefunction(predicate):
                result = await predicate()
            else:
                result = predicate()
            
            if result:
                return result
        except Exception as e:
            logger.debug(f"Predicate raised exception: {e}")
            
        if asyncio.get_event_loop().time() - start_time >= timeout:
            raise asyncio.TimeoutError(f"{message} (waited {timeout}s)")
            
        await asyncio.sleep(interval)
