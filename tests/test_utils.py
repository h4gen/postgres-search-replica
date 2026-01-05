import pytest
import asyncio
from pg_replica.utils import wait_until

@pytest.mark.asyncio
async def test_wait_until_success_sync():
    call_count = 0
    def predicate():
        nonlocal call_count
        call_count += 1
        return call_count > 2
    
    result = await wait_until(predicate, timeout=1.0, interval=0.1)
    assert result is True
    assert call_count == 3

@pytest.mark.asyncio
async def test_wait_until_success_async():
    call_count = 0
    async def predicate():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return call_count > 2
    
    result = await wait_until(predicate, timeout=1.0, interval=0.1)
    assert result is True
    assert call_count == 3

@pytest.mark.asyncio
async def test_wait_until_timeout():
    def predicate():
        return False
    
    with pytest.raises(asyncio.TimeoutError) as excinfo:
        await wait_until(predicate, timeout=0.2, interval=0.05, message="Testing timeout")
    
    assert "Testing timeout" in str(excinfo.value)
    assert "(waited 0.2s)" in str(excinfo.value)

@pytest.mark.asyncio
async def test_wait_until_exception_handling():
    call_count = 0
    def predicate():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("Intermittent error")
        return call_count > 2
    
    # Should ignore the first exception and succeed on the third call
    result = await wait_until(predicate, timeout=1.0, interval=0.1)
    assert result is True
    assert call_count == 3
