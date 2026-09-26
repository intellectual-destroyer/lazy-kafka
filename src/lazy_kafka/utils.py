from __future__ import annotations

import time


def get_current_time() -> str:
    """Get current time formatted as HH:MM:SS or HH:MM:SS.microseconds."""
    # Get microseconds from time.time()
    current_time = time.time()
    microseconds = int((current_time % 1) * 1_000_000)
    
    # Format the main time part
    time_str = time.strftime("%H:%M:%S", time.localtime(current_time))
    
    # Append microseconds if present
    if microseconds > 0:
        return f"{time_str}.{microseconds:06d}"
    return time_str
