from typing import Optional, Any


class EisaApiError(Exception):
    """Custom exception for handling EISA API errors."""

    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[Any] = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(message)
