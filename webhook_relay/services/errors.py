from __future__ import annotations


class ServiceError(Exception):
    status_code = 400
    code = "bad_request"

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class NotFoundError(ServiceError):
    status_code = 404
    code = "not_found"


class UnauthorizedError(ServiceError):
    status_code = 401
    code = "unauthorized"


class ConflictError(ServiceError):
    status_code = 409
    code = "conflict"
