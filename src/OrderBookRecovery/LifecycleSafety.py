import logging
import traceback
from functools import wraps
from uuid import uuid4
from flask import jsonify, make_response, request
from werkzeug.exceptions import HTTPException
from src import db

logger = logging.getLogger(__name__)


class LifecycleValidationError(Exception):
    def __init__(self, code, status=409, fields=None):
        self.code, self.status, self.fields = code, status, fields or []
        super().__init__(code)


def log_incident(error, operation):
    incident_id = uuid4().hex
    frames = [{"file": frame.filename, "line": frame.lineno, "function": frame.name}
              for frame in traceback.extract_tb(error.__traceback__)]
    sqlstate = getattr(getattr(error, "orig", None), "pgcode", None)
    logger.error("lifecycle incident_id=%s operation=%s error_class=%s sqlstate=%s traceback=%s",
                 incident_id, operation, type(error).__name__, sqlstate, frames)
    return incident_id


def lifecycle_endpoint(operation):
    def decorate(method):
        @wraps(method)
        def wrapped(*args, **kwargs):
            try:
                if request.method in {"POST", "PATCH"} and request.content_length:
                    if not isinstance(request.get_json(), dict):
                        raise LifecycleValidationError("invalid_json_object", 400)
                return method(*args, **kwargs)
            except LifecycleValidationError as error:
                db.session.rollback()
                return make_response(jsonify(success=False, obj={"msg": error.code,
                    "code": error.code, "fields": error.fields}), error.status)
            except HTTPException as error:
                db.session.rollback()
                return make_response(jsonify(success=False, obj={"msg": "invalid_request",
                    "code": error.code}), error.code)
            except Exception as error:
                try:
                    db.session.rollback()
                except Exception as rollback_error:
                    log_incident(rollback_error, operation + ".rollback")
                incident_id = log_incident(error, operation)
                return make_response(jsonify(success=False, obj={"msg": operation + "_failed_internal",
                    "incident_id": incident_id}), 500)
        return wrapped
    return decorate
