from flask import make_response, jsonify


class Response:
    # RESPONSES
    @staticmethod
    def response(success, obj, status_code) -> make_response:
        return make_response(jsonify(success=success, obj=obj), status_code)

    @staticmethod
    def response_conflict(msg=None):
        return Response.response(False, {'msg': msg or 'exist'}, 409)

    @staticmethod
    def response_not_found(msg=None):
        return Response.response(False, {'msg': msg or 'not found'}, 404)

    @staticmethod
    def response_invalid_password():
        return Response.response(False, {'msg': 'incorrect password'}, 401)

    @staticmethod
    def response_created(msg=None):
        return Response.response(True, {'msg': msg or 'successfully created'}, 200)

    @staticmethod
    def response_err_msg(msg):
        return Response.response(False, {'msg': msg}, 501)

    @staticmethod
    def response_updated(msg=None):
        return Response.response(True, {'msg': msg or 'successfully updated'}, 200)

    @staticmethod
    def response_ok(obj):
        return Response.response(True, obj, 200)

    @staticmethod
    def response_deleted(msg=None):
        return Response.response(True, {'msg': msg or 'successfully deleted'}, 200)

    @staticmethod
    def response_invalid_login():
        return Response.response(False, {'msg': 'invalid username and/or password'}, 401)

    @staticmethod
    def response_forbidden(msg=None):
        return Response.response(False, {'msg': msg or 'forbidden'}, 403)