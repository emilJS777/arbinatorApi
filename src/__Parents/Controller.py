from flask_restful import Resource, request
from flask import make_response, jsonify
from src import app
from jsonschema import ValidationError


class Controller(Resource):

    def __init__(self):
        self.per_page = self.safe_int_arg('per_page')
        self.page = self.safe_int_arg('page')
        self.id = self.safe_int_arg('id')
        self.creator_id = request.args.get('creator_id') or None
        self.arguments = request.args
        self.request = request

    @staticmethod
    def safe_int_arg(name, default=0):
        try:
            return int(request.args.get(name) or default)
        except (TypeError, ValueError):
            return default

    # BAD REQUEST EXCEPT
    @staticmethod
    @app.errorhandler(400)
    def bad_request(error):
        if isinstance(error.description, ValidationError):
            original_error = error.description
            return make_response(jsonify(success=False, obj={'msg': original_error.message}), 400)
        # handle other "Bad Request"-errors
        return make_response(jsonify(success=False, obj={'msg': error}), 400)
