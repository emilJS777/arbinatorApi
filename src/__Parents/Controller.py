from flask_restful import Resource, request
from flask import make_response, jsonify
from src import app
from jsonschema import ValidationError


class Controller(Resource):

    def __init__(self):
        self.per_page = int(request.args.get('per_page') or 0)
        self.page = int(request.args.get('page') or 0)
        self.id = int(request.args.get('id') or 0)
        self.creator_id = request.args.get('creator_id') or None
        self.arguments = request.args
        self.request = request

    # BAD REQUEST EXCEPT
    @staticmethod
    @app.errorhandler(400)
    def bad_request(error):
        if isinstance(error.description, ValidationError):
            original_error = error.description
            return make_response(jsonify(success=False, obj={'msg': original_error.message}), 400)
        # handle other "Bad Request"-errors
        return make_response(jsonify(success=False, obj={'msg': error}), 400)