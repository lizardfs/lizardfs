import json
from typing import Dict, List

from flask import Flask, request, Response, current_app

TestList = List[str]
TestSuite = Dict[str, TestList]
BuildTestData = Dict[str, TestSuite]


def create_app() -> Flask:
    app = Flask(__name__)
    app_context = app.app_context()
    app_context.push()
    current_app.config["tests"] = dict()

    @app.route("/", methods=["GET"])
    def index() -> Response:
        return Response(
            json.dumps(current_app.config["tests"]),
            status=200,
            mimetype="application/json",
        )

    @app.route("/push_list", methods=["POST"])
    def push_list() -> Response:
        json_data = request.get_json()
        assert json_data is not None
        build_id = json_data["build_id"]
        test_suite = json_data.get("test_suite")
        tests = json_data.get("tests")

        if build_id in current_app.config["tests"]:
            if test_suite in current_app.config["tests"][build_id]:
                return Response(
                    json.dumps({"details": "This tests list exists already."}),
                    status=412,
                    mimetype="application/json",
                )
        else:
            current_app.config["tests"][build_id] = {}

        current_app.config["tests"][build_id][test_suite] = tests

        return Response(
            json.dumps({"details": "OK"}), status=201, mimetype="application/json"
        )

    @app.route("/next_test", methods=["GET"])
    def next_test() -> Response:
        build_id = str(request.args.get("build_id", ""))
        test_suite = str(request.args.get("test_suite", ""))

        if build_id == "" or test_suite == "":
            return Response(
                json.dumps({"details": ""}), status=400, mimetype="application/json"
            )
        if (
            build_id not in current_app.config["tests"]
            or test_suite not in current_app.config["tests"][build_id]
        ):
            return Response(
                json.dumps({"details": ""}), status=200, mimetype="application/json"
            )
        if len(current_app.config["tests"][build_id][test_suite]) == 0:
            return Response(
                json.dumps({"details": ""}), status=200, mimetype="application/json"
            )

        first_test = current_app.config["tests"][build_id][test_suite][0]

        del current_app.config["tests"][build_id][test_suite][0]

        if len(current_app.config["tests"][build_id][test_suite]) == 0:
            del current_app.config["tests"][build_id][test_suite]
            if len(current_app.config["tests"][build_id]) == 0:
                del current_app.config["tests"][build_id]

        return Response(
            json.dumps({"details": first_test}), status=200, mimetype="application/json"
        )

    return app
