import traceback

from flask import Flask, jsonify, request

from services.recommendation import run

app = Flask(__name__)


@app.route("/", methods=["GET"])
def default():
    return jsonify({"message": "Recommendation Service is up and running!"}), 200


@app.route("/webhook", methods=["POST"])
def wehbook():
    try:
        # Step 1: Get the JSON payload
        payload = request.json

        # Step 2: Validate mandatory fields
        repo_id = payload["resource"]["repository"]["id"]
        pr_id = payload["resource"]["pullRequestId"]
        status = payload["resource"]["status"]

        if status.lower() == "completed":
            run(repo_id, pr_id)
        return (
            jsonify({"message": "Recommendation service triggered successfully!"}),
            200,
        )

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        traceback.print_exc()  # This prints the stack trace to the console
        return (
            jsonify(
                {
                    "message": "Recommendation service failed to respond.",
                    "error": str(e),
                }
            ),
            500,
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
