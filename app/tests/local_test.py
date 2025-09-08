from app.src import lambda_handler

def test_lambda_handler():
    event = {"key": "value"}
    result = lambda_handler.lambda_handler(event, None)
    assert result["statusCode"] == 200
    assert result["body"] == event

if __name__ == "__main__":
    test_lambda_handler()
    print("Test passed!")