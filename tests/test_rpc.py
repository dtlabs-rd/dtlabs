import pytest
import dtlabs.rpc
import threading
import time


class TestAddArgument(dtlabs.rpc.Message):
    x: int
    y: int

@pytest.fixture(scope="module")
def add():
    def add_fn(x: int, y: int) -> int:
        return x + y
    return add_fn  # Return function instead of executing it immediately

# NOTE: This rpc_server will cause 2 warnings when run the test. Maybe, in the future, we might need to change it.
@pytest.fixture(scope="module")
def rpc_server(add):
    host = "localhost"
    queue = "test_rpc_queue"

    server = dtlabs.rpc.RPCServer(host=host, queue=queue, func=add)

    # Start the server in a separate thread
    thread = threading.Thread(target=server.start_consuming, daemon=True)
    thread.start()

    # Allow some time for the server to start
    time.sleep(1)

    yield server  # Provide the server to the test

    # Cleanup: Stop the RPC server after tests are done
    server.stop_consuming()  # Ensure your RPCServer has a method to stop
    thread.join(timeout=2)

def test_rpc_client(rpc_server):
    host = "localhost"
    routing_key = "test_rpc_queue"

    message = TestAddArgument(x=3, y=5)

    client = dtlabs.rpc.RPCClient(host=host)
    response = client.call(message, routing_key=routing_key, timeout=5)  # Add timeout
    assert int(response) == 8  # Check if the sum is correct
