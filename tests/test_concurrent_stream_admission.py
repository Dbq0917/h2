"""
Test outbound concurrent-stream admission.
"""
from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.exceptions
import h2.frame_buffer
import h2.settings

REQUEST_HEADERS = [
    (b":method", b"GET"),
    (b":path", b"/"),
    (b":authority", b"example.com"),
    (b":scheme", b"https"),
]

RESPONSE_HEADERS = [
    (b":status", b"200"),
]


def connected_peers() -> tuple[h2.connection.H2Connection, h2.connection.H2Connection]:
    """
    Complete an in-memory HTTP/2 preface and SETTINGS exchange.
    """
    client = h2.connection.H2Connection()
    server = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=False),
    )
    client.initiate_connection()
    server.initiate_connection()

    for _ in range(4):
        client_data = client.data_to_send()
        if client_data:
            server.receive_data(client_data)
        server_data = server.data_to_send()
        if server_data:
            client.receive_data(server_data)

    return client, server


def parse_frames(data: bytes) -> list:
    """
    Parse bytes into HTTP/2 frames.
    """
    buffer = h2.frame_buffer.FrameBuffer()
    buffer.max_frame_size = 2**16
    buffer.add_data(data)
    return list(buffer)


def test_reduced_limit_queues_new_streams_without_resetting_open_streams() -> None:
    """
    Lowering the peer's limit queues new streams without disturbing open ones.
    """
    client, server = connected_peers()

    for stream_id in range(1, 100, 2):
        client.send_headers(stream_id, REQUEST_HEADERS, end_stream=True)
    server.receive_data(client.data_to_send())

    assert client.open_outbound_streams == 50
    assert server.open_inbound_streams == 50

    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 10},
    )
    for _ in range(3):
        server_data = server.data_to_send()
        if server_data:
            client.receive_data(server_data)
        client_data = client.data_to_send()
        if client_data:
            server.receive_data(client_data)

    assert client.remote_settings.max_concurrent_streams == 10

    for stream_id in range(101, 141, 2):
        client.send_headers(stream_id, REQUEST_HEADERS, end_stream=True)

    assert client.open_outbound_streams == 50
    assert client.pending_outbound_streams == 20
    assert client.pending_outbound_stream_ids == tuple(range(101, 141, 2))
    assert client.data_to_send() == b""

    # Finish 45 of the streams that were already open. This leaves five open,
    # so only the first five queued requests may now be admitted.
    for stream_id in range(1, 91, 2):
        server.send_headers(stream_id, RESPONSE_HEADERS, end_stream=True)
    client.receive_data(server.data_to_send())

    assert client.open_outbound_streams == 10
    assert client.pending_outbound_streams == 15
    assert client.pending_outbound_stream_ids == tuple(range(111, 141, 2))

    admitted = [
        frame for frame in parse_frames(client.data_to_send())
        if frame.stream_id
    ]
    assert [frame.stream_id for frame in admitted] == list(range(101, 111, 2))
    assert all("END_STREAM" in frame.flags for frame in admitted)

    server.receive_data(b"".join(frame.serialize() for frame in admitted))
    assert server.open_inbound_streams == 10


def test_increasing_limit_admits_queued_streams_in_order() -> None:
    """
    Raising the peer's limit drains the queue in FIFO order.
    """
    client, server = connected_peers()
    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1},
    )
    client.receive_data(server.data_to_send())
    server.receive_data(client.data_to_send())
    client.clear_outbound_data_buffer()

    client.send_headers(1, REQUEST_HEADERS, end_stream=True)
    client.clear_outbound_data_buffer()
    for stream_id in (3, 5, 7):
        client.send_headers(stream_id, REQUEST_HEADERS, end_stream=True)

    assert client.pending_outbound_stream_ids == (3, 5, 7)
    assert client.data_to_send() == b""

    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 4},
    )
    client.receive_data(server.data_to_send())

    admitted = [
        frame for frame in parse_frames(client.data_to_send())
        if frame.stream_id
    ]
    assert [frame.stream_id for frame in admitted] == [3, 5, 7]
    assert client.pending_outbound_streams == 0


def test_queued_stream_buffers_data_until_admitted() -> None:
    """
    Data sent to a queued stream is held until its HEADERS can be sent.
    """
    client, server = connected_peers()
    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1},
    )
    client.receive_data(server.data_to_send())
    server.receive_data(client.data_to_send())
    client.clear_outbound_data_buffer()

    client.send_headers(1, REQUEST_HEADERS, end_stream=True)
    client.clear_outbound_data_buffer()
    client.send_headers(3, REQUEST_HEADERS)
    client.send_data(3, b"hello", end_stream=True)

    assert client.pending_outbound_stream_ids == (3,)
    assert client.data_to_send() == b""

    client.reset_stream(1)

    frames = parse_frames(client.data_to_send())
    assert [type(frame).__name__ for frame in frames] == [
        "RstStreamFrame",
        "HeadersFrame",
        "DataFrame",
    ]
    assert [frame.stream_id for frame in frames] == [1, 3, 3]
    assert frames[-1].data == b"hello"


def test_queue_count_and_id_aliases() -> None:
    """
    Both queue introspection names expose the same FIFO state.
    """
    client, server = connected_peers()
    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1},
    )
    client.receive_data(server.data_to_send())

    client.send_headers(1, REQUEST_HEADERS, end_stream=True)
    client.clear_outbound_data_buffer()
    client.send_headers(3, REQUEST_HEADERS, end_stream=True)

    assert client.pending_outbound_streams == client.queued_outbound_streams == 1
    assert client.pending_outbound_stream_ids == client.queued_outbound_stream_ids == (3,)


def test_queued_stream_cannot_send_headers_twice() -> None:
    """
    A queued stream already has its initial HEADERS block reserved.
    """
    client, server = connected_peers()
    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1},
    )
    client.receive_data(server.data_to_send())

    client.send_headers(1, REQUEST_HEADERS, end_stream=True)
    client.clear_outbound_data_buffer()
    client.send_headers(3, REQUEST_HEADERS, end_stream=True)

    with pytest.raises(h2.exceptions.ProtocolError):
        client.send_headers(3, REQUEST_HEADERS, end_stream=True)


def test_reset_queued_stream_cancels_it_without_wire_frames() -> None:
    """
    Cancelling a queued stream only removes it from the queue.
    """
    client, server = connected_peers()
    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1},
    )
    client.receive_data(server.data_to_send())

    client.send_headers(1, REQUEST_HEADERS, end_stream=True)
    client.clear_outbound_data_buffer()
    client.send_headers(3, REQUEST_HEADERS, end_stream=True)

    client.reset_stream(3)

    assert client.pending_outbound_streams == 0
    assert client.data_to_send() == b""
    assert 3 not in client.streams


def test_queued_stream_rejects_data_after_end_stream_headers() -> None:
    """
    A queued stream whose HEADERS end the stream cannot accept data.
    """
    client, server = connected_peers()
    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1},
    )
    client.receive_data(server.data_to_send())

    client.send_headers(1, REQUEST_HEADERS, end_stream=True)
    client.clear_outbound_data_buffer()
    client.send_headers(3, REQUEST_HEADERS, end_stream=True)

    with pytest.raises(h2.exceptions.StreamClosedError):
        client.send_data(3, b"hello")


def test_queued_stream_enforces_flow_control_on_buffered_data() -> None:
    """
    Buffered data on a queued stream still observes the flow-control window.
    """
    client, server = connected_peers()
    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1},
    )
    client.receive_data(server.data_to_send())

    client.send_headers(1, REQUEST_HEADERS, end_stream=True)
    client.clear_outbound_data_buffer()
    client.send_headers(3, REQUEST_HEADERS)
    for _ in range(4):
        client.send_data(3, b"x" * 16000)

    with pytest.raises(h2.exceptions.FlowControlError):
        client.send_data(3, b"x" * 2000)


def test_queued_stream_end_stream_is_buffered() -> None:
    """
    ``end_stream`` is deferred until a queued stream is admitted.
    """
    client, server = connected_peers()
    server.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1},
    )
    client.receive_data(server.data_to_send())

    client.send_headers(1, REQUEST_HEADERS, end_stream=True)
    client.clear_outbound_data_buffer()
    client.send_headers(3, REQUEST_HEADERS)
    client.end_stream(3)

    with pytest.raises(h2.exceptions.StreamClosedError):
        client.end_stream(3)

    client.reset_stream(1)

    frames = parse_frames(client.data_to_send())
    assert [type(frame).__name__ for frame in frames] == [
        "RstStreamFrame",
        "HeadersFrame",
        "DataFrame",
    ]
    assert "END_STREAM" in frames[-1].flags
