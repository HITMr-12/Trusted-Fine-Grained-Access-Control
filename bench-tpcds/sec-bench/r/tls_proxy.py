"""TLS-terminating forwarder: TLS on the LAN leg, plain to local MinIO.

Listens TLS on <listen> and forwards each connection to <target> over plain
TCP. Used to give the NATIVE S3A path a TLS posture without restarting MinIO.
"""
import asyncio
import ssl
import sys

LISTEN_HOST, LISTEN_PORT, TARGET_HOST, TARGET_PORT, CERT, KEY = sys.argv[1:7]


async def pipe(reader, writer):
    try:
        while True:
            data = await reader.read(1 << 20)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def handle(client_reader, client_writer):
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(TARGET_HOST, int(TARGET_PORT))
    except OSError:
        client_writer.close()
        return
    await asyncio.gather(pipe(client_reader, upstream_writer),
                         pipe(upstream_reader, client_writer))


async def main():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(CERT, KEY)
    server = await asyncio.start_server(handle, LISTEN_HOST, int(LISTEN_PORT), ssl=context)
    print('TLS_PROXY_READY %s:%s -> %s:%s' % (LISTEN_HOST, LISTEN_PORT, TARGET_HOST, TARGET_PORT), flush=True)
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
