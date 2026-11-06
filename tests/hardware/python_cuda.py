"""Installed-wheel CUDA exporter gate. Requires an assigned real GPU and PyTorch."""
import gc
import json
from pathlib import Path
import tempfile
import time
import weakref

import orbite as te
import torch


class Array:
    def __init__(self, tensor, stream):
        self.tensor, self.stream = tensor, stream

    @property
    def __cuda_array_interface__(self):
        return dict(version=3, shape=(self.tensor.numel(),), typestr='|u1',
                    strides=None, data=(self.tensor.data_ptr(), False),
                    stream=self.stream.cuda_stream)

    def __dlpack_device__(self):
        return self.tensor.__dlpack_device__()


class Pack:
    def __init__(self, tensor, stream):
        self.tensor, self.stream = tensor, stream

    def __dlpack_device__(self):
        return self.tensor.__dlpack_device__()

    def __dlpack__(self, **kwargs):
        # Torch inserts the dependency from the retained producer stream to the
        # consumer's stream. Both old and max_version-aware producers are valid.
        with torch.cuda.stream(self.stream):
            return self.tensor.__dlpack__(**kwargs)


def unregister(engine, address):
    deadline = time.monotonic() + 30
    while True:
        try:
            engine.unregister_local_memory(address)
            return
        except te.InternalError as error:
            if not str(error).endswith('native status 2') or time.monotonic() >= deadline:
                raise
            time.sleep(.001)


def main():
    assert 'site-packages' in te.__file__, te.__file__
    assert torch.cuda.is_available() and torch.cuda.device_count() == 1
    cases = []
    for factory in (Array, Pack):
        for gpu_is_source in (False, True):
            for size in (64, 1024 * 1024):
                with tempfile.TemporaryDirectory() as directory:
                    cfg = Path(directory) / 'engine.json'
                    cfg.write_text(json.dumps(dict(metadata_type='p2p', rpc_server_hostname='127.0.0.1',
                        rpc_server_port=0, transports={'tcp': {'enable': True}, 'rdma': {'enable': False},
                                                     'hp_tcp': {'enable': False}, 'shm': {'enable': False}})))
                    source, requester = te.TransferEngine(str(cfg)), te.TransferEngine(str(cfg))
                    stream = torch.cuda.Stream()
                    with torch.cuda.stream(stream):
                        tensor = torch.full((size + 32,), 0x71, dtype=torch.uint8, device='cuda')
                        tensor[16:-16].fill_(0x29)
                    owner = factory(tensor, stream)
                    witness = weakref.ref(owner)
                    gpu_address = tensor.data_ptr()
                    gpu_engine = source if gpu_is_source else requester
                    host_engine = requester if gpu_is_source else source
                    gpu_engine.register_local_memory(gpu_address, tensor.nbytes, owner=owner)
                    host = bytearray(b'x' * 16 + b'!' * size + b'x' * 16)
                    host_address = host_engine.register_buffer(host)
                    peer = requester.open_segment(source.get_segment_name())
                    del owner, tensor
                    gc.collect()
                    assert witness() is not None
                    local, remote = (host_address, gpu_address) if gpu_is_source else (gpu_address, host_address)
                    for opcode in (te.READ, te.WRITE):
                        if opcode == te.WRITE:
                            if gpu_is_source:
                                host[16:-16] = b'S' * size
                            else:
                                witness().tensor[16:-16].fill_(0x53)
                                torch.cuda.synchronize()  # a fresh external producer handoff
                        with requester.allocate_batch_guard(1) as batch:
                            requester.submit_transfer(batch, [te.Request(opcode, local + 16, peer,
                                remote + 16, size, transport_hint=te.TCP)])
                            result = requester.wait(batch, 30)
                            assert result.state == te.COMPLETED and result.bytes == size
                        expected = b'S' if opcode == te.WRITE else (b')' if gpu_is_source else b'!')
                        assert host == b'x' * 16 + expected * size + b'x' * 16
                        copied = bytes(witness().tensor.cpu().tolist())
                        assert copied == b'q' * 16 + expected * size + b'q' * 16
                    unregister(gpu_engine, gpu_address)
                    unregister(host_engine, host_address)
                    requester.close()
                    source.close()
                    gc.collect()
                    assert witness() is None
                    cases.append(dict(protocol=factory.__name__, gpu_source=gpu_is_source, bytes=size,
                                      read_write_guards='passed', exporter_release='passed'))
    print(json.dumps(dict(status='passed', torch=torch.__version__, cuda=torch.version.cuda,
                          device=torch.cuda.get_device_name(), cases=cases)))


if __name__ == '__main__':
    main()
