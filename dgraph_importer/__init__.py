import traceback
import asyncio
import signal
from typing import Any, Dict, List, Tuple, Optional

from nats.aio.client import Client as NATS
import itertools
import orjson
import pydgraph
from pydantic import BaseModel, validator


def _query_string_frag(name, value):
    return f'eq({name}, \"{value}\")'

def _nquad(name, rel, value):
    return f'{name} <{rel}> {value}.'




class EntityField(BaseModel):
    name: str
    key: Optional[str] = None

    @validator('key', pre=True, always=True)
    def key_default(cls, v, *, values, **kwargs):
        return v or values['name']

    def value(self, data):
        return data[self.key]

class Entity(BaseModel):
    name: str
    dgraph_type: str
    query_filter: EntityField
    attributes: List[EntityField] = []
    multiple: bool = False

    def nquads(self, data):
        for (name, attr, val) in self._each_all_attr(data):
            yield _nquad(f"uid({name})", attr, val)

    def _each_all_attr(self, data, include_filter=True, include_dg_type=True):
        if include_dg_type:
            for name in self.each_name(data):
                yield (name, "dgraph.type", f'"{self.dgraph_type}"')
        if include_filter:
            for (name, val) in self._each(data):
                yield (name, self.query_filter.name, f'"{val}"')
        for attr in self.attributes:
            for (name, val) in self._each(data, attr):
                yield (name, attr.name, f'"{val}"')


    def each_name(self, data):
        if not self.multiple:
            yield self.name
            return
        else:
            vals = self.query_filter.value(data)
            assert isinstance(vals, list)
            for i in range(len(vals)):
                yield f"{self.name}_{i}"


    def _each(self, data, attribute=None):
        if attribute is None:
            attribute = self.query_filter
        if not self.multiple:
            yield (self.name, attribute.value(data))
            return

        vals = attribute.value(data)
        if not isinstance(vals, list):
            if attribute is self.query_filter:
                print(attribute, vals)
                raise RuntimeError("When multiple is set QueryFilter must be list")
            else:
                vals = [vals] * len(self.query_filter) # Replicate the same value for each
        for (i, x) in enumerate(vals):
            yield (f"{self.name}_{i}", x)


    def query(self, data):
        qf = self.query_filter
        for (name, val) in self._each(data):
            yield f"{name} as var(func: {_query_string_frag(qf.name, val)})"

class ExtractionConfig(BaseModel):
    entities: List[Entity]
    relations: List[Tuple[str, str, str]]

    def _find_ent(self, name: str) -> Optional[Entity]:
        for e in self.entities:
            if name == e.name:
                return e
        return None

    def _relation_quads(self, data):
        for (src, rel, dst) in self.relations:
            sources = self._find_ent(src).each_name(data)
            destinations = self._find_ent(dst).each_name(data)
            for (src_name, dst_name) in itertools.product(sources, destinations):
                yield _nquad(f"uid({src_name})", rel, f"uid({dst_name})")

    def _nquads(self, data):
        for e in self.entities:
            yield from e.nquads(data)

        yield from self._relation_quads(data)

    def nquads(self, data):
        return "\n".join(self._nquads(data))

    def query(self, data):
        queries = itertools.chain(*[e.query(data) for e in self.entities])
        return '{\n'+ "\n".join(queries) + "\n }"



class ExtractionTrigger(BaseModel):
    config: ExtractionConfig
    data: Dict[str, Any]

    def nquads(self):
        return self.config.nquads(self.data)

    def query(self):
        return self.config.query(self.data)

    @classmethod
    def from_bytes(cls, b: bytes) -> "ExtractionTrigger":
        return cls.parse_obj(orjson.loads(b))


TOPIC = "conthesis.action.dgraph.Import"

class Matcher:
    fut_stop: asyncio.Future
    def __init__(self):
        self.nc = NATS()
        client_stub = pydgraph.DgraphClientStub('dgraph:9080')
        self.dg = pydgraph.DgraphClient(client_stub)
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self.stop)
        loop.add_signal_handler(signal.SIGHUP, self.stop)
        loop.add_signal_handler(signal.SIGINT, self.stop)
        self.fut_stop = loop.create_future()

    async def setup(self):
        await self.nc.connect("nats://nats")
        await self.nc.subscribe(TOPIC, "matcher", self.on_update)
        print("Listening on ", TOPIC)
        self.setup_schema()

    def setup_schema(self):
        schema = """
        email_address: string @index(exact) .
        id: string @index(exact) .
        """

        op = pydgraph.Operation(schema=schema)
        self.dg.alter(op)

    async def reply(self, msg, resp):
        if msg.reply:
                await self.nc.publish(msg.reply, resp)


    async def on_update(self, msg) -> None:
        tr = None
        try:
            tr = ExtractionTrigger.from_bytes(msg.data)
        except Exception:
            traceback.print_exc()
            await self.reply(msg, orjson.dumps({ "ok": False }))
            return

        txn = self.dg.txn()
        nq = None
        query = None
        try:
            nq = tr.nquads()
            query = tr.query()
            mutation = txn.create_mutation(set_nquads=nq)
            request = txn.create_request(query=query, mutations=[mutation], commit_now=True)
            txn.commit()
            await self.reply(msg, orjson.dumps({"ok": True}))
        except Exception:
            traceback.print_exc()
            print(nq, query)
            txn.discard()
            await self.reply(msg, orjson.dumps({ "ok": False }))

    def stop(self) -> None:
        self.fut_stop.set_result(True)

    async def run_until_done(self) -> None:
        await self.fut_stop
        await asyncio.gather(self.nc.drain())
