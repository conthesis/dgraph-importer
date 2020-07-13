import asyncio

import dgraph_importer as dg

async def main():
    m = dg.Matcher()
    await m.setup()
    await m.run_until_done()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
