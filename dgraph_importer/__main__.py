import asyncio

import matcher

async def main():
    m = matcher.Matcher()
    await m.setup()
    await m.run_until_done()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
