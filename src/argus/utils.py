from loguru import logger


def void():
    logger.info("Doing nothing to the parameters.")


def double(x: int):
    logger.info("Doubling x")
    return {"x": 2 * x}


def triple(x: int):
    logger.info("Tripling")
    return {"x": 3 * x}


def choice(x: int):
    logger.info("Checking if divisible by 3.")
    return x % 3 == 0


def getitems():
    logger.info("Getting items for loop")
    return [1, 2, 3]


def additem(item: int, x: int):
    logger.info(f"Adding {item} to x")
    return {"x": item + x}


def addy(x: int, y: int):
    logger.info("Adding y to x")
    return {"x": x + y, "y": y}
