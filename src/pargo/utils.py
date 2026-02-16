from typing import Any

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


def true():
    logger.info("Returning True")
    return True


def false():
    logger.info("Returning False")
    return False


def get_items():
    logger.info("Getting items for loop")
    return {"item":[1, 2, 3]}


def get_ys():
    logger.info("Getting items for loop")
    return {"y":[1, 2, 3]}


def echo_item(item: Any):
    logger.info(f"Item has value {item} an type {type(item).__name__}")


def add_item(item: int, x: int):
    logger.info(f"Adding {item} to x, save as y")
    return {"y": item + x}


def add_y(x: int, y: int):
    logger.info("Adding y to x, save as y")
    return {"y": x + y}
