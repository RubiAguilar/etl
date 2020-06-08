import logging
import sys

logging.basicConfig(level=logging.ERROR)
root = logging.getLogger("flat_table")
root.setLevel(logging.ERROR)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.ERROR)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
