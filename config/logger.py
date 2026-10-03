import logging, os, uuid
from datetime import datetime, timezone

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

def get_logger(name):
    log = logging.getLogger(name)
    if logging.log.handlers: # no duplicate handlers on re-import
        return logging.log
    logging.log.setLevel(logging.INFO)
    fmt = logging.Formatter(f"%(asctime)s %(levelname)-7s [{RUN_ID}] [%(name)s] %(message)s")
    os.makedirs("logs", exist_ok=True)
    for h in (logging.StreamHandler(), logging.FileHandler("logs/pipeline.log")):
        h.setFormatter(fmt)
        logging.log.addHandler(h)
    return logging.log