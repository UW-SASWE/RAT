from utils.logging import init_logger


log = init_logger(
    "/houston2/pritam/rat_mekong_v3/backend/logs", 
    "DEBUG", 
    True
    )

log.info("Oh shit!")