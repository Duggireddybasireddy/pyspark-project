import logging

def get_logger(name, log_path):
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logging.basicConfig(filename=log_path,filemode="a", level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
    return logger

if __name__=="__main___":
    get_logger("main", "log.log")  
