import logging
class LogUtil:
  @staticmethod
  def getLogger(filename,name=''):
    logger=logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter=logging.Formatter('%(asctime)-15s %(name)-8s %(levelname)-8s [(Line %(lineno)d) %(filename)s %(funcName)s] | %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    handler=logging.FileHandler(filename,encoding='utf-8')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger