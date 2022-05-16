from distutils.log import debug
import logging



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(levelname)s %(message)s',
                    datefmt='%a %d %b %Y %H:%M:%S')
                   
log = logging.getLogger("RRSDK")

log.info("logging info!")
log.debug("hello logging!")
log.warning("hello logging warning!")
log.error("hello logging error!")
log.critical("logging critical!")