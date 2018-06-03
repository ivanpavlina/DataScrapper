from lib.logger import get_logger
from threads.MikrotikScrapper import MikrotikScrapper
from threads.Database import Database
from threads.APCScrapper import APCScrapper
from time import sleep
import Queue
import signal

run_loop = True


def run():
    global run_loop
    # Will be filled by mikrotik/apcScrapper and saved by database thread
    database_input_queue = Queue.Queue()

    # Initialize threads
    threads = [
        Database(database_input_queue),
        MikrotikScrapper(database_input_queue)
        #APCScrapper(database_input_queue)
    ]

    # Start threads
    for thread in threads:
        thread.start()

    logger.info("Threads started")

    # Sleep for thread init
    sleep(5)

    # Run thread checker loop
    while True:
        try:

            # Check if shutdown event is triggered, cleanup threads and exit loop
            if not run_loop:
                logger.info("Received shutdown event, cleaning up")
                cleanup_threads(threads)
                break

            # Check if threads are alive and well
            for thread in threads:
                try:
                    if not thread.running:
                        logger.warning("{} thread not running, triggering shutdown event".format(thread.__class__.__name__))
                        raise Exception
                except Exception:
                    run_loop = False
                    continue

            # Everything ok with threads, just print message
            logger.debug("Running...")
            sleep(5)

        except KeyboardInterrupt:
            logger.warning("Got keyboard shutdown event")
            cleanup_threads(threads)
            break
        except Exception, e:
            logger.warning("Got unknown shutdown event [{}]".format(e))
            cleanup_threads(threads)
            break


def cleanup_threads(threads):
    logger.info("Cleaning up")
    for thread in threads:
        thread.wantRunning = False
        logger.info("Joining {}".format(thread.__class__.__name__))
        thread.join(10)
        logger.info("Thread {} joined".format(thread.__class__.__name__))


def shutdown_loop(signo, stack_frame):
    # Called on sigterm
    global run_loop
    run_loop = False


if __name__ == '__main__':
    # Setup logger
    logger = get_logger('Main')
    logger.info("***************************")
    logger.info("*      DATA SCRAPPER      *")
    logger.info("*         V0.0.1          *")
    logger.info("*          Exit           *")
    logger.info("***************************")

    # Setup listener for sigterm
    signal.signal(signal.SIGTERM, shutdown_loop)

    # Run main function
    # Function is blocking until threads are alive
    run()

    # Main done
    logger.info("+++++++++++++++++++++++++++")
    logger.info("+       Main done         +")
    logger.info("+++++++++++++++++++++++++++")
