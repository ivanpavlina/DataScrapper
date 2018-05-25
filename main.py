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
    while True:
        try:
            sleep(60)

            # Check if main should loop signal is sent
            if not run_loop:
                logger.info("Received shutdown request, shutting down")
                cleanup_threads(threads)
                break

            # Check if threads are alive and well
            for thread in threads:
                if not thread.running:
                    logger.warning("{} thread not running, shutting down".format(thread.__class__.__name__))
                    cleanup_threads(threads)
                    break

            # Everything ok with threads, just print message
            logger.info("Running...")

        except KeyboardInterrupt:
            logger.warning("Got keyboard shutdown")
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
        thread.join(10)


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
