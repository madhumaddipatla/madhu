from subprocess import Popen, PIPE
from signal import SIGINT, signal
import threading
import Queue
import argparse
from itertools import product
import re
import time
import os.path
from functools import partial
from ConfigParser import SafeConfigParser
from kafka import KafkaProducer
from Logging import logger

CONFIG_FILE = "config.ini"
CONFIG_SECTION = "FileReader"
DELIMITER = ","

parser = SafeConfigParser()
parser.read(CONFIG_FILE)
kafka_brokers = parser.get(CONFIG_SECTION, "kafka_brokers")
kafka_brokers = kafka_brokers.split(',')
producer = KafkaProducer(bootstrap_servers=kafka_brokers)
topic = parser.get(CONFIG_SECTION, "kafka_topic")
print_flag = False
print_flag = parser.get(CONFIG_SECTION, "print_flag")

def validate_files(files):
    logger.debug("Validating the input files")
    final_list = []
    file_list = files.split(DELIMITER)
    for file_name in file_list:
        status = os.path.isfile(file_name)
        if status:
            final_list.append(file_name)
        else:
            print "File : %s does not exist. Skipping" %(file_name)
    return (DELIMITER).join(final_list)


def print_with_color(data, color):
    print "\033[%dm%s\033[0m" % (color, data)

def split_strip_and_filter():
    _files = parser.get(CONFIG_SECTION, "files")
    validated_files = validate_files(_files)
    output = map(lambda x: x.strip(), validated_files.split(DELIMITER))
    return filter(bool, output)

class Tailor(threading.Thread):
    daemon = True
    running = True
    def __init__(self, queue, lock, server, file, match = None, ignore = None):
        self.lock = lock
        threading.Thread.__init__(self)
        self.server = server
        self.file = file
        self.queue = queue
        self.match = match
        self.ignore = ignore
        self.start()

    def run(self):
        self._connect()
        for line in self._lines():
            self._process_line(line)
        self.stop()

    def stop(self):
        if self.running:
            print "Closing: %s:%s" % (self.server, self.file)
            self._stop()
        return self

    def _lines(self):
        line = self.tail_process.stdout.readline()
        while self.running and line:
            yield line
            line = self.tail_process.stdout.readline()

    def _is_local(self):
        return self.server not in ('localhost', 'local', '')

    def _tail_command(self):
        #command = ['ssh', '-t', self.server] if  self._is_local() else []
        #return command + ['tail', '-f', self.file]
        return  ['tail', '-f', self.file]

    def _start_tail_process(self):
        self.tail_process = Popen(
            self._tail_command(),
            stdout = PIPE,
            stdin = PIPE,
            stderr = PIPE
        )

    def _ignore(self, line):
        return self.ignore and re.search(self.ignore, line)

    def _match(self, line):
        if self._ignore(line):
            return False
        elif self.match:
            return re.search(self.match, line)
        else:
            return True

    def _process_line(self, line):
        line = line.strip()
        if self._match(line):
            self._put_in_queue(line)

    def _put_in_queue(self, line):
        self.queue.put((self.server, self.file, line))

    def _connect(self):
        self.lock.acquire()
        try:
            self._start_tail_process()
        finally:
            self.lock.release()

    def _stop(self):
        self.running = False
        self._stop_tailing_process()

    def _stop_tailing_process(self):
        try:
            self.tail_process.terminate()
        finally:
            self._Thread__stop()

class TailManager(object):
    match = None
    ignore = None
    def __init__(self):
        self.queue = Queue.Queue()
        self.lock = threading.Lock()
        self._init_args()
        self._set_colors()

    def run(self):
        signal(SIGINT, self._stop)
        self.running = True
        self._tail()

    def _stop(self, *args):
        self.running = False
        for t in self.trailers:
            t.stop()

    def _init_args(self):
        self.servers = split_strip_and_filter()
        self.files = split_strip_and_filter()
        #self._set_rules(args)

    def _set_rules(self, args):
        if args.ignore is not None:
            self.ignore = re.compile(args.ignore, re.I)
        if args.match is not None:
            self.match = re.compile(args.match, re.I)

    def _print_open(self, server, file):
        message = "Opening {file} on {server}".format(
            file = file,
            server = server
        )
        self._print(message, server, file)

    def _init_tailor(self, server, file):
        self._print_open(server, file)
        return Tailor(
            self.queue,
            self.lock,
            server,
            file,
            self.match,
            self.ignore
        )

    def _tail(self):
        self._start_trailers()
        while self.running:
            if self.queue.empty():
                time.sleep(.5)
            else:
                self._print_line()

    def _print_line(self):
        server, file, line = self.queue.get()
        if print_flag:
            self._print(line + "\r", server, file)
        producer.send(topic, line)

    def _start_trailers(self):
        server_file_combos = product(self.servers, self.files)
        self.trailers = [self._init_tailor(s, f) for s,f in  server_file_combos]

    def _print(self, message, server, file):
        identifier = server if self.color_by == 'server' else file
        print_with_color(message, self.colors[identifier])

    def _set_colors(self):
        self.color_by = 'file'
        alternates = self.files
        self.colors = { f: (91 + i) % 100 for i, f in enumerate(alternates) }

if __name__ == "__main__":
    TailManager().run()
