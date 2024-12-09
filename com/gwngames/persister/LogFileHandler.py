import logging
import os


class LogFileHandler(logging.Handler):
    def __init__(self, filename, mode='a', max_lines=10000, encoding=None, delay=False):
        super().__init__()
        self.baseFilename = filename
        self.mode = mode
        self.max_lines = max_lines
        self.encoding = encoding
        self.delay = delay
        self.current_line_count = 0
        self.stream = self._open()

    def should_rollover(self):
        """
        Determines whether the current line count has reached or exceeded the maximum line count.

        :return: True if the current line count is equal to or greater than the maximum line count, False otherwise.
        :rtype: bool
        """
        return self.current_line_count >= self.max_lines

    def emit(self, record):
        """
        Write the log record to the output stream.

        :param record: The log record to be emitted.
        :return: None
        """
        try:
            msg = self.format(record)
            self.stream.write(msg + '\n')  # Adding newline character after each message
            self.current_line_count += 1
            if self.should_rollover():
                self.roll_over()
        except Exception:
            self.handleError(record)

    def roll_over(self):
        """
        Roll over the log file, closing the current file, renaming it, and opening a new file.
        This is to avoid having huge files to navigate across while troubleshooting.

        :return: None
        """
        self.stream.close()
        self.current_line_count = 0
        # Rename the existing log file
        if os.path.exists(self.baseFilename):
            index = 1
            while True:
                new_name = f"{self.baseFilename}.{index}"
                if not os.path.exists(new_name):
                    os.rename(self.baseFilename, new_name)
                    break
                index += 1
        # Open a new log file
        self.stream = self._open()

    def _open(self):
        """
        Opens the file with the specified encoding, if any.

        :return: the opened file object
        """
        if self.encoding is None:
            return open(self.baseFilename, self.mode, buffering=True)
        else:
            return open(self.baseFilename, self.mode, buffering=True, encoding=self.encoding)
