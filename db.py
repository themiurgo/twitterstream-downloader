import datetime
#import gzip

class FilenameRotator(object):
    def __init__(self, base_filename):
        self._base_filename = base_filename

    @property
    def filename(self):
        today = datetime.datetime.now().strftime("%Y%m%d")
        return "{0}_{1}.json".format(self._base_filename, today)

class FileAppendDb(object):
    def __init__(self, filename):
        self._fname_rotator = FilenameRotator(filename)
        self._last_file_object = None
        self._last_filename = ""

    def save(self, text):
        text = str(text)
        fname = self._fname_rotator.filename
        if fname == self._last_filename:
            f = self._last_file_object
        else:
            try:
                self._last_file_object.close()
            except:
                pass
            #f = gzip.open(fname, "a")
            f = open(fname, "a")
            self._last_file_object = f
            self._last_filename = fname
        f.write(text)
        f.write("\n")

    def sync(self):
        self._last_file_object.flush()
