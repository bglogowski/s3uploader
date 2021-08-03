import sys


class Common(object):

    def _identify(self):
        """

        :return:
        :rtype:
        """
        return f"{self.__class__.__name__}.{sys._getframe(1).f_code.co_name}"
