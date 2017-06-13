from pyspark import SparkConf,HiveContext,SparkContext
from LogSetup import Logs
import ConfigParser
import os
import ast

class SetupContext:


    def __init__(self,logger,config_file):
        method_name = {'module_name': SetupContext.__name__, 'method_name': 'Init'}
        self.logger = logger
        self.logger.info('logger initialized', extra=method_name)

        try:
            config = ConfigParser.ConfigParser()
            config.read(config_file)
            config.sections()
            self.SparkContextProperty = ast.literal_eval(config.get('sparkcontext', 'SparkContextProperty'))
            self.MASTER = config.get('sparkcontext', 'MASTER')
            self.APP_NAME = config.get('sparkcontext', 'APP_NAME')
        except BaseException ,err:
            self.logger.error('***Error  '+err.message, extra=method_name)
        pass


    def createSparkContext(self):
        method_name = {'module_name': SetupContext.__name__, 'method_name': 'createSparkContext()'}
        self.logger.info('creating spark context', extra=method_name)


        for key,value in self.SparkContextProperty.iteritems():
            SparkContext.setSystemProperty(key, value)

        self.sparkContext = SparkContext(self.MASTER, self.APP_NAME)
        return self.sparkContext

    def createHiveContext(self):
        method_name = {'module_name': SetupContext.__name__, 'method_name': 'createHiveContext()'}
        self.logger.info('creating Hive context', extra=method_name)
        self.createSparkContext()
        self.hiveContext=HiveContext(self.sparkContext)
        return self.hiveContext

if __name__ == '__main__':
    method_name = {'module_name': SetupContext.__name__, 'method_name': '__main__'}
    config_file = os.path.normpath(
        os.path.dirname(os.path.realpath(__file__)) + os.sep + os.pardir) + '/conf/settings.conf '
    logger = Logs(config_file).logger

    j=SetupContext(logger,config_file)












